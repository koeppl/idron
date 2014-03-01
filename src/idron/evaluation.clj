(ns idron.evaluation
  [:use (idron tinkerpop osm tests algo sssp tools synthesized)]
  [:use incanter.distributions]
  (:import (com.tinkerpop.blueprints Graph Vertex Edge))
)(set! *warn-on-reflection* true)

(defn nano-time [] (System/nanoTime))

(defmacro timing 
  "Executes expr and return a tuple that contains the result of expr and the time it took to evaluate expr."
  [expr]
  `(let [start# (nano-time)
         ret# ~expr]
     (list ret# (- (nano-time) start#))))


(defn dissoc-landmark
  "Removes the cached vertex with id hid from the Landmark" 
  [landmark-data hid]
  {:pre [(nil? (get (:poi-nodeids landmark-data) hid ))] }
  (dosync
    (alter (:nodeids landmark-data) disj hid)
    (doseq [nodeid (:poi-nodeids landmark-data)]
      (alter (:matrix landmark-data) dissoc {:from hid :to nodeid})
      (alter (:matrix landmark-data) dissoc {:to hid :from nodeid})
      )))

(defn clear-cache
 "Clears the cache, i.e., removes all cached vertices along with its distances from the Landmark
 (and hence from its matrix)" 
  [landmark-data]
   (doseq [nodeid (clojure.set/difference  @(:nodeids landmark-data) (:poi-nodeids landmark-data))]
     (dissoc-landmark landmark-data nodeid)))
 ;TODO: this is wrong!


(defn add-test-cache
  "Add point s to the Landmark's cache"
  [g landmark-data s]
  (let [pdata (apply merge
                     (for [nodeid (:poi-nodeids landmark-data)]
                       {nodeid (struct-map Poi
                                           :graph-vertex (node-by-id g nodeid) 
                                           :dist (agent nil)
                                           :rdist (agent nil)
                                           ) } )) ]
    (do
      (dijkstra-dispatch-forward (promise) s :driveable 
                                 (apply merge 
                                        (for [ [pid p] pdata ]
                                          {(:graph-vertex p) (:dist p) }
                                          ))
                                 (list))
      (dijkstra-dispatch-reverse (promise) s :driveable 
                                 (apply merge 
                                        (for [ [pid p] pdata ]
                                          {(:graph-vertex p) (:rdist p) }
                                          ))
                                 (list))
      (cache-landmark s landmark-data pdata)
      )))


(def measure-count "Number of samples to evaluate for each setting" 30)

(defn reset-landmark-stats
 "Reset the stats :cache-hit, :cache-missed and :sssp-completed" 
  [landmark-data]
  (do
  (doseq [k [:cache-hit :cache-missed]]
    (reset! (k landmark-data) 0)))
  (reset! (:sssp-completed landmark-data) (list)))


(defn sssp-completed-means
 "Take the means of :sssp-completed. If there are no entries, take 0 as result." 
  [landmark-data]
  (/ (#(if (empty? %) 0.0 (mean %)) @(:sssp-completed landmark-data)) (count (:poi-nodeids landmark-data))))


(def Dataset 
  "The dataset to plot consists of the algorithm name, the starting point, the number of pois, the
  the stats of Landmark, optional the number of cols/rows of a 2D-graph and whether caching was used"
  (create-struct :algo-name :s :time :pois-count :cache-size :cache-hit :cache-missed :sssp-completed :graph-cols :graph-rows))

(defn measure-algorithm [^Graph g landmark-data s algo caption clear-fn]
  (let [ret
        (struct-map Dataset
                    :algo-name (str (:name algo) " " caption)
                    :s s  
                    :time (mean (butlast (rest (sort
                                  (for [_ (range measure-count)]
                                    (do
                                      (clear-fn landmark-data)
                                      (second (timing ((:fn algo) g landmark-data (node-by-id g s))))))))))
                    :pois-count (count (:poi-nodeids landmark-data))
                    :cache-size (- (count @(:nodeids landmark-data))  (count (:poi-nodeids landmark-data)))
                    :cache-hit (/ @(:cache-hit landmark-data) measure-count)
                    :cache-missed (/ @(:cache-missed landmark-data) measure-count)
                    :sssp-completed (sssp-completed-means landmark-data))
        ]
    (do
      (linfo ret)
      (reset! (:sssp-completed landmark-data) (list))
      ret)))

(defn run-nocache-routing [^Graph g landmark-data ss algo]
  (do
    (doall
         (for [s ss] 
           (do
             (linfo "Running " (:name algo) " for s = " s)
             (reset-landmark-stats landmark-data)
             (measure-algorithm g landmark-data s algo "" (fn [_] ))
               ))
           )))

(defn run-routing [^Graph g landmark-data ss algo]
  (do
    (reset-landmark-stats landmark-data)
    (doall
         (for [s ss] 
           (do
             (linfo "Running " (:name algo) " for s = " s)
             (measure-algorithm g landmark-data s algo "NoCache" clear-cache)
               ))
           )))

(defn run-cached-routing [^Graph g landmark-data ss algo]
  (do
    (clear-cache landmark-data)
    (reset-landmark-stats landmark-data)
    (concat (doall
      (for [s ss] 
        (do
          (linfo "Running warm-up cached " (:name algo) " for s = " s)
          (measure-algorithm g landmark-data s algo "First" (fn [_] ))
          )))
      (do
        (reset-landmark-stats landmark-data)
        (doall (for [s ss] 
          (do
            (linfo "Running cached " (:name algo) " for s = " s)
            (measure-algorithm g landmark-data s algo "Second" (fn [_]))
            )))))
    ))


(defn generate-lattice-equal-dist-pois [graph-row-count distance rows cols]
  (let [gen-id (matrix-element-id graph-row-count) ]
    (for [x (range 0 cols distance) y (range 0 rows distance)]
      (gen-id x y))))

(defn generate-nocache-data [g ss landmark-data]
  (concat
    (apply concat (doall
                    (for [algo [
                                {:name "novelDijkstra" :fn route-dijkstra} 
                                {:name "novelAStar" :fn route-astar}
                                ] ]
                        (run-nocache-routing g landmark-data ss algo) 
                        )))))
(defn generate-naive-data [g ss landmark-data]
  (concat
    (apply concat (doall
                    (for [algo [
                                {:name "naiveDijkstra" :fn naive-routing-dijkstra} 
                                {:name "naiveAStar" :fn naive-routing-astar} 
                                ] ]
                        (run-nocache-routing g landmark-data ss algo) 
                        )))))

(defn generate-data [g ss landmark-data]
  (concat
    (apply concat (doall
                    (for [algo [
                                {:name "novelAStar" :fn route-astar}
                                {:name "novelDijkstra" :fn route-dijkstra} 
                                ] ]
                      (concat
                        (run-routing g landmark-data ss algo) 
                        (run-cached-routing g landmark-data ss algo)))))

    (apply concat (doall
                    (for [algo [
                                {:name "naiveDijkstra" :fn naive-routing-dijkstra} 
                                {:name "naiveAStar" :fn naive-routing-astar} 
                                ] ]
                      (run-routing g landmark-data ss algo) ))))) 



(defn move-testing 
  "Moves the start point s in a given 2D-lattice diagonal from one POI to another."
  []
  (let [g (tinkergraph-inmem) row-size 15 col-size 15 node-size (* row-size col-size)
        gen-id (matrix-element-id row-size) ]
    (do 
     (generate-lattice-graph g row-size col-size)
     (make-undirected g :driveable)
      (let [pois (map (partial apply gen-id) (list [0 0]  [(dec col-size) (dec row-size)]))
            ss (map (partial apply gen-id) (take-while (partial every? pos?) (iterate (partial map dec) [(dec col-size) (dec row-size)])))
            landmark-data (create-landmark g pois) ]
        (generate-data g ss landmark-data)
        ))))




(defn star-testing 
  "Creates a star shaped network where s sits at the center. 
  At each iteration, the distance from s to the POIs gets larger."
  []
  (apply concat (doall
            (for [col-size (range 30 110 10)]
              (let [g (tinkergraph-inmem) 
                    row-size 15 
                    gen-id (matrix-element-id row-size)
                    ]
                (do 
                  (generate-star-graph g row-size col-size)
                  (make-undirected g :driveable)
                  (let [
                        pois (drop-last (map (fn [row] (apply gen-id [(int (/ (* (* (inc row) (inc row)) (dec col-size)) (* row-size row-size) )) row])) (take-while (complement neg?) (iterate dec (dec row-size)))))
                        ss (list (gen-id 0 0))
                        landmark-data (create-landmark g pois)
                        ]
                    (map #(assoc % :graph-cols col-size) (generate-data g ss landmark-data) )
                    )))))))


(defn star-far-testing []
  "Same as star-testing, but with an additional gap between s and the POIs."
  (apply concat (doall
            (for [col-size (range 100 160 5)]
              (let [g (tinkergraph-inmem) 
                    row-size 15 
                    gen-id (matrix-element-id row-size)
                    ]
                (do 
                  (generate-star-graph g row-size col-size)
                  (make-undirected g :driveable)
                  (let [poipos (reverse (drop-last (map (fn [row] [(int (/ (* (* (inc row) (inc row)) (dec col-size)) (* row-size row-size) )) row]) (take-while (partial < (/ row-size 2)) (iterate dec (dec row-size))))))
                        pois (map (partial apply gen-id) poipos)
                        smallest-dist (- (first (second poipos)) (first (first poipos)))
                        ss (list (gen-id 0 0))
                        landmark-data (create-landmark g pois)
                        ]
                    (map #(assoc % :graph-cols col-size)
                    (concat
                      (generate-naive-data g ss landmark-data)
                      (do
                        ;(if (= col-size 130) (debug-repl))
                        (clear-cache landmark-data)
                        (generate-nocache-data g ss landmark-data) )
                      (do
                        (clear-cache landmark-data)
                        (add-test-cache g landmark-data (node-by-id g (gen-id 2 0)))
                        (map #(assoc % :algo-name (str (:algo-name %) 0)) (generate-nocache-data g ss landmark-data) ))
                      (apply concat 
                             (doall
                               (for [factor (list (/ 2 3) (/ 1 2) (/ 1 4))]
                                 (do
                                  (clear-cache landmark-data)
                                  (linfo factor " -> "  (int (* smallest-dist factor)) "->" (gen-id (int (* smallest-dist factor)) 0))
                                  (add-test-cache g landmark-data (node-by-id g (gen-id (int (* smallest-dist factor)) 0)))
                                   (map #(assoc % :algo-name (str (:algo-name %) " " factor)) (generate-nocache-data g ss landmark-data) )
                                   ))))
                    )))))))))

(defn equidistant-numbers 
  "Distributes size-elements from 0 to upper-bound, returns a list" 
  [size upper-bound] 
  (range 0 upper-bound (int (/ upper-bound size) )))

(defn line-testing 
  "Move s along a line on which multiple equi-distantly distributed POIs are sitting."
  []
  (let [g (tinkergraph-inmem) row-size 20 col-size 20 node-size (* row-size col-size)]
    (do 
      (generate-line-graph g node-size)
     (make-undirected g :driveable)
      (let [pois (equidistant-numbers 13 node-size)
            ss (equidistant-numbers 17 node-size)
            landmark-data (create-landmark g pois)
            ]
        (generate-data g ss landmark-data)
        ))))

