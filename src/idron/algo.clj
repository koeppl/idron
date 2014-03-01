(ns idron.algo
  (:require [clojure.data.json :as json] 
            [clojure.tools.logging :as l])
  [:use clojure.stacktrace]
  [:use (idron readwritelock tinkerpop tools sssp osm)]
  (:import (com.tinkerpop.blueprints Graph Vertex Edge Direction)
           (clojure.lang Keyword))
)(set! *warn-on-reflection* true)


(def Landmark 
  "The data landmark consists of 
  a dynamical resizeable matrix of distances, 
  a static list of poi-node-ids, 
  a dynamic list of cached nodeids
  statistics of many times we got a cache hit, a cache missed, 
  and number of accessed POIs by the forward search algorithm during the last run (sssp-completed)"
  (create-struct :matrix :nodeids :poi-nodeids :cache-hit :cache-missed :sssp-completed))

(defn create-landmark 
  "We create the landmark data by calculating the distances between all POIs that are stored in the matrix :matrix"
  [^Graph g nodes]
  (let [calc 
        (apply merge (for [x nodes] (apply merge (for [y nodes] { {:from x :to y} (future (dijkstra (node-by-id g x) :driveable (node-by-id g y))) })) ))
        ]
                 
    (struct-map Landmark :nodeids (ref (set nodes)) :poi-nodeids (set nodes) :cache-hit (atom 0) :cache-missed (atom 0) :sssp-completed (atom (list))
                :matrix (ref (apply merge (for [entry calc] {(key entry) @(val entry)})))
    )))


(defn write-landmark [^Graph g nodes filename] (spit filename (json/write-str (create-landmark g nodes))))
(defn read-landmark [filename] (json/read-str (slurp filename)))


(def p-bound 
  "A p-bound is a point for which both forward and backward distance have been calculated." 
  (create-struct :nodeid :dist-f :dist-r))

(defn upper-bound 
  "We use the list of p-bounds to generate an upper bound for the (forward) distance"
  [p-bounds poi-nodeid cost-matrix]
  (let [l
         (for [p-bound p-bounds 
               :let [cost-b-p (cost-matrix (:nodeid p-bound) poi-nodeid)]
               :when (not-nil? cost-b-p) ] 
           (+ (:dist-f p-bound) cost-b-p )) ]
    (if (empty? l) Double/MAX_VALUE (apply min l))))

(defn lower-bound 
  "We use the list of p-bounds to generate a lower bound for the (forward) distance"
  [p-bounds poi-nodeid cost-matrix]
  (let [l
         (for [p-bound p-bounds 
               :let [cost-p-b (cost-matrix poi-nodeid (:nodeid p-bound))
                     cost-b-p (cost-matrix (:nodeid p-bound) poi-nodeid)]
               :when (and (not-nil? cost-p-b) (not-nil? cost-b-p) ) ] 
         (max
           (- (:dist-f p-bound) cost-p-b)
           (- cost-b-p (:dist-r p-bound)))
         )]
    (if (empty? l) 0 (apply max l))))



(defn err-handler-fn [ag ex] (do (print-stack-trace ex) (l/error "Agent-Error: " ex ", value: " @ag)))
(def Poi 
  "If the forward/backward SSSP algorithm has found a POI, it is saved in dist/rdist.
  Each POI is represented both as a node :graph-vertex in the routing network and 
  as a vertex in the ordering :order-vertex.
  Remember that it is our goal to complete the ordering."
  (create-struct :dist :rdist :graph-vertex :order-vertex))

(defn cache-landmark 
  "Stores s along with its distances to all POIs in the Landmark-data.
  Note that we need both forward and backward distance to all POIs."
  [s landmark-data pois]
    (dosync
      (alter (:nodeids landmark-data) conj (node-id s))
      (doseq [[pid p] pois]
        (alter (:matrix landmark-data) assoc {:from (node-id s) :to pid } (#(if (= -1 %) nil %) @(:dist p)))
        (alter (:matrix landmark-data) assoc  {:to (node-id s) :from pid } @(:rdist p))
        )))

(defn route-known 
  "If we start the routing algorithm at a starting point zid that is already tracked in our Landmark-data,
  either because it is a POI or a cached point,
  we just have to analyze landmark's matrix to generate the ordering."
  [^Graph g landmark-data zid] 
  {:post [ (not (empty? (all-edges %))) ]}
  (let [ordering (tinkergraph-inmem)
        cost-matrix (fn [x y] (get @(:matrix landmark-data) {:from x :to y} ))
        pois (apply merge (for [nodeid (:poi-nodeids landmark-data)] {nodeid (add-vertex ordering nodeid) } )) ]
    (do
      (swap! (:cache-hit landmark-data) inc)
      (doseq [ [lid l] pois :let [ldist (cost-matrix zid lid)] ] 
        (doseq [ [rid r] pois :let [rdist (cost-matrix zid rid)] :when (and (< rid lid) (not (edge-connected? l r) )) ]
          (cond
            (and (nil? ldist) (nil? rdist) ) (add-edge ordering l := r)
            (and (nil? ldist) (not-nil? rdist) ) (add-edge ordering l :< r)
            (and (nil? rdist) (not-nil? ldist) ) (add-edge ordering r :< l)
            (< ldist rdist) (add-edge ordering l :< r) 
            (< rdist ldist) (add-edge ordering r :< l)
            :else (add-edge ordering l := r))
          ))
      ordering)))

(defn dist-reachable? "Is it not nil and > 0?" [dist] (and (not-nil? dist) (pos? dist)))
(defn create-bounds 
  "If we have for a POI/cached point both forward and backward distances, we create a new b-bound"
  [nodeid poi-data]
  (let [p (get poi-data nodeid) dist-f @(:dist p) dist-r @(:rdist p) ]
                       (if (and (dist-reachable? dist-f) (dist-reachable? dist-r))
                         (struct-map p-bound :nodeid nodeid :dist-f dist-f :dist-r dist-r )
                         nil)))

(defn dist-nil? [poi] (nil? @(:dist poi)))

(defn novel-routing 
  "The main algorithm. It uses two dispatch SSSP algorithms for forward and backward search 
  from vertex s to all POIs defined in landmark-data"
  [sssp-forward-fn sssp-reverse-fn ^Graph g landmark-data ^Vertex s]
  (if (some #(= % (node-id s)) @(:nodeids landmark-data))
    (route-known g landmark-data (node-id s))
    (let [ordering (tinkergraph-inmem)
          lock (read-write-lock)
          edge-connected-ordering? (fn [u v] 
                                     (do-read-locking lock
                                                      (edge-connected? (:order-vertex u) (:order-vertex v))))
          ordering-edges (fn [] (do-read-locking lock (all-edges ordering)))

          add-edge-ordering (fn [u ^Keyword k v] 
                              (do-write-locking lock
                                                (if (not (edge-connected-ordering? u v)) (add-edge ordering (:order-vertex u) k (:order-vertex v))
                                                  )))
          pois (apply merge
                      (for [nodeid (:poi-nodeids landmark-data)]
                        {nodeid (struct-map Poi
                                            :graph-vertex (node-by-id g nodeid) 
                                            :dist (agent nil) 
                                            :rdist (agent nil) 
                                            :order-vertex (add-vertex ordering nodeid)) } ))
          helpers (apply merge
                                (for [nodeid (clojure.set/difference  @(:nodeids landmark-data) (:poi-nodeids landmark-data)) ]
                                      { nodeid (struct-map Poi
                                       :graph-vertex (node-by-id g nodeid)
                                            :dist (agent nil) 
                                            :rdist (agent nil) 
                                            :order-vertex nil) } ))
                                      
          bounds (ref (hash-set))
          poi-size (count (:poi-nodeids landmark-data))
          cost-matrix (fn [x y] (get @(:matrix landmark-data) {:from x :to y} ))
          unfinished (ref (set (keys pois)))  ;(for [ [number vertex] (map-indexed vector order-vertexes)] {:number number :vertex vertex}  ))
          ubound (fn [n] (upper-bound @bounds n cost-matrix))
          lbound (fn [n] (lower-bound @bounds n cost-matrix))

          ; this promise is later realized as the ordering. Hence, the last command of this routine is waiting for 'return' to realize.
          return (promise)



          check-finished (fn [] 
                           (do-read-locking lock
                                            (doseq [nodeid @unfinished 
                                                    :when (= (count (edges-of (get-vertex ordering nodeid) Direction/BOTH)) (- poi-size 1)) ] 
                                              (dosync (alter unfinished disj nodeid)))
                                            (if (empty? @unfinished) (deliver return ordering))
                                            )
                           )

          graph-reasoning (fn []
                            (do
                              (ldebug "unfinished" @unfinished)
                              (ldebug "bounds" @bounds)
                              (ldebug "dists" (for [p (vals pois)] @(:dist p)))

                              (doseq [lid @unfinished :let [l (get pois lid) ldist @(:dist l)] ]
                                (doseq [rid @unfinished
                                        :let [r (get pois rid) rdist @(:dist r)]
                                        :when (and (< rid lid) (not (edge-connected-ordering? l r) )) 
                                        ]
                                  (cond
                                    (and (not-nil? ldist) (not-nil? rdist) )
                                    (cond 
                                      (and (= ldist -1) (= rdist -1) ) (do (ldebug "Both not connected.") (add-edge-ordering l := r))
                                      (and (= ldist -1) (not-nil? rdist) ) (add-edge-ordering l :< r)
                                      (and (= rdist -1) (not-nil? ldist) ) (add-edge-ordering r :< l)
                                      (< ldist rdist) (add-edge-ordering l :< r) 
                                      (< rdist ldist) (add-edge-ordering r :< l) 
                                      :else (add-edge-ordering r := l) )
                                    :else
                                    (cond
                                      ;TODO: if s not connected to l, but r connected to l, then s cannot be connected to l
                                      ;          (and (not-nil? ldist) (= ldist -1) (> 0 (cost-matrix rid lid))) (add-edge-ordering r := l) 
                                      ; vice versa
                                      ;         (and (not-nil? rdist) (= rdist -1) (> 0 (cost-matrix lid rid))) (add-edge-ordering r := l) 
                                      (< (ubound lid) (lbound rid)) (do 
                                                                      (ldebug "Bounds: " @bounds)
                                                                      (ldebug "We have " (ubound lid) " < " (lbound rid) " and thus " lid " < " rid )
                                                                      (add-edge-ordering l :< r))
                                      (< (ubound rid) (lbound lid)) (do
                                                                      (ldebug "Bounds: " @bounds)
                                                                      (ldebug "We have " (ubound rid) " < " (lbound lid) " and thus " lid " > " rid )
                                                                      (add-edge-ordering r :< l))
                                      )
                                    )))
                              (locking ordering   ; otherwise throws error!
                                (ldebug "Ordering " ordering)
                                (ldebug "Ordering " (ordering-edges))
                                ;          (transitive-directed-closure ordering :<)
                                ;          (transitive-undirected-closure ordering :=)
                                (ldebug "Transitive Ordering " (ordering-edges))
                                (check-finished)
                                )
                              (ldebug "unfinished" @unfinished)
                              ))
        add-bounds (fn [nodeid poi-data] 
                     (if-let [b (create-bounds nodeid poi-data)]
                         (dosync (alter bounds conj b))
                         nil))
          watch-rdistance (fn [pid _ _ _] 
                            (when-not (realized? return)
                              (ldebug "Watch for reverse " pid)
                              (if (not-nil? (add-bounds pid pois))
                                (graph-reasoning))
                              ))

watch-distance-helper (fn [hid _ _ _]
                        (when-not (realized? return)
                         (if (not-nil? (add-bounds hid helpers))
                                (graph-reasoning))
                         ))

watch-distance (fn [pid _ _ _] 
                 (when-not (realized? return)
                   (ldebug "Watch for " pid)
                   (add-bounds pid pois)
                   (graph-reasoning)
                   ))
_ (doseq [[pid p] pois] 
    (set-error-handler! (:dist p) err-handler-fn) 
    (set-error-handler! (:rdist p) err-handler-fn))
_ (ldebug "Start Routing from " (:poi-nodeids landmark-data) " to " s)


forward-search (future (sssp-forward-fn return s :driveable 
                                                (apply merge 
                                                       (for [ [pid p] pois  :when (dist-nil? p) ]
                                                         (do
                                                           (add-watch (:dist p) pid watch-distance)
                                                           {(:graph-vertex p) (:dist p) }))) 
                                                (apply merge 
                                                       (for [ [hid h] helpers :when (dist-nil? h) ]
                                                         (do
                                                           (add-watch (:dist h) hid watch-distance-helper)
                                                           {(:graph-vertex h) (:dist h) }))) 
                                                       
                                                       ) )


backward-search (future (sssp-reverse-fn return s :driveable 
                                                         (apply merge 
                                                                (for [ [pid p] pois  :when (dist-nil? p) ]
                                                                  (do
                                                                    (add-watch (:rdist p) pid watch-rdistance)
                                                                    {(:graph-vertex p) (:rdist p) })))
                                                         (apply merge
                                                                (for [ [hid h] helpers :when (dist-nil? h) ]
                                                                  (do
                                                                    (add-watch (:rdist h) hid watch-distance-helper)
                                                                    {(:graph-vertex h) (:rdist h) })))
                                                                ) )

]
(do
  (ldebug "Waiting for Forward Search to finish...")
  (ldebug "Forward-search " @forward-search)
  @forward-search ; Wait until forward-search is finished
  (ldebug "Forward Search finished.")
  (when (and (not (realized? return)) (some #(nil? @(:dist %)) (vals pois)))
    (doseq [p (vals pois) ] (await (:dist p)))
    (doseq [[pid p] pois :let [dist (:dist p)] :when (nil? @dist) ] 
      (ldebug "Switching nils to -1.")
      (remove-watch (:dist p) pid)
      (send dist (fn [_] -1)))
    (doseq [p (vals pois) ] (await (:dist p)))
    (graph-reasoning) ;twice as some expect that watches were not removed
    ;  (graph-reasoning)
    )
  (swap! (:sssp-completed landmark-data) conj
          (if @forward-search ; is forward-search interrupted?
             (max 
               (count @bounds)
               (count (filter #(do (await %) (not-nil? @%)) (map :dist (vals pois)))))
            poi-size))
 ; (future  ; can be made as future
    (if-not (or @forward-search @backward-search) ; both shalt not be interrupted
    (dosync
      (swap! (:cache-missed landmark-data) inc)
      (cache-landmark s landmark-data pois)
      (ldebug "Updating matrix")
      (ldebug "Data:" landmark-data)
      )
    );)

  (ldebug "Route FINISHED")
  (ldebug (for [[pid p] pois :let [dist (:dist p)]] (do ( agent-error dist) @dist)))
  (ldebug "dists" (for [[pid p] pois] (list pid @(:dist p))))
  (ldebug "rdists" (for [[pid p] pois] (list pid @(:rdist p))))
  (ldebug "CacheSize" (count @(:nodeids landmark-data)))
  @return))))



(def route-dijkstra 
  "Call the routing algorithm with Dijkstra" 
  (partial novel-routing dijkstra-dispatch-forward dijkstra-dispatch-reverse))

(def route-astar 
  "Call the routing algorihm with A*"
  (partial novel-routing astar-dispatch-forward astar-dispatch-reverse))
