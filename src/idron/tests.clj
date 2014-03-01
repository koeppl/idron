(ns idron.tests
  (:require [clojure.string :as s] )
  [:use clojure.test]
  [:use (idron tinkerpop synthesized osm tests algo sssp tools)]
  (:import (com.tinkerpop.blueprints Graph Vertex Edge))
)(set! *warn-on-reflection* true)

(defn time-str
  "Evaluates expr and prints the time it took.  Returns the value of expr. Takes an extra label."
  [label expr] 
  (do
    (prn label ": ")
    (time expr)))

(defn equal-ordering? 
  "Does this order only have equal edges?"
  [^Graph a]
  (let [edges (all-edges a)]
    (or
      (empty? edges)
      (every? #(= (get-label %) :=) edges)
    )))
  

(defn graph-equal? 
  "Checks if all drawn edges in a correspond with the edges of b and vice versa."
  [^Graph a ^String a-name ^Graph b ^String b-name]
  (do ;when-not (and (equal-ordering? a) (equal-ordering? b))
    (println b)
    (def err (atom (list)))
    (def avs (all-vertices a))
    (doseq [av avs :let [aid (vertex-id av) bv (get-vertex b aid) ]  ]
            (let [bs< (atom (set (for [b< (out-vertices-of bv :<)] (vertex-id b<)  )))]
              (doseq [a< (map vertex-id (out-vertices-of av :<)) ]
                (if (contains? @bs< a<)
                  (swap! bs< disj a<)
                  (swap! err conj (str a-name " has " aid "<" a< " but " b-name " not"))))
              (doseq [b< @bs<]
                (swap! err conj (str b-name " has " aid "<" b< " but " a-name " not"))))

            (let [bs= (atom (set (for [b= (neighbor-vertices-of bv :=)] (vertex-id b=) )))]
              (doseq [a= (map vertex-id (neighbor-vertices-of av :=)) ]
                (if (contains? @bs= a=)
                  (swap! bs= disj a=)
                  (swap! err conj (str a-name " has " aid "=" a= " but " b-name " not"))))
              (doseq [b= @bs=]
                (swap! err conj (str b-name " has " aid "=" b= " but " a-name " not")))) )
      (println (s/join "\n" @err))
      (empty? @err)
    ))

(defn naive-routing 
  "Generating a ranking without any proposed methods is done by calling just a forward search algorithm.
  When the SSSP-solver terminates, we can use the returned distances to generate the ordering."
  [sssp ^Graph g landmark-data ^Vertex s]
    
  (do 
    (def ordering (tinkergraph-inmem))
    (def sol
    (for [p (map list (:poi-nodeids landmark-data) 
                  (sssp s :driveable (for [nodeid (:poi-nodeids landmark-data)] (node-by-id g nodeid)))  )
          ]
      {:nodeid (first p) :dist (second p) :vertex (add-vertex ordering (first p)) }))

    (doseq [ {lid :nodeid ldist :dist l :vertex} sol]
      (doseq [ {rid :nodeid rdist :dist r :vertex} sol :when (and (< rid lid)  (not (edge-connected? l r) )) ]
              (cond 
                (and (nil? ldist) (nil? rdist) ) (add-edge ordering l := r)
                (and (nil? ldist) (not-nil? rdist) ) (add-edge ordering l :< r)
                (and (nil? rdist) (not-nil? ldist) ) (add-edge ordering r :< l)
                (< ldist rdist) (add-edge ordering l :< r) 
                (< rdist ldist) (add-edge ordering r :< l)
                :else (add-edge ordering l := r))
        ))
    ordering
    ))

(def naive-routing-dijkstra (partial naive-routing dijkstra-multi))
(def naive-routing-astar (partial naive-routing astar-multi))

;TODO: Generate tests!

(defn graph-test [^Graph g landmark-data s]
  (let [a (time-str "naive" (naive-routing-dijkstra g landmark-data s)) 
        b (time-str "novel dijkstra" (route-dijkstra g landmark-data s))
        c (time-str "novel astar" (route-astar g landmark-data s))
        ]
    (and
      (graph-equal? a "naive" b "novel-dij" )
      (graph-equal? a "naive" c "novel-astar" ))))

(defn sssp-test [s ts]
  "Tests whether all sssp-algorithm will return the same result."
  (let
    [d-val (for [t ts] (dijkstra s :driveable t))
     a-val (for [t ts] (astar s :driveable t))
     dm-val (dijkstra-multi s :driveable ts) ]
    (do
      (is (= d-val a-val dm-val))
      d-val
      )))

(defn generate-sample-pois [ts] 
  "The list of distances that are given to the callback algorithms are agents 
  that are called whenever its respective target point is found."
  (apply merge (for [v ts] {v (agent nil) })))

(defn run-sssp-dispatch [sssp-fn s ts hs]
  "Runs an SSSP algorithm in the dispatch variant. 
  ts is a list of targets (POIs) and hs a list of helpers (cached points)."
  (let [ts-data (generate-sample-pois ts)
        hs-data (generate-sample-pois hs) ]
    (do
      (sssp-fn (promise) s :driveable ts-data hs-data)
      (for [p (vals ts-data) ] 
        (do (await p) @p))
      )))
    
(defn sssp-dispatch-test [s ts hs]
  "Tests all SSSP solvers that are implemented in the dispatch variant."
  (let [a-val  (run-sssp-dispatch astar-dispatch-forward s ts hs)
        ar-val (run-sssp-dispatch astar-dispatch-reverse s ts hs)
        d-val  (run-sssp-dispatch dijkstra-dispatch-forward s ts hs)
        dr-val (run-sssp-dispatch dijkstra-dispatch-reverse s ts hs) ]
    (is (= a-val d-val))
    (is (= ar-val dr-val))
    a-val
    ))


(defn generate-community-testgraph 
  "Generates a sythesized community graph of the JUNG library."
  [vertex-size edge-size]
  (let [g (tinkergraph-inmem)]
    (do
      (generate-community g vertex-size edge-size 10 3 2.3 0.10)
      (make-undirected g :driveable)
      g)))

(defn generate-line-testgraph [vertex-size]
  (let [g (tinkergraph-inmem)]
    (do
      (generate-line-graph g vertex-size)
      (make-undirected g :driveable)
      g)))

(def test-edge-size 1000)
(def test-vertex-size 200)
(def test-node-count 20)
(def test-nodeids (range 0 200 (int (/ test-vertex-size test-node-count) )))

(defn generate-testgraphs []
  (list
    (generate-community-testgraph test-vertex-size test-edge-size) 
    (generate-line-testgraph test-vertex-size)))

(deftest sssp-testing
  (doseq [g (generate-testgraphs)
        :let [ nodes (for [nodeid test-nodeids]  (node-by-id g nodeid))] ]
    (doseq [id (range test-vertex-size)  :let [s (node-by-id g id)]] 
      (sssp-test s nodes)
      (sssp-dispatch-test s nodes nil)
      )))

(deftest novel-testing 
  (doseq [g (generate-testgraphs)
          :let [nodes (for [nodeid test-nodeids]  (node-by-id g nodeid))
                landmark-data (create-landmark g test-nodeids)] ]
    (doseq [id (range test-vertex-size)  :let [s (node-by-id g id)]] 
      (println "Testing with s = " id)
      (with-test-out
        (is (graph-test g landmark-data s)
            (str "Test failed with " " S: " id " POIS: " test-nodeids
                 " Dijkstra: " (s/join "," (dijkstra-multi s :driveable (for [nodeid (:poi-nodeids landmark-data)] (node-by-id g nodeid))))
                 )))
      )))
