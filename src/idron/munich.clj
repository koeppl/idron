(ns idron.munich
  [:use (idron tinkerpop osm algo sssp tools evaluation tests)]
  [:use alex-and-georges.debug-repl] 
  (:require [clojure.java.io :as io])
  (:import (com.tinkerpop.blueprints Graph Vertex Edge))
)(set! *warn-on-reflection* true)



;(with-data
;  ($order :time :asc (to-dataset d)) 
;  (view (line-chart :s :time :group-by :algo-name :legend true :auto-sort false  )))

(defn munich-testing [db-path]
  (with-graph [g (read-only-graph (create-neo4j-graph db-path))]
    (let [node-ids [1659974155 814227811 737219042 2475788349 1487012462 1485915818 398692]
          nodes (map (partial node-by-id g) node-ids)
          landmark-data (create-landmark g node-ids)
          start-vertices (all-vertices g) ;[(node-by-id g 302734698)]
          collector (fn [f] 
                      (for [s start-vertices :when (and (not-nil? (node-id s)) (every? not-nil? (dijkstra-multi s :driveable nodes)))] 
                        (f (node-id s))  ))
          ]
;          s (node-by-id g 302734698) ]
;      (debug-repl)
      
      (doall
        (concat
          (collector #(measure-algorithm g landmark-data % {:name "novel" :fn route-dijkstra} "Dij1" (fn [_])))
          (collector #(measure-algorithm g landmark-data % {:name "novel" :fn route-dijkstra} "Dij2" (fn [_])))
          (collector #(measure-algorithm g landmark-data % {:name "novel" :fn route-dijkstra} "DijNoCache" clear-cache))
          (collector #(measure-algorithm g landmark-data % {:name "naive" :fn naive-routing-dijkstra} "Dij" (fn [_])))

;          (println (all-edges (route-dijkstra g landmark-data s)))

          ))
      
      )))

(defn munich-generate [db-path]
  (if (not (.exists (io/as-file db-path)))
    (import-osm db-path 
               (io/input-stream (str "http://api.openstreetmap.org/api/0.6/map?bbox=" "11.54,48.14,11.543,48.145"))
               true)))

(comment
  (def db-path "/home/niki/projects/db/mu")
  (def g (read-only-graph (create-neo4j-graph db-path)))
  (def node-ids [1659974155 814227811 737219042 2475788349 1487012462 1485915818 398692])
  (def landmark-data (create-landmark g node-ids))
  (def s (node-by-id g 302734698) )
  (def (novel-routing g landmark-data s))
  
  )
