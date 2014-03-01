(ns idron.sssp
  (:require [clojure.data.priority-map :as pm])
  [:use (idron tools tinkerpop)]
  (:import (clojure.lang Keyword)
           (com.tinkerpop.blueprints Vertex Edge Graph))
)(set! *warn-on-reflection* true)

(defn dijkstra 
  "Computes Dijkstra starting from node s to node t, while only edges labeled with label are considered."
  [^Vertex s ^Keyword label ^Vertex t]
  (let [g (ref (hash-map s 0))
        open (ref (clojure.data.priority-map/priority-map s (get @g s) ))
        closed (ref (hash-set))
        come-from (ref (hash-map))]
    (do
      (while (not (empty? @open))
        (let [v (key (peek @open))]
          (ldebug "Testing v " v)
          (if (= v t)
            (dosync
              (ref-set open (clojure.data.priority-map/priority-map))
              (ldebug "Found v " (get @g v))
              )
            (do
              (dosync
                (alter open pop)
                (alter closed conj v)
                )
              (doseq [w (out-vertices-of v label)]
                (let [tg (+ (get @g v) (cost v label w))]
                  (if (and 
                        (or (not-contains? @g w) (< tg (get @g w))) 
                        (not-contains? @closed w))
                    (dosync
                      (ldebug "Adjust w " w)
                      (alter g assoc w tg)
                      (alter come-from assoc w v)
                      (alter open assoc w (@g w))
                      )
                    )))))))
      (ldebug "Finished Dijkstra")
      (get @g t)
      )
    ))


(defn dijkstra-multi 
  "Multi-Target version of Dijkstra that stops when all targets are found or the search is exhausted."
  [^Vertex s ^Keyword label ts]
  (let [g (ref (hash-map s 0))
        open (ref (clojure.data.priority-map/priority-map s (get @g s) ))
        closed (ref (hash-set))
        unfinished (ref (set ts))
        come-from (ref (hash-map))]
    (do
      (while (not (empty? @open))
        (let [v (key (peek @open))]
          (ldebug "Testing v " v)
          (if (get @unfinished v)
            (dosync
              (ldebug "Found v " (get @g v))
              (alter unfinished disj v)
              (if (empty? @unfinished)
                (do
                  (ref-set open (clojure.data.priority-map/priority-map))
                  (ldebug "Found all")
                ))
              )
            (do
                  (small-delay)
              (dosync
                (alter open pop)
                (alter closed conj v)
                )
              (doseq [w (out-vertices-of v label)]
                (let [tg (+ (get @g v) (cost v label w))]
                  (if (and 
                        (or (not-contains? @g w) (< tg (get @g w))) 
                        (not-contains? @closed w))
                    (dosync
                      (ldebug "Adjust w " w)
                      (alter g assoc w tg)
                      (alter come-from assoc w v)
                      (alter open assoc w (@g w))
                      )
                    )))))))
      (ldebug "Finished Multi-Dijkstra")
      (for [t ts] (get @g t))
      )
    ))

(defn euklidean-distance [a b] (Math/sqrt (reduce + (map #(* % %) (map - a b)))))
(defn beeline
  "We use the beeline for sythetic graphs to measure distance between two points by the attributes :x and :y - 
  as :lat and :lon are set to zero."
  [^Vertex a ^Vertex b] (apply euklidean-distance (map #(vec [ (get-prop % :x) (get-prop % :y)]) [a b]  )))


(defn astar 
  "Computes A* starting from node s to node t, while only edges labeled with label are considered."
  [^Vertex s ^Keyword label ^Vertex t]
  (let [bound-f (partial beeline t) 
        f (ref (hash-map s (bound-f s)))
        g (ref (hash-map s 0))
        open (ref (clojure.data.priority-map/priority-map s (bound-f s) ))
        closed (ref (hash-set))
        come-from (ref (hash-map)) ]

    (do 
      (while (not (empty? @open))
        (let [v (key (peek @open))]
          (ldebug "Testing v " v)
          (if (= v t)
            (dosync
              (ref-set open (clojure.data.priority-map/priority-map))
              (ldebug "Found v " (get @g v))
              )
            (do
              (dosync
                (alter open pop)
                (alter closed conj v)
                )
              (doseq [w (out-vertices-of v label)]
                (let [tg (+ (get @g v) (cost v label w)) tf (+ tg (bound-f w)) ]
                  (if 
                    (or 
                      (and (not-contains? @closed w) (not-contains? @open w))
                      (< tg (get @g w)))
                    (dosync
                      (ldebug "Adjust w " w)
                      (alter g assoc w tg)
                      (alter f assoc w tf)
                      (alter come-from assoc w v)
                      (alter open assoc w tf)
                      ))))))))
      (ldebug "Finished A-Star")
      (get @g t)
      )))


;(defn astar-bound-forward [s t] (fn [v] (/ (- (beeline s t) (beeline s v) ) 2)))

;(defn beeline [^Vertex a ^Vertex b] (orthodromic-distance (coords a) (coords b)))

(defn astar-multi
  "For a multi-target A*, we have to update the heuristic each time a new target is found."
  [^Vertex s ^Keyword label ts] ;  ts = (map (vertex, ref dist))
  (let [g (ref (hash-map s 0))
        unfinished (ref (apply clojure.data.priority-map/priority-map 
                               (apply concat 
                                      (for [ vertex ts ]
                                        (list vertex (beeline s vertex))))))
        open (ref (clojure.data.priority-map/priority-map s 0 ))
        closed (ref (hash-set))
        come-from (ref (hash-map)) 
        f (ref (hash-map s 0)) 
        target (ref nil)
        bound-f (ref nil)
        update-target (fn []
                   (dosync
                     (ref-set target (key (peek @unfinished)))
                     (when-not (nil? @target)
                       (ref-set bound-f (partial beeline @target))
                       (doseq [w (keys @f)] (alter f assoc w (+ (get @g w) (@bound-f w))))
                       (doseq [vertex (keys @open)] (alter open assoc vertex (@bound-f vertex)))
                           ))) ]
    (do 
      (update-target)
      (while (not (empty? @open))
        (let [v (key (peek @open))]
          (ldebug "Testing v " v)


          (if (get @unfinished v)
            (dosync
              (ldebug "Found v " (get @g v))
              (alter unfinished dissoc v)
              (if (empty? @unfinished)
                (do
                  (ref-set open (clojure.data.priority-map/priority-map))
                  (ldebug "Found all")
                  ))
              (if (and (= target v) (not (empty? @open)))
                (do
                  (ldebug "Found also t!")
                  (update-target)
                  ))
              )
              (do
                (small-delay)
                (dosync
                  (alter open pop)
                  (alter closed conj v)
                  )
                (doseq [w (out-vertices-of v label)]
                  (let [tg (+ (get @g v) (cost v label w)) tf (+ tg (@bound-f w)) ]
                    (if 
                      (or 
                        (and (not-contains? @closed w) (not-contains? @open w))
                        (< tg (get @g w)))
                      (dosync
                        (ldebug "Adjust w " w)
                        (alter g assoc w tg)
                        (alter f assoc w tf)
                        (alter come-from assoc w v)
                        (alter open assoc w tf)
                        ))))))))
      (ldebug "Finished Multi-Astar")
      (for [t ts] (get @g t))
      ;Attention: After return of this function, we have to await for the value before dereferencing it!
      )))


(defn dijkstra-dispatch 
  "Asynchronous multi-target-dikjstra.
  When one of ts/notes is found, it calls the appropriate agent while updating the distance value.
  The return value is a promise that will not be set by the algorithm itself.
  Instead, whenever we wish to prematurely terminate the algorithm, we realize the promise.
  The algorithm returns the value of return, i.e., whether it was interrupted.
  notes and ts are from type (map(nodeid, agent dist))).
  Actually, ts is the set of POIs and notes the set of cached points. "
  [vertices-of-fn cost-fn return ^Vertex s ^Keyword label ts notes] 
  (let [g (ref (hash-map s 0))
        open (ref (clojure.data.priority-map/priority-map s (get @g s) ))
        closed (ref (hash-set))
        unfinished (ref (set (keys ts)))
        to-notify (ref (set (keys notes)))
        come-from (ref (hash-map))]
    (do
      (while (and (not (realized? return)) (not (empty? @open)))
        (let [v (key (peek @open))]
          (ldebug "Testing v " v)
          (if (get @unfinished v)
            (dosync
              (ldebug "Found v " (get @g v))
              (send (get ts v) (fn [_] (get @g v)))
              (alter unfinished disj v)
              (if (empty? @unfinished)
                (do
                  (ref-set open (clojure.data.priority-map/priority-map))
                  (ldebug "Found all")
                ))
              )
            (if (get @to-notify v)
              (dosync
                (send (get notes v) (fn [_] (get @g v)))
                (alter to-notify disj v)
                )
              (do
                (small-delay)
                (dosync
                  (alter open pop)
                  (alter closed conj v)
                  )
                (doseq [w (vertices-of-fn v label)]
                  (let [tg (+ (get @g v) (cost-fn v label w))]
                    (if (and 
                          (or (not-contains? @g w) (< tg (get @g w))) 
                          (not-contains? @closed w))
                      (dosync
                        (ldebug "Adjust w " w)
                        (alter g assoc w tg)
                        (alter come-from assoc w v)
                        (alter open assoc w (@g w))
                        )
                      ))))))))
        (ldebug "Finished Multi-Dijkstra")
        (realized? return)
       ; (for [t (vals ts)] (do (await t) @t))
        )
      ))




(defn astar-dispatch 
  "see dijkstra-dispatch"
  [vertices-of-fn cost-fn return ^Vertex s ^Keyword label ts notes]
  (let [g (ref (hash-map s 0))
        unfinished (ref (apply clojure.data.priority-map/priority-map 
                               (apply concat 
                                      (for [ vertex (keys ts) ]
                                        (list vertex (beeline s vertex))))))
        to-notify (ref (set (keys notes)))
        open (ref (clojure.data.priority-map/priority-map s 0 ))
        closed (ref (hash-set))
        come-from (ref (hash-map)) 
        f (ref (hash-map s 0)) 

        target (ref nil)
        bound-f (ref nil)

        update-target (fn []
                   (dosync
                     (ref-set target (key (peek @unfinished)))
                     (when-not (nil? @target)
                       (ref-set bound-f (partial beeline @target))
                       (doseq [w (keys @f)] (alter f assoc w (+ (get @g w) (@bound-f w))))
                       (doseq [vertex (keys @open)] (alter open assoc vertex (@bound-f vertex)))
                           ))) ]
    (do 
      (update-target)
      (while (and (not (realized? return)) (not (empty? @open)))
        (let [v (key (peek @open))]
          (ldebug "Testing v " v)


          (if (get @unfinished v)
            (dosync
              (ldebug "Found v " (get @g v))
              (send (get ts v) (fn [_] (get @g v)))
              (alter unfinished dissoc v)
              (if (empty? @unfinished)
                (do
                  (ref-set open (clojure.data.priority-map/priority-map))
                  (ldebug "Found all")
                  ))
              (if (and (= target v) (not (empty? @open)))
                (do
                  (ldebug "Found also t!")
                  (update-target)
                  ))
              )
            (if (get @to-notify v)
              (dosync
                (send (get notes v) (fn [_] (get @g v)))
                (alter to-notify disj v)
                )
              (do
                (small-delay)
                (dosync
                  (alter open pop)
                  (alter closed conj v)
                  )
                (doseq [w (vertices-of-fn v label)]
                  (let [tg (+ (get @g v) (cost-fn v label w)) tf (+ tg (@bound-f w)) ]
                    (if 
                      (or 
                        (and (not-contains? @closed w) (not-contains? @open w))
                        (< tg (get @g w)))
                      (dosync
                        (ldebug "Adjust w " w)
                        (alter g assoc w tg)
                        (alter f assoc w tf)
                        (alter come-from assoc w v)
                        (alter open assoc w tf)
                        )))))))))
      (ldebug "Finished A-Star")
      (realized? return)
      ;Attention: After return of this function, we have to await for the value before dereferencing it!
      )))

; For directed graphs we reverse the directions to generated distances for the way back.
(def dijkstra-dispatch-forward (partial dijkstra-dispatch out-vertices-of cost))
(def dijkstra-dispatch-reverse (partial dijkstra-dispatch in-vertices-of #(cost %3 %2 %1) ))
(def astar-dispatch-forward (partial astar-dispatch out-vertices-of cost))
(def astar-dispatch-reverse (partial astar-dispatch in-vertices-of #(cost %3 %2 %1) ))

