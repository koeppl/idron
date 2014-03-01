(ns idron.synthesized
  [:use (idron tinkerpop osm)]
  (:import (com.tinkerpop.blueprints Graph Vertex Edge)
           (com.tinkerpop.furnace.generators Distribution PowerLawDistribution NormalDistribution CommunityGenerator EdgeAnnotator VertexAnnotator))
)(set! *warn-on-reflection* true)

(defn annotate-vertex 
  "Creates minimal attributes of a vertex used in sythensized graphs."
  [^Vertex v ^Long id lat lon]
  (set-props v  {:nodeid id :lat lat :lon lon :x lon :y lat :vid (gen-vertex-node-id id)}))

(def ^EdgeAnnotator edge-annotator 
  "We use equi-distant edges (distance = 1) for synthesized graphs."
  (proxy [EdgeAnnotator]  []
    (annotate [^Edge e]
      (set-prop e :distance 1  ))))

(def ^VertexAnnotator vertex-annotator 
  "For synthesized graphs, we do not use the orthodromic/spheroid distance and hence keep latitude/longitude at zero."
  (proxy [VertexAnnotator]  []
    (annotate [^Vertex v m]
      (let [id (read-string (.getId v))]
        (annotate-vertex v id 0 0)))))



(defn generate-line-graph 
  "Generate a graph which can be drawn as a line with points on the line as vertices."
  [^Graph g vertex-count]
  (do
    (doseq [i (range vertex-count)] 
      (if (nil? (.getVertex g i))
        (.annotate vertex-annotator (add-vertex g i) nil)))
    (doseq [pair (partition 2 1 (range vertex-count))]
      (.annotate edge-annotator 
                 (add-edge g
                           (get-vertex g (first pair))
                           :driveable
                           (get-vertex g (second pair))
                           )))))

(defn matrix-element-id 
  "Used for sythesized matrix-graphs. We map the 1D-node-identifier to a 2D-matrix by using the number of rows."
  [row-count]
  (fn [x y] (+ y (* row-count x))))

(defn generate-lattice-graph 
  "This graph can be drawn as a lattice of the size row-count/col-count. 
  There, nodes are only connected horizontal and vertical, but not diagonal."
  [^Graph g row-count col-count]
  (let [gen-id (matrix-element-id row-count) ]
    (doseq [x (range col-count) y (range row-count)
            :let [vid (gen-id x y) v (add-vertex g vid)] ]
      (annotate-vertex v vid x y)
      (when (pos? x) (.annotate edge-annotator (add-edge g (get-vertex g (gen-id (dec x) y)) :driveable v)))
      (when (pos? y) (.annotate edge-annotator (add-edge g (get-vertex g (gen-id x (dec y))) :driveable v))))))


(defn generate-star-graph 
  "This graph can be drawn as a star.
  Vertex with vid=0 is in the center and connceted with every axis that can be represented by a single straight line."
  [^Graph g row-count col-count]
  (let [gen-id (matrix-element-id row-count) ]
    (doseq [x (range col-count) y (range row-count)
            :let [vid (gen-id x y) v (add-vertex g vid)] ]
      (annotate-vertex v vid x y)
      (when (pos? x) (.annotate edge-annotator (add-edge g (get-vertex g (gen-id (dec x) y)) :driveable v)))
      (when (and (pos? y) (zero? x)) (.annotate edge-annotator (add-edge g (get-vertex g (gen-id 0 0)) :driveable v))))))

(defn generate-community [^Graph g vertex-count edge-count community-count std-deviation gamma cross-percentage]
  (do
    (doseq [i (range vertex-count)] 
      (if (nil? (.getVertex g i))
        (add-vertex g i)))
    (doto (CommunityGenerator. (name :driveable) edge-annotator vertex-annotator)
      (.setCommunityDistribution (NormalDistribution. std-deviation))
      (.setDegreeDistribution (PowerLawDistribution. gamma))
      (.setCrossCommunityPercentage cross-percentage)
      (.generate g community-count edge-count))
    ))
