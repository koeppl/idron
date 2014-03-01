(ns idron.jung
  [:use idron.tinkerpop]
  [:use idron.osm]
  (:import (com.tinkerpop.blueprints.oupls.jung GraphJung)
           (javax.swing JFrame)
           (com.tinkerpop.blueprints Vertex Edge Graph)
           (edu.uci.ics.jung.visualization VisualizationViewer)
           (edu.uci.ics.jung.algorithms.layout CircleLayout DAGLayout Layout)
           (org.apache.commons.collections15 Transformer Factory)
           (java.awt Dimension) )
)(set! *warn-on-reflection* true)

(defn visualize-graph 
  "Visualizes a graph with the JUNG framework. The layout-klass is used for the shape and both transformers for the labeling."
  [layout-klass ^Transformer vertex-label-transformer ^Transformer edge-label-transformer ^Graph g ^String caption]
  (let [gj (GraphJung. g)
        ^Layout layout (clojure.lang.Reflector/invokeConstructor layout-klass (to-array [gj]))
        viz (VisualizationViewer. layout)
        render-context (.getRenderContext viz)
        frame (JFrame. caption) ]
    (do
      (doto viz
        (.setPreferredSize (Dimension. 500 350)))
      (doto render-context
          (.setEdgeLabelTransformer edge-label-transformer)
          (.setVertexLabelTransformer vertex-label-transformer))
      (doto frame
        (.. getContentPane (add viz))
        (.pack)
        (.setVisible true)))))

(def visualize-ordering 
  "Visualize a DAG. Needs the graph and a label as parameters."
  (partial visualize-graph DAGLayout
           (proxy [Transformer] []
             (transform [^Vertex v] 
               (vertex-id v)))
           (proxy [Transformer] []
             (transform [^Edge e]
               (name (get-label e ))))))

(def visualize-route 
  "Visualize a generated routing network. Needs the graph and a label as parameters."
  (partial visualize-graph CircleLayout
           (proxy [Transformer] []
             (transform [^Vertex v] 
               (str (node-id v))))
           (proxy [Transformer] []
             (transform [^Edge e]
               (str (get-prop e :driveable))))))
