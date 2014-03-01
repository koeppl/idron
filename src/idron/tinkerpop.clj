(ns idron.tinkerpop
  [:use idron.tools]
  [:use alex-and-georges.debug-repl] 
  (:import (clojure.lang Keyword)
           (com.tinkerpop.blueprints Element Graph IndexableGraph Index TransactionalGraph
                                     ThreadedTransactionalGraph TransactionalGraph$Conclusion KeyIndexableGraph
                                     Vertex Parameter Edge Direction) 
           (com.tinkerpop.blueprints.impls.neo4j Neo4jGraph)
           (com.tinkerpop.blueprints.impls.neo4j.batch Neo4jBatchGraph)
           (com.tinkerpop.blueprints.impls.tg TinkerGraph
                                              TinkerGraphFactory)
           (com.tinkerpop.blueprints.util.wrappers.batch BatchGraph VertexIDType)
           (com.tinkerpop.blueprints.util.wrappers.readonly ReadOnlyGraph)
           (com.tinkerpop.blueprints.util.io.graphml GraphMLWriter) )
)(set! *warn-on-reflection* true)


(defn shutdown-graph 
  "Destructor for a graph or its underlying graph if the given graph is a proxy graph."
  [^Graph g]
  (if (instance? ReadOnlyGraph g)
    (.. ^ReadOnlyGraph g getBaseGraph shutdown)
    (.shutdown g)))

(defmacro with-graph
  "Takes a graph as first argument and closes it on exit."
  [bindings & body]
  {:pre [(vector? bindings) (even? (count bindings))] }
  (cond
    (= (count bindings) 0) `(do ~@body)
    (symbol? (bindings 0)) `(let ~(subvec bindings 0 2)
                              (try
                                (with-graph ~(subvec bindings 2) ~@body)
                                (finally (shutdown-graph ~(bindings 0) ))))
    :else (throw (IllegalArgumentException.
                   "with-open only allows Symbols in bindings"))))

(defn write-graphml [^Graph g ^java.io.OutputStream o] (GraphMLWriter/outputGraph g o))

(def vertex-class com.tinkerpop.blueprints.Vertex) 

;(t/ann read-only-graph [Graph -> Graph])
(defn ^Graph read-only-graph [^Graph g] (ReadOnlyGraph. g))

;(t/ann read-only-graph [Nothing -> Graph])
(defn ^Graph tinkergraph-inmem [] (TinkerGraph.))

(defn ^Graph create-neo4j-graph [^String path] (Neo4jGraph. path))

(defn ^Graph create-neo4j-batch-graph [^String path] (Neo4jBatchGraph. path))

(defn ^BatchGraph create-batch-graph 
  "Wraps a graph up in a BatchGraph for fast insertions." [^Graph graph] (BatchGraph. graph VertexIDType/STRING BatchGraph/DEFAULT_BUFFER_SIZE))

(defn ^BatchGraph create-batch-graph-indexed 
  "Wraphs a Graph with a primary key-index on k up in a BatchGraph."
  [^KeyIndexableGraph g ^Keyword k] 
  (do
    (.createKeyIndex g (name k) vertex-class (make-array Parameter 0) )
    (let [bg (create-batch-graph g)]
      (.setVertexIdKey bg (name k))
      bg)))

(defn property-keys 
  "Get all properties of an edge/vertex as a list"
  [^Element e] (map keyword (.getPropertyKeys e)))

(defn get-prop [^Element e k] (.getProperty e (name k)))

(defn set-prop [^Element e k v] (.setProperty e (name k) v))

(defn set-props [^Element e props] (doseq [[k v] props] (set-prop e k v)))

(defn ^Edge add-edge 
  ([^Graph g id ^Vertex u ^Keyword label ^Vertex v] (.addEdge g id u v (name label) ))
  ([^Graph g ^Vertex u ^Keyword label ^Vertex v] (add-edge g nil u label v)))

(defn ^Vertex add-vertex
  ([^Graph g id] (.addVertex g id))
  ([^Graph g] (.addVertex g nil)))

(defn ^Vertex get-vertex [^Graph g id] (.getVertex g id))

(defn ^Index get-or-add-vertex [^Graph g n] 
  (let [vertex (get-vertex g n)]
    (if (nil? vertex)
      (add-vertex g n)
      vertex)))

(defn vertex-id 
  "Gets the vertex id by lookup on the primary key. Maybe nil."
  [^Vertex v] (.getId v))


(defn edges-of
  ([^Vertex u ^Keyword label ^Direction dir] 
   (iterator-seq (.iterator 
                   (.getEdges u dir (into-array String [(name label)])) 
                   )))
  ([^Vertex u ^Direction dir]
   (iterator-seq (.iterator 
                   (.getEdges u dir (make-array String 0))
                   ))))

(defn undirected-edges-between
  ([^Vertex u ^Keyword label ^Vertex v] (filter 
                                          #(or (= v (.getVertex ^Edge % Direction/IN)) (= v (.getVertex ^Edge % Direction/OUT)))
                                          (edges-of u label Direction/BOTH)))
  ([^Vertex u ^Vertex v] (filter 
                           #(or (= v (.getVertex ^Edge % Direction/IN)) (= v (.getVertex ^Edge % Direction/OUT)))
                           (edges-of u Direction/BOTH))))
(defn edges-between
  ([^Vertex u ^Keyword label ^Vertex v] (filter #(= v (.getVertex ^Edge % Direction/IN)) (edges-of u label Direction/OUT)))
  ([^Vertex u ^Vertex v] (filter #(= v (.getVertex ^Edge % Direction/IN)) (edges-of u Direction/OUT))))

(defn edge-connected? [^Vertex u ^Vertex v] (pos? (count (undirected-edges-between u v))))

(def edge-between? (comp (partial < 0) (comp count edges-between)))

(defn out-edges-of
  ([^Vertex u ^Keyword label] (edges-of u label Direction/OUT))
  ([^Vertex u] (edges-of u Direction/OUT)))

(defn in-edges-of
  ([^Vertex u ^Keyword label] (edges-of u label Direction/IN))
  ([^Vertex u] (edges-of u Direction/IN)))

(def in-vertices-of (comp (partial map #(.getVertex ^Edge % Direction/OUT)) in-edges-of))

(def out-vertices-of (comp (partial map #(.getVertex ^Edge % Direction/IN)) out-edges-of))

(defn neighbor-vertices-of [^Vertex u ^Keyword label] (concat (in-vertices-of u label) (out-vertices-of u label)))

(defn all-edges [^Graph g ] (iterator-seq (.iterator (.getEdges g ))))

(defn all-vertices [^Graph g ] (iterator-seq (.iterator (.getVertices g ))))

(defn ^Keyword get-label [^Edge e] (keyword (.getLabel e)))

(defn edge-get-vertex [^Edge e ^Direction dir] (.getVertex e dir))

(defn cost 
  "If there is an edge between u and v, read its distance value as cost."
  [^Vertex u ^Keyword label ^Vertex v]
  (try
    (get-prop (first (edges-between u label v)) :distance)
    (catch NullPointerException e (debug-repl))))



(defn transitive-closure 
  "Generates the transitive closure G+ of a graph of all edges with label"
  [edges-between-fn ^Graph g ^Keyword label]
  (let [done (atom false)]
    (while (= @done false)
      (do
        (reset! done true)
       (doseq [edge-in (all-edges g) edge-out (all-edges g)
               :let [vertex-in (edge-get-vertex edge-in Direction/IN) vertex-out (edge-get-vertex edge-out Direction/OUT)]
               :when (and 
                       (= label (get-label edge-in))
                       (= label (get-label edge-out))
                       (= (edge-get-vertex edge-in Direction/OUT) (edge-get-vertex edge-out Direction/IN))
                       (= 0 count (edges-between-fn vertex-in label vertex-out) )
                       )
               ]
         (ldebug "OK" edge-in edge-out)
         (add-edge g vertex-in label vertex-out)
         (reset! done false)
         )
      ))
    ))

(def transitive-directed-closure (partial transitive-closure edges-between))
(def transitive-undirected-closure (partial transitive-closure undirected-edges-between))

(defn make-undirected [^Graph g ^Keyword label]
  (doseq [edge (all-edges g)
          :when (= label (get-label edge))
          :let [u (edge-get-vertex edge Direction/OUT)
                v (edge-get-vertex edge Direction/IN) ]]
          (if-not (edge-between? v label u)
            (let [e (add-edge g v label u)]
                (doseq [k (property-keys edge)]
                  (set-prop e k (get-prop edge k)) 
              ) ))))


(defn topological-sorting 
  "Topological sorts a DAG."
  [^Graph g ^Keyword label]
  (let [L (atom (list ))
        color (atom (hash-map))
        depth-search (fn depth-search [^Vertex v]
                       (do
                         (swap! color assoc v :gray)
                         (doseq [w (out-vertices-of v label)]
                           (if (not (contains? @color w))
                             (depth-search w)))
                         (swap! color assoc v :black)
                         (swap! L conj v)))]
    (do
      (doseq [u (all-vertices g)
            :when (not (contains? @color u))]
        (depth-search u)) 
      @L)))

(defn scoring 
  "We sort a graph topological and give the sorted vertices ascending numbers by its order."
  [^Graph g]
  (let [score (atom 0)
        l (partition 2 1 (topological-sorting g :<)) ] 
    (for [pair l
          :let [ ret (list (vertex-id (first pair)) @score)] ]
      (do
        (if-not (edge-between? (first pair) := (second pair))
          (swap! score inc))
        ret
        ))))
