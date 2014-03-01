(ns idron.osm
  (:require [clojure.xml :as xml] 
            [clojure.zip :as zip]
            [clojure.string :as s])
  [:use (idron tools tinkerpop)]
  (:import java.lang.Math
           (com.tinkerpop.blueprints Vertex Edge Graph))
)(set! *warn-on-reflection* true)

(def ^Double earth-radius "The earth radius in km" 6371.009)

(defn orthodromic-distance [a b]
  "Big circle distance between two points a and b on a sphere. Points have :lat and :lon attributes in radians."
  (* earth-radius
  (Math/acos
     (+ 
       (* (Math/sin (:lat a))  (Math/sin (:lat b)))
       (* (Math/cos (:lat a))
          (Math/cos (:lat b))
          (Math/cos (- (:lon b) (:lon a)))
          )))))

(defn mercator [a] {:x (* earth-radius (Math/toRadians (:lon a)))
                    :y (* earth-radius
                            (Math/log
                              (Math/tan
                                (+ 
                                  (/ Math/PI 4)
                                  (/ (Math/toRadians (:lat a)) 2)
                                  )))) })

;We distinguish between node-vertices and way-vertices. 

;Because we use the single attribute 'vid' (for vertex id) as primary key on the vertices, we add 'n' and 'w' as a prefix to denote a node or way-vertex, respectively
(defn gen-vertex-node-id
  "node-vertices are actual points on the surface on the earth with :lon and :lat attributes."
  [^Long id] 
  (str "n" id))

(defn gen-vertex-way-id 
  "Ways consists of multiple nodes that are connected by the same street segment.
  These nodes grouped together will give us a way-node, i.e. a vertex which edges point to all nodes of the same street segment."
  [^Long id] 
  (str "w" id))

(defn node-by-id [^Graph g ^Long id] 
  (first (iterator-seq (.iterator (.getVertices g "vid" (gen-vertex-node-id id))))))
  
(defn node-id [^Vertex v] (get-prop v :nodeid))

(defn coords 
  "Turns a node to a point that stores latitude and longitude in radians"
  [^Vertex v] {:lon (Math/toRadians (get-prop v :lon)) :lat (Math/toRadians (get-prop v :lat)) })


(defn create-arc 
  "An arc is the edge that connects two nodes of a routing network. It stores :distance and :maxspeed as cost factors."
  [^Graph g ^Vertex way tags ^Vertex a ^Vertex b]
  (let [maxspeed 
        {:maxspeed (let [maxspeed (:maxspeed tags)]
                     (if (not-nil? maxspeed)
                       (if (re-find #"mph" maxspeed)
                         (* (read-string (s/trim (s/replace maxspeed "mph" ""))) 1.609)
                         (read-string maxspeed)
                         )
                       0
                       ))} 
        props (merge maxspeed {:distance (orthodromic-distance (coords a) (coords b))}) ]
    (do
      (linfo "Create arc from " (.getId a) " to " (.getId b))

      (doseq [d
              (merge
                     (case (:oneway tags)
                       :yes {:from a :to b}
                       :-1  {:from b :to a}
                       (if (or (= (:highway tags) "motorway") (= (:junction tags) "roundabout"))
                         {:from a :to  b} ;implicite yes
                         [ {:from a :to b},
                           {:from b :to a}] )))]
        (let [ edge (add-edge g (:from d) (if (contains? tags "highway") :driveable :osmconnected) (:to d))]
            (set-props edge props)
        )
      ))
  ))

(defn osm-tags 
  "OSM stores tags in a key-value list instead of name/value-attributes. 
  Hence we iterate through the key-value list of a OSM-XML-node and create a map."
  [xml-node]
  (apply merge
         (for [subnode (:content xml-node)
               :let [subnode-attrs (:attrs subnode)]
               :when (= :tag (:tag subnode))]
           { (:k subnode-attrs) (:v subnode-attrs) } 
           ))) 

(defn import-osm 
  "Parses an osm-xml-input file and stores it in a Neo4J-graph located at db-path. 
  If the database already contains OSM data collected by this method, do not use from-scratch?."
  [ ^String db-path osm-input-stream ^Boolean from-scratch?]
  (with-graph [g (create-batch-graph-indexed (create-neo4j-graph db-path) :vid)]
    (do
    (.setLoadingFromScratch g from-scratch?)
    (loop [nodes (:content (get (zip/xml-zip (xml/parse osm-input-stream)) 0))]
      (let [^Vertex node (first nodes) node-attrs-num #(read-string (%1 (:attrs node))) ] 
        (case (:tag node) 
          :node (let [^Long nodeid (node-attrs-num :id) tags (osm-tags node) 
                      ^Vertex node-vertex ((if from-scratch? add-vertex get-or-add-vertex) g (gen-vertex-node-id nodeid))
                      node-tags {:nodeid nodeid :lat (node-attrs-num :lat) :lon (node-attrs-num :lon)}
                      mercator-tags (mercator node-tags)
                      ]
                  (do
                    (set-props node-vertex (merge tags node-tags mercator-tags))
                    (linfo "Create node" nodeid)
                  ))
          :way (let [^Long wayid (node-attrs-num :id) tags (osm-tags node) way-key-id (gen-vertex-way-id wayid)]
                 (if (or from-scratch? (nil? (get-vertex g way-key-id)))
                   (let [^Vertex way-vertex (add-vertex g way-key-id)
                         ; In real-data there are often non-referenced nodes, hence we have to filter the nils out
                         subnodes (filter not-nil? (for [way-subnode (:content node) 
                                        :let [way-subnode-attrs-num #(read-string (%1 (:attrs way-subnode)))]
                                        :when (= :nd (:tag way-subnode))]
                                    (do
                                      (linfo "Way has node " (way-subnode-attrs-num :ref))
                                      (get-vertex g (gen-vertex-node-id (way-subnode-attrs-num :ref)))
                                      )))
                         ]
                     (do
;                       (debug-repl)
                       (linfo "Way has tags " tags)
                       (set-props way-vertex  (apply merge tags {:wayid wayid}))
                       ; In real-data there are often non-referenced nodes, hence we have to filter the nils out
                       (dorun (map (partial apply (partial create-arc g way-vertex tags)) (partition 2 1 subnodes)))
                       (dorun (map (partial add-edge g way-vertex :way) subnodes))
                       ))))
          nil
          )
        )
        (when (> (count nodes) 1)
        (recur (rest nodes)))
      )
  ))
)
