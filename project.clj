(defproject idron "0.0.1"
  :description "Inferring Distance Rankings on Networks"
  :dependencies [
                 [org.clojure/data.xml "0.0.7"]
                 [org.clojure/tools.nrepl "0.2.3"]
                 [org.clojure/tools.trace "0.7.5"]
                 [org.clojure/clojure "1.5.1"]
                 [com.tinkerpop.blueprints/blueprints-core "2.4.0"]
                 [com.tinkerpop.blueprints/blueprints-neo4j-graph "2.4.0"]
                 [org.clojure/tools.logging "0.2.6"]
                 [clj-logging-config "1.9.7"]
;                 [org.clojure/core.typed "0.2.19"] ; not yet used

                 [org.clojure/data.priority-map "0.0.4"]
                 [org.clojure/data.json "0.2.3"]
                 [org.clojars.gjahad/debug-repl "0.3.3"]
                 [com.tinkerpop.furnace/furnace "0.1.0-SNAPSHOT"]
                 [jung/jung "1.7.6"]
                 [com.tinkerpop.blueprints/blueprints-graph-jung "2.4.0"]
                 [incanter "1.5.4"]
                 ]
  :repositories {"typesafe" "http://repo.typesafe.com/typesafe/repo"}
  :main idron.core
  :aot [idron.core]
  :jvm-opts ["-Xmx10g" "-Xms6g"]

  )
