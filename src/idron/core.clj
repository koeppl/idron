(ns idron.core
  [:use (idron incanter osm evaluation munich)]
  (:gen-class)
  (:require [clojure.java.io :as io])
)(set! *warn-on-reflection* true)


(defn gen-plot-synthetic [] 
  (partial plot-synthetic
            [
             {:name "starfar" :data (star-far-testing) :x :graph-cols}
             {:name "star" :data (star-testing) :x :graph-cols}
             {:name "move" :data (move-testing) :x :s}
             {:name "line" :data (line-testing) :x :s}]))

(defn gen-plot-osm [db-path] 
  (do
    (munich-generate db-path)
    (partial plot-osm
             [
              {:name "munich" :data (munich-testing db-path) :x :s}
              ])))


(defn -main [& aargs]
  (if (= 0 (count aargs))
    (throw (Exception. "Parameters: (import [dbname] [osmfile] [from-scratch?] | plot [out-dir] )"))
    (let [args (rest aargs)]
      (case (first aargs)
        "import" 
        (if (not= 3 (count args))
          (throw (Exception. "Parameters: [dbname] [osmfile] [from-scratch?]"))
          (let [dbname (nth args 0)
                osm-stream (io/input-stream (nth args 1))
                from-scratch? (Boolean/valueOf ^String (nth args 2))]
            (import-osm dbname osm-stream from-scratch?)))
        "plot"
        (if (not= 1 (count args))
          (throw (Exception. "Parameters: [out-dir]"))
          (apply (gen-plot-synthetic) args))
        (throw (Exception. "First parameter not in list."))))))




