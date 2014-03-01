(ns idron.incanter
  (:require [clojure.string :as s])
  [:use (incanter core io datasets stats charts)]
)(set! *warn-on-reflection* true)

(defn gen-gnuplot 
  "Generates from an incanter dataset a data-string with xcol:ycol entries that can be used with gnuplot."
  [dat xcol ycol]
  (let [algo-names (vec (sort (set ($ :algo-name dat))))
        xvals (vec (sort (set ($ xcol dat))))
        gdat 
        (apply merge 
               (for [algo-name algo-names 
                     :let [v (to-vect ($ [xcol ycol] ($where {:algo-name algo-name} dat)))] ] 
                 (hash-map algo-name 
                           (apply merge
                                         (map #(hash-map (first %) (second %) ) v) )))) ]
    (do 
    (s/join "\n"
            (list
              (s/join "\t" (conj (conj (for [algo-name algo-names] algo-name) (name xcol) ) "#" ))
              (s/join "\n"
                      (for [xval xvals]
                        (str xval "\t"
                             (s/join "\t" 
                                     (for [algo-name algo-names]
                                       (get (get gdat algo-name) xval))
                                     )))))))))

(defn plot-synthetic
  "Plots the data to a filesystem-directory as both png and gnuplot files"
  [algo-data out-dir]
  (let [plotter (fn [x y title]  (line-chart x y :title title :group-by :algo-name :legend true))]
    (doseq [dat algo-data :let [dataset (to-dataset (:data dat))]]
      (with-data dataset 
        (save (plotter (:x dat) :time (:name dat)) (str out-dir "/" (:name dat) ".png"))
        (save (plotter (:x dat) :sssp-completed (:name dat)) (str out-dir "/" (:name dat) "sssp.png")))
      (save dataset (str out-dir "/" (:name dat) ".csv"))
      (spit (str out-dir "/" (:name dat) ".dat") (gen-gnuplot dataset (:x dat) :time) )
      (spit (str out-dir "/" (:name dat) "sssp.dat") (gen-gnuplot dataset (:x dat) :sssp-completed) ))))


(defn plot-osm
  "Plots the OSM-scenario"
  [algo-data out-dir]
  (let [plotter (fn [x y title]  (line-chart x y :title title :group-by :algo-name :legend true :auto-sort false))]
    (doseq [dat algo-data :let [dataset (to-dataset (:data dat))]]
      (with-data ($order :time :asc dataset) 
        (save (plotter (:x dat) :time (:name dat)) (str out-dir "/" (:name dat) ".png"))
      (save dataset (str out-dir "/" (:name dat) ".csv"))
      (spit (str out-dir "/" (:name dat) ".dat") (gen-gnuplot dataset (:x dat) :time) )))))

