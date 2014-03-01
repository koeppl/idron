(ns idron.tools
  (:require [clojure.tools.logging :as l]
            [clj-logging-config.log4j :as lc] )
)(set! *warn-on-reflection* true)

(defn not-nil? [x] (not (nil? x)))

(defn ^:dynamic small-delay []
   )
;  (Thread/sleep 0 1))
;  (take 20 (iterate (fn [a] (Math/sin a)) 1)))

(def DEBUG false)
(def INFO false)
;(def WARN false)
(def WARN true)

(defmacro linfo [& body]
   `(when INFO
           (l/info ~@body)))

(defmacro lwarn [& body]
   `(when WARN
           (l/warn ~@body)))

(defmacro ldebug [& body]
   `(when DEBUG
           (l/debug ~@body)))

(def not-contains? (comp not contains?))
