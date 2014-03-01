(ns idron.readwritelock
  (:import (java.util.concurrent.locks ReentrantReadWriteLock))
)(set! *warn-on-reflection* true)

(defn ^ReentrantReadWriteLock read-write-lock 
  "Constructs a read/write lock"
  [] (ReentrantReadWriteLock.))

(defmacro do-locking 
  "internal - executes body while locking lock by calling method which to lock the lock."
  [which lock & body]
  `(do
    (.. ~lock ~which lock)
  (try
     ~@body
    (finally 
      (.. ~lock ~which unlock)))))

(defmacro do-read-locking 
  "Executes body while read locking. First argument is the lock, second the code to execute."
  [& body] `(do-locking readLock ~@body))

(defmacro do-write-locking 
  "Executes body while write locking. First argument is the lock, second the code to execute."
  [& body] `(do-locking writeLock ~@body))
