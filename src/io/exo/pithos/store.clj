(ns io.exo.pithos.store
  (:require [qbits.alia :as alia]))

(defmacro execute
  [store & body]
  `(alia/with-session ~store
     (do ~@body)))

(defn cassandra-store
  [{:keys [cluster keyspace]}]
  (-> (alia/cluster cluster)
      (alia/connect keyspace)))
