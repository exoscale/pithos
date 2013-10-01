(ns io.exo.pithos.store
  (:require [qbits.alia :as alia]
            [qbits.hayt :refer [use-keyspace create-keyspace with]]))

(defmacro execute
  [store & body]
  `(alia/with-session ~store
     (do ~@body)))

(defn cassandra-store
  "Connect to a cassandra cluster, and use a specific keyspace.
   When the keyspace is not found, try creating it"
  [{:keys [cluster keyspace hints]
    :or {hints {:replication {:class             "SimpleStrategy"
                              :replication_factor 1}}}}]
  (let [session (-> (alia/cluster cluster) (alia/connect))]
    (try (alia/execute session (use-keyspace keyspace))
         session
         (catch com.datastax.driver.core.exceptions.InvalidQueryException e
           ;;
           (if (re-find #"^[kK]eyspace.*does not exist$" (.getMessage e))
             (alia/execute session (create-keyspace keyspace (with hints)))
             (throw e))))))
