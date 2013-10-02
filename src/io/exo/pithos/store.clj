(ns io.exo.pithos.store
  (:require [qbits.alia            :as alia]
            [qbits.hayt            :refer [use-keyspace create-keyspace with]]
            [clojure.tools.logging :refer [debug]]))

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
  (debug "building cassandra store for: " cluster keyspace hints)
  (let [session (-> (alia/cluster cluster) (alia/connect))]
    (try (alia/execute session (use-keyspace keyspace))
         session
         (catch com.datastax.driver.core.exceptions.InvalidQueryException e
           ;;
           (if (re-find #"^[kK]eyspace.*does not exist$" (.getMessage e))
             (do (alia/execute session (create-keyspace keyspace (with hints)))
                 (alia/execute session (use-keyspace keyspace))
                 session)             
             (throw e))))))
