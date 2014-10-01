(ns io.pithos.store
  "Generic cassandra cluster connection services."
  (:import com.datastax.driver.core.exceptions.InvalidQueryException)
  (:require [qbits.alia            :as alia]
            [qbits.hayt            :refer [use-keyspace create-keyspace with]]
            [clojure.tools.logging :refer [debug]]))

(defn cassandra-store
  "Connect to a cassandra cluster, and use a specific keyspace.
   When the keyspace is not found, try creating it"
  [{:keys [cluster keyspace hints repfactor]
    :or {hints {:replication {:class             "SimpleStrategy"
                              :replication_factor (or repfactor 1)}}}}]
  (debug "building cassandra store for: " cluster keyspace hints)
  (let [cluster (if (sequential? cluster) cluster [cluster])
        session (-> (alia/cluster {:contact-points cluster}) (alia/connect))]
    (try (alia/execute session (use-keyspace keyspace))
         session
         (catch clojure.lang.ExceptionInfo e
           (let [{:keys [exception]} (ex-data e)]
             (if (and (= (class exception) InvalidQueryException)
                      (re-find #"^[kK]eyspace.*does not exist$"
                               (.getMessage exception)))
               (do (alia/execute session (create-keyspace keyspace (with hints)))
                 (alia/execute session (use-keyspace keyspace))
                 session)
               (throw e)))))))
