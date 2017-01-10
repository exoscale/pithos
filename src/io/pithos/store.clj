(ns io.pithos.store
  "Generic cassandra cluster connection services."
  (:import com.datastax.driver.core.exceptions.InvalidQueryException)
  (:require [qbits.alia            :as alia]
            [qbits.hayt            :refer [use-keyspace create-keyspace with]]
            [clojure.tools.logging :refer [debug]]))

(defprotocol Convergeable
  (converge! [this]))

(defprotocol Crudable
  (fetch [this k] [this k1 k2] [this k1 k2 k3])
  (update! [this k v] [this k1 k2 v] [this k1 k2 k3 v])
  (delete! [this k] [this k1 k2] [this k1 k2 k3])
  (create! [this k v] [this k1 k2 v] [this k1 k2 k3 v]))

(defn cassandra-store
  "Connect to a cassandra cluster, and use a specific keyspace.
   When the keyspace is not found, try creating it"
  [{:keys [cluster keyspace hints repfactor username password] :as opts}]
  (debug "building cassandra store for: " cluster keyspace hints)
  (let [hints   (or hints
                    {:replication {:class             "SimpleStrategy"
                                   :replication_factor (or repfactor 1)}})
        copts   (dissoc opts :cluster :keyspace :hints :repfactor :username :password :use)
        cluster (if (sequential? cluster) cluster [cluster])
        session (-> (assoc copts :contact-points cluster)
                    (cond-> (and username password)
                      (assoc :credentials {:user username
                                           :password password}))
                    (alia/cluster)
                    (alia/connect))]
    (try (alia/execute session (use-keyspace keyspace))
         session
         (catch clojure.lang.ExceptionInfo e
           (let [{:keys [exception]} (ex-data e)]
             (if (and (= (class exception) InvalidQueryException)
                      (re-find #"^[kK]eyspace.*does not exist$"
                               (.getMessage exception)))
               (do (alia/execute session
                                 (create-keyspace keyspace (with hints)))
                 (alia/execute session (use-keyspace keyspace))
                 session)
               (throw e)))))))
