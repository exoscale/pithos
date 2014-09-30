(ns io.pithos.quickstart
  (:require [io.pithos             :refer [get-cli get-action]]
            [clojure.tools.logging :refer [info debug error]]
            [io.pithos.api    :as api]
            [io.pithos.schema :as schema]
            [io.pithos.loader :as loader]
            [io.pithos.system :as system]
            [io.pithos.config :as config])
  (:import com.datastax.driver.core.Statement
           org.apache.cassandra.service.EmbeddedCassandraService)
  (:gen-class))

(defn start-cassandra
  [path]
  (info "starting embedded cassandra daemon")

  (info "configuration path: " path)
  (System/setProperty "pithos.configuration" path)
  (System/setProperty "cassandra.config.loader" "io.pithos.PithosConfigLoader")
  (System/setProperty "cassandra.foreground" "yes")
  (System/setProperty "log4j.defaultInitOverride" "false")
  (let [s (EmbeddedCassandraService.)]
    (.start s)))

(defn startup
  [system]
  (schema/converge-schema system false)
  (api/run system))

(defn -main
  [& args]
  (let [[{:keys [path help action quiet]} args banner] (get-cli args)]
    (when help
      (println banner)
      (System/exit 0))

    (start-cassandra path)

    (debug "cassandra has been started, now starting pithos")

    (-> path
        (config/init quiet)
        (system/system-descriptor)
        (startup))
    nil))
