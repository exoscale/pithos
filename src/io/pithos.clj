(ns io.pithos
  "
pithos: object storage daemon
=============================

Pithos is an object storage daemon with
pluggable implementation of storage
engines. See [pithos.io](http://pithos.io) for details.

The `io.pithos` namespace is only responsible for parsing
command line arguments, loading configuration and starting
up the appropriate action."
  (:gen-class)
  (:require [io.pithos.api     :as api]
            [io.pithos.schema  :as schema]
            [io.pithos.config  :as config]
            [io.pithos.system  :as system]
            [clojure.tools.cli :refer [cli]]))

(defn get-action
  "Figure out what the expected action is from the command-line."
  [action]
  (let [amap {"api-run"        api/run
              "install-schema" schema/converge-schema}]
    (or (get amap action)
        (do (println "unknown action: " action)
            (System/exit 1)))))

(defn get-cli
  "Parse command line arguments and ensure we return a proper structure."
  [args]
  (try
    (-> (cli args
             ["-h" "--help" "Show Help"
              :default false :flag true]
             ["-f" "--path" "Configuration file path"
              :default nil]
             ["-q" "--quiet" "Never output to stdout"
              :default false :flag true]
             ["-a" "--action" "Specify an action (api-run, install-schema)"
              :default "api-run"])
        (update-in [0 :action] get-action))
    (catch Exception e
      (println "Could not parse arguments: " (.getMessage e))
      (System/exit 1))))

(defn -main
  "Main startup path, parse command line arguments, then dispatch to
   appropriate action.

   Only two actions are available:

     - `api-run`: run the S3 api handler
     - `install-schema`: converge cassandra schema"
  [& args]
  (let [[{:keys [path help action quiet]} args banner] (get-cli args)]

    (when help
      (println banner)
      (System/exit 0))

    (-> path
        (config/init quiet)
        (system/system-descriptor)
        action))
  nil)
