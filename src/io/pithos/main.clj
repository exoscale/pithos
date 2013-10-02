(ns io.pithos.main
  (:gen-class)
  (:require [io.pithos.api     :as api]
            [io.pithos.schema  :as schema]
            [io.pithos.config  :as config]
            [clojure.tools.cli :refer [cli]]))

(defn get-action
  [action]
  (let [amap {"api-run"        api/run
              "install-schema" schema/converge-schema}]
    (or (get amap action)
        (do (println "unknown action: " action)
            (System/exit 1)))))

(defn get-cli
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
  [& args]
  (let [[{:keys [path help action quiet]} args banner] (get-cli args)]
    (when help
      (println banner)
      (System/exit 0))
    (-> path (config/init quiet) action))
  nil)

