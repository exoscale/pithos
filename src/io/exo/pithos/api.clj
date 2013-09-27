(ns io.exo.pithos.api
  (:require [org.httpkit.server        :refer [run-server]]
            [io.exo.pithos.operations  :refer [dispatch]]
            [io.exo.pithos.request     :refer [prepare]]))

(defn run
  [{:keys [keystore datastore service options]}]
  (println "running with options: " options)
  (println "running with service: " service)
  (println "running with keystore: " keystore)
  (println "running with datastore: " datastore)
  (run-server #(-> % (prepare keystore datastore options) (dispatch datastore))
              (merge {:host "127.0.0.1" :port 8080} service)))
