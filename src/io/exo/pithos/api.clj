(ns io.exo.pithos.api
  (:require [org.httpkit.server        :refer [run-server]]
            [clojure.tools.logging     :refer [info]]
            [io.exo.pithos.operations  :refer [dispatch]]
            [io.exo.pithos.request     :refer [prepare]]))

(defn run
  [{:keys [keystore datastore service options]}]
  (run-server #(-> % (prepare keystore datastore options) (dispatch datastore))
              service)
  (info "server up and running"))
