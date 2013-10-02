(ns io.pithos.api
  (:require [org.httpkit.server    :refer [run-server]]
            [clojure.tools.logging :refer [info]]
            [io.pithos.operations  :refer [dispatch]]
            [io.pithos.request     :refer [prepare]]))

(defn run
  [{:keys [keystore metastore regions service options]}]
  (run-server #(-> %
                   (prepare keystore metastore regions options)
                   (dispatch metastore regions))
              service)
  (info "server up and running"))
