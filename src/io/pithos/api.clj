(ns io.pithos.api
  (:require [org.httpkit.server    :refer [run-server]]
            [clojure.tools.logging :refer [info]]
            [io.pithos.operations  :refer [dispatch]]
            [io.pithos.request     :refer [prepare]]))

(defn run
  [{:keys [keystore bucketstore regions service options]}]
  (run-server #(-> %
                   (prepare keystore bucketstore regions options)
                   (dispatch bucketstore regions))
              service)
  (info "server up and running"))
