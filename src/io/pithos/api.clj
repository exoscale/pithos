(ns io.pithos.api
  (:require [aleph.http            :as http]
            [lamina.core           :refer [enqueue]]
            [clojure.tools.logging :refer [info]]
            [io.pithos.operations  :refer [dispatch]]
            [io.pithos.request     :refer [prepare]]))

(defn run
  [{:keys [keystore bucketstore regions service options]}]
  (let [handler (fn [chan request] 
                  (-> (assoc request :chan chan)
                      (prepare keystore bucketstore regions options)
                      (dispatch bucketstore regions)))]
    (http/start-http-server handler service))
  (info "server up and running"))
