(ns io.pithos.api
  "Our main HTTP facade. Serving functionality is provided by aleph"
  (:require [aleph.http            :as http]
            [lamina.core           :refer [enqueue]]
            [clojure.tools.logging :refer [info]]
            [io.pithos.operations  :refer [dispatch]]
            [io.pithos.request     :refer [prepare]]))

(defn run
  "Run an asynchronous API handler through Netty thanks to aleph http.
   The request handler is an anonymous function which stores the channel
   inside the request to mimick the operations of http-kit then runs
   several wrappers defined in `io.pithos.api.request` before letting
   `io.pithos.operations` dispatch based on the type of request"
  [{:keys [keystore bucketstore regions service options]}]
  (let [handler (fn [chan request] 
                  (-> (assoc request :chan chan)
                      (prepare keystore bucketstore regions options)
                      (dispatch bucketstore regions)))]
    (http/start-http-server handler service))
  (info "server up and running"))
