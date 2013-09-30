(ns io.exo.pithos.operations
  (:require [io.exo.pithos.response :refer [header response status
                                            xml-response request-id
                                            exception-status]]
            [io.exo.pithos.store    :as store]
            [io.exo.pithos.bucket   :as bucket]
            [io.exo.pithos.xml      :as xml]
            [clojure.tools.logging  :refer [debug info warn]]))

(defn get-service
  "lists all bucket"
  [{{:keys [tenant]} :authorization :as request} filestore]
  (store/execute filestore
    (-> (bucket/fetch tenant)
        (xml/list-all-my-buckets)
        (xml-response)
        (request-id request))))

(defn put-bucket
  [{{:keys [tenant]} :authorization :keys [bucket] :as request} filestore]
  (store/execute filestore (bucket/create! tenant bucket))
  (-> (response)
      (request-id request)
      (header "Location" (str "/" bucket))
      (header "Connection" "close")))

(defn delete-bucket
  [{{:keys [tenant]} :authorization :keys [bucket] :as request} filestore]
  (debug "delete! called on bucket " tenant bucket)
  (store/execute filestore (bucket/delete! tenant bucket))
  (-> (response)
      (request-id request)
      (status 204)))

(defn unknown
  "unknown operation"
  [req filestore]
  (-> (xml/unknown req)
      (xml-response)))

(def opmap
  {:get-service get-service
   :put-bucket  put-bucket
   :delete-bucket delete-bucket})

(defn ex-handler
  [request exception]
  (-> (xml-response (xml/exception request exception))
      (exception-status (ex-data exception))
      (request-id request)))

(defn dispatch
  [{:keys [operation] :as request} filestore]
  (let [handler (get opmap operation unknown)]
    (try (handler request filestore)
         (catch Exception e
           (warn "caught exception during operation: " (str e))
           (ex-handler request e)))))


