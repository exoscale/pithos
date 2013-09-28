(ns io.exo.pithos.operations
  (:require [io.exo.pithos.response :refer [header response
                                            xml-response request-id]]
            [io.exo.pithos.store    :as store]
            [io.exo.pithos.bucket   :as bucket]
            [io.exo.pithos.xml      :as xml]))

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
  (store/execute filestore (bucket/update! tenant bucket))
  (-> (response)
      (request-id request)
      (header "Location" (str "/" bucket))
      (header "Connection" "close")))

(defn unknown
  "unknown operation"
  [req filestore]
  (-> (xml/unknown req)
      (xml-response)))

(def opmap
  {:get-service get-service
   :put-bucket  put-bucket})

(defn dispatch
  [{:keys [operation] :as request} filestore]
  (let [handler (get opmap operation unknown)]
    (handler request filestore)))


