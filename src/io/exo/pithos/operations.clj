(ns io.exo.pithos.operations
  (:require [io.exo.pithos.response :refer [response xml-response]]
            [io.exo.pithos.store    :as store]
            [io.exo.pithos.bucket   :as bucket]
            [io.exo.pithos.xml      :as xml]))

(defn get-service
  "lists all bucket"
  [{{:keys [tenant]} :authorization} filestore]
  (store/execute filestore
    (-> (bucket/fetch tenant)
        (xml/list-all-my-buckets)
        (xml-response))))

(defn unknown
  "unknown operation"
  [req filestore]
  (-> (xml/unknown req)
      (xml-response)))

(def opmap
  {:get-service get-service})

(defn dispatch
  [{:keys [operation] :as request} filestore]
  (let [handler (get opmap operation unknown)]
    (handler request filestore)))


