(ns io.exo.pithos.operations
  (:require [io.exo.pithos.response :refer [header response status
                                            xml-response request-id
                                            content-type exception-status]]
            [io.exo.pithos.store    :as store]
            [io.exo.pithos.bucket   :as bucket]
            [io.exo.pithos.path     :as path]
            [io.exo.pithos.xml      :as xml]
            [clojure.tools.logging  :refer [debug info warn]]))

(defn get-region
  [regions region]
  (or (get regions region)
      (throw (ex-info (str "could not find region: " region)
                      {:status-code 500}))))

(defn get-service
  "lists all bucket"
  [{{:keys [tenant]} :authorization :as request} metastore]
  (store/execute metastore
    (-> (bucket/fetch tenant)
        (xml/list-all-my-buckets)
        (xml-response)
        (request-id request))))

(defn put-bucket
  [{{:keys [tenant]} :authorization :keys [bucket] :as request}
   metastore regions]
  (store/execute metastore (bucket/create! tenant bucket))
  (-> (response)
      (request-id request)
      (header "Location" (str "/" bucket))
      (header "Connection" "close")))

(defn delete-bucket
  [{{:keys [tenant]} :authorization :keys [bucket] :as request}
   metastore regions]
  (debug "delete! called on bucket " tenant bucket)
  (store/execute metastore (bucket/delete! tenant bucket))
  (-> (response)
      (request-id request)
      (status 204)))

(defn put-bucket-acl
  [{:keys [bucket body] :as request} metastore regions]
  (let [acl (slurp body)]
    (store/execute metastore (bucket/update! bucket {:acl acl}))
    (-> (response)
        (request-id request))))

(defn get-bucket-acl
  [{:keys [bucket] :as request} metastore regions]
  (store/execute metastore
                 (-> (bucket/fetch bucket)
                     :acl
                     (xml/default)
                     (xml-response)
                     (request-id request))))

(defn get-object
  [{:keys [bucket object] :as request} metastore regions]
  ;; get object !
  (-> (response "toto\n")
      (request-id request)
      (content-type "text/plain")))

(defn put-object
  [{:keys [bucket object] :as request} metastore regions]
  ;; put object !
  (-> (response)
      (request-id request)))


(defn head-object
  [{:keys [bucket object] :as request} metastore regions]
  (-> (response)
      (request-id request)))

(defn get-object-acl
  [{:keys [bucket object] :as request} metastore regions]
  (let [{:keys [region]} (store/execute metastore (bucket/fetch bucket))
        metatstore       (get-region regions region)]
    (-> (store/execute metastore (path/fetch bucket object))
        :acl
        (xml/default)
        (xml-response)
        (request-id request))))

(defn put-object-acl
  [{:keys [bucket object body] :as request} metastore regions]
  (let [{:keys [region]} (store/execute metastore (bucket/fetch bucket))
        metastore        (get-region regions region)
        acl              (slurp body)]
    (store/execute metastore (path/update! bucket object {:acl acl}))
    (-> (response)
        (request-id request))))

(defn delete-object
  [{:keys [bucket object] :as request} metastore regions]
  (let [{:keys [region]} (store/execute metastore (bucket/fetch bucket))
        metastore        (get-region regions region)]
    ;; delete object
    (-> (response)
        (request-id request))))

(defn unknown
  "unknown operation"
  [req metastore regions]
  (-> (xml/unknown req)
      (xml-response)))

(def opmap
  {:get-service {:handler get-service 
                 :perms   [:authenticated]}
   :put-bucket  {:handler put-bucket 
                 :perms   [[:memberof "authenticated"]]}
   :delete-bucket {:handler delete-bucket 
                   :perms [[:memberof "authenticated"]
                           [:bucket   :owner]]}
   :get-bucket-acl {:handler get-bucket-acl
                    :perms   [[:bucket "READ_ACP"]]}
   :put-bucket-acl {:handler put-bucket-acl
                    :perms [[:bucket "WRITE_ACP"]]}
   :get-object {:handler head-object
                :perms [[:object "READ"]]}
   :head-object {:handler head-object
                 :perms [[:object "READ"]]}
   :put-object {:handler put-object
                :perms   [[:bucket "WRITE"]]}
   :delete-object {:handler delete-object
                   :perms [[:bucket "WRITE"]]}
   :get-object-acl {:handler get-object-acl 
                    :perms [[:object "READ_ACP"]]}
   :put-object-acl {:handler put-object-acl 
                    :perms [[:object "WRITE_ACP"]]}})

(defmacro ensure!
  [pred]
  `(when-not ~pred
     (throw (ex-info "access denied" {}))))

(defn granted?
  [acl needs for]
  (= (get acl for) needs))

(defn bucket-satisfies?
  [{:keys [tenant acl]} & {:keys [for groups needs]}]
  (or (= tenant for)
      (granted? acl needs for)
      (some identity (map (partial granted? acl needs) groups))))

(defn object-satisfies?
  [{tenant :tenant} {acl :acl} & {:keys [for groups needs]}]
  (or (= tenant for)
      (granted? acl needs for)
      (some identity (map (partial granted? acl needs) groups))))

(defn authorize
  [{:keys [authorization bucket object] :as request} perms metastore regions]
  (let [{:keys [tenant memberof]} authorization
        memberof?                 (set memberof)]
    (doseq [[perm arg] (if (seq? perms) perms [perms])]
      (case perm
        :authenticated (ensure! (not= tenant :anonymous))
        :memberof      (ensure! (memberof? arg))
        :bucket        (ensure! (bucket-satisfies? (bucket/fetch bucket)
                                                   :for    tenant
                                                   :groups memberof?
                                                   :needs  arg))
        :object        (ensure! (object-satisfies? (bucket/fetch bucket)
                                                   (path/fetch bucket object)
                                                   :for    tenant
                                                   :groups memberof?
                                                   :needs  arg))))))

(defn ex-handler
  [request exception]
  (-> (xml-response (xml/exception request exception))
      (exception-status (ex-data exception))
      (request-id request)))

(defn dispatch
  [{:keys [operation] :as request} metastore regions]
  (let [{:keys [handler perms] :or {handler unknown}} (get opmap operation)]
    (try (authorize request perms metastore regions)
         (handler request metastore regions)
         (catch Exception e
           (warn "caught exception during operation: " (str e))
           (ex-handler request e)))))


