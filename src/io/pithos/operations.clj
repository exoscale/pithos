(ns io.pithos.operations
  (:require [io.pithos.response     :refer [header response status
                                            xml-response request-id
                                            content-type exception-status]]
            [org.httpkit.server     :refer [with-channel send!]]
            [io.pithos.store        :as store]
            [io.pithos.bucket       :as bucket]
            [io.pithos.meta         :as meta]
            [io.pithos.blob         :as blob]
            [io.pithos.xml          :as xml]
            [qbits.alia.uuid        :as uuid]
            [clojure.tools.logging  :refer [debug info warn error]]))

(defn get-region
  [regions region]
  (or (get regions region)
      (throw (ex-info (str "could not find region: " region)
                      {:status-code 500}))))

(defn get-service
  "lists all bucket"
  [{{:keys [tenant]} :authorization :as request} bucketstore regions]
  (-> (bucket/by-tenant bucketstore tenant)
      (xml/list-all-my-buckets)
      (xml-response)
      (request-id request)))

(defn put-bucket
  [{{:keys [tenant]} :authorization :keys [bucket] :as request}
   bucketstore regions]
  (bucket/create! bucketstore tenant bucket {})
  (-> (response)
      (request-id request)
      (header "Location" (str "/" bucket))
      (header "Connection" "close")))

(defn delete-bucket
  [{{:keys [tenant]} :authorization :keys [bucket] :as request}
   bucketstore regions]
  (debug "delete! called on bucket " tenant bucket)
  (bucket/delete! bucketstore bucket)
  (-> (response)
      (request-id request)
      (status 204)))

(defn get-bucket
  [{:keys [params bucket] :as request} bucketstore regions]
  (let [{:keys [region tenant] :as binfo} (bucket/by-name bucketstore bucket)
        {:keys [metastore]}               (get-region regions region)
        params (select-keys params [:delimiter :prefix])
        prefixes (meta/prefixes metastore bucket params)]
    (debug "got prefixes: " prefixes)
    (-> prefixes
        (xml/list-bucket binfo params)
        (xml-response)
        (request-id request))))

(defn put-bucket-acl
  [{:keys [bucket body] :as request} bucketstore regions]
  (let [acl (slurp body)]
    (bucket/update! bucketstore bucket {:acl acl})
    (-> (response)
        (request-id request))))

(defn get-bucket-acl
  [{:keys [bucket] :as request} bucketstore regions]
  (-> (bucket/by-name bucketstore bucket)
      :acl
      (xml/default)
      (xml-response)
      (request-id request)))

(defn get-object
  [{:keys [bucket object] :as request} bucketstore regions]
  ;; get object !
  (-> (response "toto\n")
      (request-id request)
      (content-type "text/plain")))

(defn put-object
  [{:keys [body bucket object] :as request} bucketstore regions]
  ;; put object !
  (let [{:keys [region]}                    (bucket/by-name bucketstore bucket)
        {:keys [metastore storage-classes]} (get-region regions region)
        {:keys [inode]}                     (meta/fetch metastore bucket object)
        blobstore                           (get storage-classes :standard)
        inode                               (or inode (uuid/random))
        version                             (uuid/time-based)]

    (debug "starting object upload for: " bucket object inode)
    (with-channel request chan
      (let [finalize! (fn [inode version size checksum]
                        (debug "finalizing object with details "
                               bucket object inode version size checksum)
                        (meta/update! metastore bucket object
                                      {:inode inode
                                       :version version
                                       :size size
                                       :checksum checksum
                                       :multi false
                                       :storageclass "standard"
                                       :acl "private"})
                        (send! chan (-> (response)
                                        (header "ETag" checksum)
                                        (request-id request))))]
        (blob/append-stream! blobstore inode version body finalize!)))))

(defn head-object
  [{:keys [bucket object] :as request} bucketstore regions]
  (-> (response)
      (request-id request)))

(defn get-object-acl
  [{:keys [bucket object] :as request} bucketstore regions]
  (let [{:keys [region]} (bucket/by-name bucketstore bucket)
        {:keys [metastore]}        (get-region regions region)]
    (-> (meta/fetch metastore bucket object)
        :acl
        (xml/default)
        (xml-response)
        (request-id request))))

(defn put-object-acl
  [{:keys [bucket object body] :as request} bucketstore regions]
  (let [{:keys [region]} (bucket/by-name bucketstore bucket)
        {:keys [metastore]} (get-region regions region)
        acl              (slurp body)]
    (meta/update! metastore bucket object {:acl acl})
    (-> (response)
        (request-id request))))

(defn delete-object
  [{:keys [bucket object] :as request} bucketstore regions]
  (let [{:keys [region]}    (bucket/by-name bucketstore bucket)
        {:keys [metastore]} (get-region regions region)]
    ;; delete object
    (meta/delete! metastore bucket object)
    (-> (response)
        (request-id request))))

(defn unknown
  "unknown operation"
  [req bucketstore regions]
  (-> (xml/unknown req)
      (xml-response)))

(def opmap
  {:get-service    {:handler get-service 
                    :perms   [:authenticated]}
   :put-bucket     {:handler put-bucket 
                    :perms   [[:memberof "authenticated-users"]]}
   :delete-bucket  {:handler delete-bucket 
                    :perms   [[:memberof "authenticated-users"]
                              [:bucket   :owner]]}
   :get-bucket     {:handler get-bucket
                    :perms   [[:bucket "READ"]]}
   :get-bucket-acl {:handler get-bucket-acl
                    :perms   [[:bucket "READ_ACP"]]}
   :put-bucket-acl {:handler put-bucket-acl
                    :perms   [[:bucket "WRITE_ACP"]]}
   :get-object     {:handler head-object
                    :perms   [[:object "READ"]]}
   :head-object    {:handler head-object
                    :perms   [[:object "READ"]]}
   :put-object     {:handler put-object
                    :perms   [[:bucket "WRITE"]]}
   :delete-object  {:handler delete-object
                    :perms   [[:bucket "WRITE"]]}
   :get-object-acl {:handler get-object-acl 
                    :perms   [[:object "READ_ACP"]]}
   :put-object-acl {:handler put-object-acl 
                    :perms   [[:object "WRITE_ACP"]]}})

(defmacro ensure!
  [pred]
  `(when-not ~pred
     (debug "could not ensure: " (str (quote ~pred)))
     (throw (ex-info "access denied" {:status-code 403
                                      :type        :access-denied}))))

(defn granted?
  [acl needs for]
  (= (get acl for) needs))

(defn bucket-satisfies?
  [{:keys [tenant acl]} {:keys [for groups needs]}]
  (debug "got tenant: " tenant ", and for: " for)
  (or (= tenant for)
      (granted? acl needs for)
      (some identity (map (partial granted? acl needs) groups))))

(defn object-satisfies?
  [{tenant :tenant} {acl :acl} {:keys [for groups needs]}]
  (debug "got tenant: " tenant ", and for: " for)
  (or (= tenant for)
      (granted? acl needs for)
      (some identity (map (partial granted? acl needs) groups))))

(defn authorize
  [{:keys [authorization bucket object] :as request} perms bucketstore regions]
  (let [{:keys [tenant memberof]} authorization
        memberof?                 (set memberof)]
    (doseq [[perm arg] (map (comp flatten vector) perms)]
      (debug "about to validate " bucket perm arg tenant memberof?)
      (case perm
        :authenticated (ensure! (not= tenant :anonymous))
        :memberof      (ensure! (memberof? arg))
        :bucket        (ensure! (bucket-satisfies?
                                 (bucket/by-name bucketstore bucket)
                                 {:for    tenant
                                  :groups memberof?
                                  :needs  arg}))
        :object        (ensure! (object-satisfies?
                                 (bucket/by-name bucketstore bucket)
                                 nil
                                 {:for    tenant
                                  :groups memberof?
                                  :needs  arg}))))))

(defn ex-handler
  [request exception]
  (-> (xml-response (xml/exception request exception))
      (exception-status (ex-data exception))
      (request-id request)))

(defn dispatch
  [{:keys [operation] :as request} bucketstore regions]
  (let [{:keys [handler perms] :or {handler unknown}} (get opmap operation)]
    (try (authorize request perms bucketstore regions)
         (info "request-now: " (with-out-str (clojure.pprint/pprint request)))
         (handler request bucketstore regions)
         (catch Exception e
           (when-not (:type (ex-data e))
             (error e "caught exception during operation"))
           (ex-handler request e)))))


