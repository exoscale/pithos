(ns io.pithos.operations
  "The operations namespace maps an operation as figured out when
   preparing a request and tries to service it"
  (:require [io.pithos.response     :refer [header response status
                                            xml-response request-id
                                            content-type exception-status]]
            [io.pithos.util         :refer [piped-input-stream
                                            parse-uuid
                                            ->channel-buffer]]
            [clojure.core.async     :refer [go chan >! <!]]
            [clojure.tools.logging  :refer [debug info warn error]]
            [clojure.string         :refer [split]]
            [io.pithos.store        :as store]
            [io.pithos.bucket       :as bucket]
            [io.pithos.meta         :as meta]
            [io.pithos.blob         :as blob]
            [io.pithos.xml          :as xml]
            [io.pithos.system       :as system]
            [io.pithos.desc         :as desc]
            [io.pithos.stream       :as stream]
            [io.pithos.perms        :as perms]
            [qbits.alia.uuid        :as uuid]))

(defn assoc-targets
  [{:keys [bucket object] {:keys [uploadid]} :params :as req} system target]
  (case target
    :bucket (assoc req :bd (bucket/bucket-descriptor system bucket))
    :object (assoc req :od (desc/object-descriptor system bucket object))
    :upload (assoc req
              :od (desc/object-descriptor system bucket object)
              :upload-id (parse-uuid uploadid))
    req))
(defn get-service
  "Lists all buckets for  tenant"
  [{{:keys [tenant]} :authorization :as request} system]
  (-> (bucket/by-tenant (system/bucketstore system) tenant)
      (xml/list-all-my-buckets)
      (xml-response)))

(defn put-bucket
  "Creates a bucket"
  [{{:keys [tenant]} :authorization :keys [bucket] :as request} system]
  (bucket/create! (system/bucketstore system) tenant bucket {})
  (-> (response)
      (header "Location" (str "/" bucket))
      (header "Connection" "close")))

(defn delete-bucket
  "Deletes a bucket, only possible if the bucket isn't empty. The bucket
   should also be checked for in-progress uploads."
  [{:keys [bd bucket] :as request} system]
  (let [[nodes _] (meta/prefixes (bucket/metastore bd) bucket {})]
    (when (pos? (count nodes))
      (throw (ex-info "bucket not empty" {:type :bucket-not-empty
                                          :bucket bucket
                                          :status-code 409})))
    (bucket/delete! (system/bucketstore system) bucket)
    (-> (response)
        (status 204))))

(defn get-bucket
  "List bucket content. It's worth noting that S3 has no actual concept of
   directories, instead, a delimiter can be supplied, in which case results will
   be split between contents and prefixes"
  [{:keys [bd params bucket] :as request} system]
  (let [params (select-keys params [:delimiter :prefix])
        prefixes (meta/prefixes (bucket/metastore bd) bucket params)]
    (-> (xml/list-bucket prefixes bd params)
        (xml-response))))

(defn head-bucket
  [request system]
  (response))

(defn get-bucket-location
  [{:keys [bd bucket] :as request} system]
  (-> (xml/bucket-location (bucket/region bd))
      (xml-response)))

(defn put-bucket-acl
  "Update bucket acl"
  [{:keys [bucket body] :as request} system]
  (let [acl (slurp body)]
    (bucket/update! (system/bucketstore system) bucket {:acl acl})
    (response)))

(defn get-bucket-acl
  "Retrieve and display bucket ACL as xml"
  [{:keys [bucket bd] :as request} system]
  (-> (bucket/by-name (system/bucketstore system) bucket)
      :acl
      (xml/default)
      (xml-response)))

(defn get-bucket-uploads
  "List current uploads"
  [{:keys [bucket bd] :as request} system]
  (-> (meta/list-uploads (bucket/metastore bd) bucket)
      (xml/list-multipart-uploads bucket)
      (xml-response)))

(defn initiate-upload
  "Start a new upload"
  [{:keys [od bucket object params] :as request} system]
  (let [content-type        (get-in request
                                    [:headers "content-type"]
                                    "binary/octet-stream")
        upload-id           (uuid/random)]
    (meta/initiate-upload! (bucket/metastore od) bucket object
                           upload-id {"content-type" content-type})
    (-> (xml/initiate-multipart-upload bucket object upload-id)
        (xml-response))))

(defn get-object-acl
  "Retrieve and format object acl"
  [{:keys [od bucket object] :as request} system]
  (-> (meta/fetch (bucket/metastore od) bucket object)
      :acl
      (xml/default)
      (xml-response)))

(defn put-object-acl
  "Update object acl"
  [{:keys [od bucket object body] :as request} system]
  (let [acl (slurp body)]
    (meta/update! (bucket/metastore od) bucket object {:acl acl})
    (response)))

(defn get-bucket-policy
  "Retrieve object policy: always fails for now"
  [request system]
  (throw (ex-info "no such bucket policy" {:type :no-such-bucket-policy
                                           :bucket (:bucket request)
                                           :status-code 404})))

(defn get-bucket-versioning
  "Retrieve bucket versioning configuration"
  [{:keys [bucket bd] :as request} system]
  (-> (xml/get-bucket-versioning (bucket/versioned? bd))
      (xml-response)))

(defn put-bucket-versioning
  "No versioning support for now even though versions are stored"
  [{:keys [bucket] :as request} system]
  (response))

(defn head-object
  "Retrieve object information"
  [{:keys [bucket object od] :as request} system]
  (when-not (desc/init-version od)
    (throw (ex-info "no such key" {:type :no-such-key
                                   :status-code 404
                                   :bucket bucket
                                   :key object})))
  (-> (response)
      (content-type "application/binary")
      (header "ETag" (str "\"" (desc/checksum od) "\""))
      (header "Last-Modified" (str (:atime od)))
      (header "Content-Length" (str (desc/size od)))))

(defn delete-object
  "Delete current revision of objects."
  [{:keys [od bucket object] :as request} system]
  (meta/delete! (bucket/metastore od) bucket object)
  (blob/delete! (desc/blobstore od) od (desc/version od))
  (response))

(defn get-object
  "Retrieve object. A response is sent immediately, whose body
  is a piped input stream. The connected outputstream will be fed data
  on a different thread."
  [{:keys [od bucket object] :as request} system]

  (let [[is os]                   (piped-input-stream)]

    (debug "will stream " (desc/inode od) (desc/version od))
    (future ;; XXX: set up a dedicated threadpool
      (try
        (stream/stream-to od os)
        (catch Exception e
          (error e "could not completely write out: "))))
    (-> (response is)
        (content-type (desc/content-type od))
        (header "Content-Length" (desc/size od))
        (header "ETag" (str "\"" (desc/checksum od) "\"")))))

(defn put-object
  "Accept data for storage. The body of this function is a bit messy and
   needs to be reworked.

   There are two radically different upload scenario, a standard upload
   where the body of the request needs to be stored and requests containing
   an `x-amz-copy-source header` which contains a reference to another object to copy.

   In this case a new inode is created and data copied over, if permissions are
   sufficient for a copy.

   Otherwise, data is streamed in."
  [{:keys [od body bucket object authorization] :as request} system]
  (let [dst      od
        previous (desc/init-version dst)]

    (if-let [source (get-in request [:headers "x-amz-copy-source"])]

      ;; we're dealing with a copy object request
      (if-let [[prefix s-bucket s-object] (split source #"/" 3)]
        (if (seq prefix)
          (throw (ex-info "invalid" {:type :invalid-request :status-code 400}))
          (when (perms/authorize {:bucket s-bucket
                                  :object s-object
                                  :authorization authorization}
                                 [[:bucket :READ]]
                                 system)
            (let [src (desc/object-descriptor system s-bucket s-object)]
              (when-not (desc/init-version src)
                (throw (ex-info "no such key" {:type :no-such-key :key s-object})))
              (stream/stream-copy src dst))))
        (throw (ex-info "invalid" {:type :invalid-request :status-code 400})))

      ;; we're dealing with a standard object creation
      (do
        (debug "got headers: " (:headers request))
        (when previous
          (desc/increment! dst))
        (stream/stream-from body od)))

    ;; if a previous copy existed, kill it
    (when previous
      (blob/delete! (desc/blobstore dst) dst previous))

    (desc/save! dst)

    (-> (response)
        (header "ETag" (str "\"" (desc/checksum dst) "\"")))))

(defn abort-upload
  "Abort an ongoing upload"
  [{:keys [od upload-id bucket object] :as request} system]
  (doseq [{:keys [inode version]}
          (meta/list-upload-parts (bucket/metastore od)
                                  bucket
                                  object
                                  upload-id)]
    (blob/delete! (desc/blobstore od) inode version))
  (meta/abort-multipart-upload! (bucket/metastore od)
                                bucket
                                object
                                upload-id)
  (-> (response)
      (status 204)))

(defn get-upload-parts
  "Retrieve upload parts"
  [{:keys [od bucket object upload-id] :as request} system]
  (-> (meta/list-upload-parts (bucket/metastore od)
                              bucket
                              object
                              upload-id)
      (xml/list-upload-parts upload-id bucket object)
      (xml-response)))

(defn put-object-part
  "Insert a new part in a multi-part upload"
  [{:keys [od upload-id body bucket object] :as request} system]
  (let [{:keys [partnumber]} (:params request)
        pd                   (desc/part-descriptor system bucket object
                                                   upload-id partnumber)]
    (stream/stream-from body pd)
    (desc/save! pd)
    (-> (response)
        (header "ETag" (str "\"" (desc/checksum pd) "\"")))))

(defn complete-upload
  "To complete an upload, all parts are read and streamed to
   a new inode which will aggregate all content from parts.
"
  [{:keys [bucket object upload-id od] :as request} system]
  (let [[is os]   (piped-input-stream)
        previous  (desc/init-version od)
        push-str  (fn [type]
                    (.write os (-> (case type :block "\n" :chunk " " type)
                                   (.getBytes)))
                    (.flush os))
        etag      (promise)]
    (future
      (try
        (when previous
          (desc/increment! od))
        (let [parts (desc/part-descriptors system bucket object upload-id)
              od    (stream/stream-copy-parts parts od push-str)]
          (desc/save! od)

          (future
            ;; This can be time consuming
            (doseq [part parts]
              (meta/abort-multipart-upload! (bucket/metastore part)
                                            bucket
                                            object
                                            upload-id)
              (blob/delete! (desc/blobstore part) part (desc/version part)))
            (when previous
              (blob/delete! (desc/blobstore od) od previous)))

          (debug "all streams now flushed")
          (push-str (xml/complete-multipart-upload bucket object
                                                   (desc/checksum od)))
          (.flush os)
          (.close os))
        (catch Exception e
          (error e "error in multipart completion"))))
    (-> (response is)
        (content-type "application/xml")
        (header "X-Accel-Buffering" "no"))))

(defn unknown
  "unknown operation"
  [request system]
  (-> (xml/unknown request)
      (xml-response)
      (status 400)))

(def opmap
  "Map requests to handler with associated necessary
   permissions"
  {:get-service            {:handler get-service
                            :target  :service
                            :perms   [:authenticated]}
   :put-bucket             {:handler put-bucket
                            :perms   [[:memberof "authenticated-users"]]}
   :put-bucket-versioning  {:handler put-bucket-versioning
                            :target  :bucket
                            :perms   [[:bucket :WRITE]]}
   :get-bucket-versioning  {:handler get-bucket-versioning
                            :target  :bucket
                            :perms   [[:bucket :WRITE]]}
   :delete-bucket          {:handler delete-bucket
                            :target  :bucket
                            :perms   [[:memberof "authenticated-users"]
                                      [:bucket   :owner]]}
   :head-bucket            {:handler head-bucket
                            :target  :bucket
                            :perms [[:bucket :READ]]}
   :get-bucket             {:handler get-bucket
                            :target  :bucket
                            :perms   [[:bucket :READ]]}
   :get-bucket-acl         {:handler get-bucket-acl
                            :target  :bucket
                            :perms   [[:bucket :READ_ACP]]}
   :put-bucket-acl         {:handler put-bucket-acl
                            :target  :bucket
                            :perms   [[:bucket :WRITE_ACP]]}
   :get-bucket-location    {:handler get-bucket-location
                            :target  :bucket
                            :perms   [[:bucket :READ]]}
   :get-bucket-policy      {:handler get-bucket-policy
                            :target  :bucket
                            :perms   [[:bucket :READ_ACP]]}
   :get-bucket-uploads     {:handler get-bucket-uploads
                            :perms   [[:bucket :READ]]
                            :target  :bucket}
   :get-object             {:handler get-object
                            :target  :object
                            :perms   [[:object :READ]]}
   :head-object            {:handler head-object
                            :target  :object
                            :perms   [[:object :READ]]}
   :put-object             {:handler put-object
                            :target  :object
                            :perms   [[:bucket :WRITE]]}
   :delete-object          {:handler delete-object
                            :target  :object
                            :perms   [[:bucket :WRITE]]}
   :get-object-acl         {:handler get-object-acl
                            :target  :object
                            :perms   [[:object :READ_ACP]]}
   :put-object-acl         {:handler put-object-acl
                            :target  :object
                            :perms   [[:object :WRITE_ACP]]}
   :post-object-uploads    {:handler initiate-upload
                            :target  :object
                            :perms   [[:bucket :WRITE]]}
   :put-object-uploadid    {:handler put-object-part
                            :target  :upload
                            :perms   [[:bucket :WRITE]]}
   :delete-object-uploadid {:handler abort-upload
                            :target  :upload
                            :perms   [[:bucket :WRITE]]}
   :post-object-uploadid   {:handler complete-upload
                            :target  :upload
                            :perms   [[:bucket :WRITE]]}
   :get-object-uploadid    {:handler get-upload-parts
                            :target  :upload
                            :perms   [[:bucket :WRITE]]}})

(defn ex-handler
  "Wrap exceptions and report them correctly"
  [request exception]
  (-> (xml-response (xml/exception request exception))
      (exception-status (ex-data exception))
      (request-id request)))

(defn dispatch
  "Dispatch operation"
  [{:keys [operation exception] :as request} system]
  (when (not= operation :options-service)
    (debug "handling operation: " operation))

  (cond
   (= :error operation)
   (ex-handler request exception)

   request
   (let [{:keys [handler perms target]} (get opmap operation)
         handler                        (or handler unknown)]
     (try (perms/authorize request perms system)
          (-> request
              (assoc-targets system target)
              (handler system)
              (request-id request))
          (catch Exception e
            (when-not (:type (ex-data e))
              (error e "caught exception during operation"))
            (ex-handler request e))))))
