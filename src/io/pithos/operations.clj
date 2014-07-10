(ns io.pithos.operations
  "The operations namespace maps an operation as figured out when
   preparing a request and tries to service it"
  (:require [io.pithos.response     :refer [header response status
                                            xml-response request-id send!
                                            content-type exception-status]]
            [io.pithos.util         :refer [piped-input-stream
                                            parse-uuid iso8601-timestamp
                                            ->channel-buffer]]
            [lamina.core            :refer [channel siphon
                                            lazy-seq->channel
                                            enqueue close]]
            [clojure.tools.logging  :refer [debug info warn error]]
            [clojure.string         :refer [split]]
            [io.pithos.store        :as store]
            [io.pithos.bucket       :as bucket]
            [io.pithos.meta         :as meta]
            [io.pithos.blob         :as blob]
            [io.pithos.xml          :as xml]
            [qbits.alia.uuid        :as uuid]))

;;
;; ### acl utilities

(defmacro ensure!
  "Assert that a predicate is true, abort with access denied if not"
  [pred]
  `(when-not ~pred
     (debug "could not ensure: " (str (quote ~pred)))
     (throw (ex-info "access denied" {:status-code 403
                                      :type        :access-denied}))))

(defn granted?
  "Do current permissions allow for operation ?"
  [acl needs for]
  (= (get acl for) needs))

(defn bucket-satisfies?
  "Ensure sufficient rights for bucket access"
  [{:keys [tenant acl]} {:keys [for groups needs]}]
  (or (= tenant for)
      (granted? acl needs for)
      (some identity (map (partial granted? acl needs) groups))))

(defn object-satisfies?
  "Ensure sufficient rights for object accessp"
  [{tenant :tenant} {acl :acl} {:keys [for groups needs]}]
  (or (= tenant for)
      (granted? acl needs for)
      (some identity (map (partial granted? acl needs) groups))))

(defn authorize
  "Check permission to service operation, each operation has a list
   of needed permissions, any failure results in an exception being raised
   which prevents any further action from being taken."
  [{:keys [authorization bucket object]} perms bucketstore regions]
  (let [{:keys [tenant memberof]} authorization
        memberof?                 (set memberof)]
    (doseq [[perm arg] (map (comp flatten vector) perms)]
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
                                 nil ;; XXX please fix me
                                 {:for    tenant
                                  :groups memberof?
                                  :needs  arg}))))
    true))

(defn get-region
  "Fetch the regionstore from regions"
  [regions region]
  (or (get regions region)
      (throw (ex-info (str "could not find region: " region)
                      {:status-code 500}))))

(defn get-service
  "Lists all buckets for  tenant"
  [{{:keys [tenant]} :authorization :as request} bucketstore regions]
  (-> (bucket/by-tenant bucketstore tenant)
      (xml/list-all-my-buckets)
      (xml-response)
      (request-id request)
      (send! (:chan request))))

(defn put-bucket
  "Creates a bucket"
  [{{:keys [tenant]} :authorization :keys [bucket] :as request}
   bucketstore regions]
  (bucket/create! bucketstore tenant bucket {})
  (-> (response)
      (request-id request)
      (header "Location" (str "/" bucket))
      (header "Connection" "close")
      (send! (:chan request))))

(defn delete-bucket
  "Deletes a bucket, only possible if the bucket isn't empty. The bucket
   should also be checked for in-progress uploads."
  [{:keys [bucket] :as request}  bucketstore regions]
  (let [{:keys [region tenant]} (bucket/by-name bucketstore bucket)
        {:keys [metastore]}     (get-region regions region)
        [nodes _]               (meta/prefixes metastore bucket {})]
    (when (pos? (count nodes))
      (throw (ex-info "bucket not empty" {:type :bucket-not-empty
                                          :bucket bucket
                                          :status-code 409})))
    (bucket/delete! bucketstore bucket)
    (-> (response)
        (request-id request)
        (status 204)
        (send! (:chan request)))))

(defn get-bucket
  "List bucket content. It's worth noting that S3 has no actual concept of
   directories, instead, a delimiter can be supplied, in which case results will
   be split between contents and prefixes"
  [{:keys [params bucket] :as request} bucketstore regions]
  (let [{:keys [region tenant] :as binfo} (bucket/by-name bucketstore bucket)
        {:keys [metastore]}               (get-region regions region)
        params (select-keys params [:delimiter :prefix])
        prefixes (meta/prefixes metastore bucket params)]
    (-> (xml/list-bucket prefixes binfo params)
        (xml-response)
        (request-id request)
        (send! (:chan request)))))

(defn head-bucket
  [{:keys [params bucket] :as request} bucketstore regions]
  (let [{:keys [region tenant] :as binfo} (bucket/by-name bucketstore bucket)
        {:keys [metastore]}               (get-region regions region)
        params (select-keys params [:delimiter :prefix])
        prefixes (meta/prefixes metastore bucket params)]
    (-> (response)
        (request-id request)
        (send! (:chan request)))))

(defn get-bucket-location
  [{:keys [bucket] :as request} bucketstore regions]
  (let [{:keys [region] :as binfo} (bucket/by-name bucketstore bucket)]
    (-> (xml/bucket-location region)
        (xml-response)
        (request-id request)
        (send! (:chan request)))))

(defn put-bucket-acl
  "Update bucket acl"
  [{:keys [bucket body] :as request} bucketstore regions]
  (let [acl (slurp body)]
    (bucket/update! bucketstore bucket {:acl acl})
    (-> (response)
        (request-id request)
      (send! (:chan request)))))

(defn get-bucket-acl
  "Retrieve and display bucket ACL as xml"
  [{:keys [bucket] :as request} bucketstore regions]
  (-> (bucket/by-name bucketstore bucket)
      :acl
      (xml/default)
      (xml-response)
      (request-id request)
      (send! (:chan request))))

(defn get-object
  "Retrieve object. A response is sent immediately, whose body
  is a piped input stream. The connected outputstream will be fed data
  on a different thread."
  [{:keys [bucket object] :as request} bucketstore regions]

  (let [{:keys [region]}          (bucket/by-name bucketstore bucket)
        {:keys [metastore
                storage-classes]} (get-region regions region)
        {:keys [size checksum
                inode version
                metadata
                storage-class]}   (meta/fetch metastore bucket object)
        blobstore                 (get storage-classes :standard)
        ctype                     (get metadata "content-type")
        [is os]                   (piped-input-stream)]

    (future ;; XXX: run this in a dedicated threadpool
      (try
        (blob/stream!
         blobstore inode version
         (fn [chunks]
           (if chunks
             (doseq [{:keys [payload]} chunks
                     :let [btow (- (.limit payload) (.position payload))
                           ba   (byte-array btow)]]
               (.get payload ba)
               (.write os ba))
             (.close os))))
        (catch Exception e
          (error e "could not completely write out: "))))
    (-> (response is)
        (content-type ctype)
        (header "Content-Length" size)
        (header "ETag" checksum)
        (send! (:chan request)))))

(defn put-object
  "Accept data for storage. The body of this function is a bit messy and
   needs to be reworked.

   There are two radically different upload scenario, a standard upload
   where the body of the request needs to be stored and requests containing
   an `x-amz-copy-source header` which contains a reference to another object to copy.

   In this case a new inode is created and data copied over, if permissions are
   sufficient for a copy.

   Otherwise, data is streamed in."
  [{:keys [body bucket object authorization] :as request} bucketstore regions]
  (let [{:keys [region]}                    (bucket/by-name bucketstore bucket)
        {:keys [metastore storage-classes]} (get-region regions region)
        {:keys [inode] :as details}         (meta/fetch metastore bucket object
                                                        false)
        old-version                         (:version details)
        blobstore                           (get storage-classes :standard)
        inode                               (or inode (uuid/random))
        version                             (uuid/time-based)
        content-type                        (get-in request
                                                    [:headers "content-type"]
                                                    "binary/octet-stream")]

    (if-let [source (get-in request [:headers "x-amz-copy-source"])]
      (if-let [[prefix s-bucket s-object] (split source #"/" 3)]
        (if (seq prefix)
          (throw (ex-info "invalid" {:type :invalid-request :status-code 400}))
          (when (authorize {:bucket s-bucket
                            :object s-object
                            :authorization authorization}
                           [[:bucket :READ]]
                           bucketstore
                           regions)
            (let [bucket-details (bucket/by-name bucketstore s-bucket)
                  region-details (get-region regions (:region bucket-details))
                  details        (meta/fetch (:metastore region-details)
                                             s-bucket s-object)
                  s-meta         (or (:metadata details) {})
                  s-blobstore    (get (:storage-classes region-details)
                                      :standard)
                  body-stream    (channel)]

              (future
                (blob/stream!
                 s-blobstore (:inode details) (:version details)
                 (fn [chunks]
                   (if chunks
                     (doseq [{:keys [payload]} chunks]
                       (enqueue body-stream (->channel-buffer payload)))
                     (close body-stream)))))
              (future
                (blob/append-stream! blobstore inode version body-stream
                                     (fn [_ _ size checksum]
                                       (let [date (iso8601-timestamp)]
                                         (debug "done streaming, updating copy")
                                         (meta/update! metastore bucket object
                                                       {:inode inode
                                                        :version version
                                                        :size size
                                                        :checksum checksum
                                                        :atime date
                                                        :storageclass "standard"
                                                        :acl "private"
                                                        :metadata s-meta})
                                         (when old-version
                                           (blob/delete! blobstore inode old-version))

                                         (send! (-> (xml/copy-object checksum date)
                                                    (xml-response)
                                                    (request-id request))
                                                (:chan request)))))))))
        (throw (ex-info "invalid" {:type :invalid-request :status-code 400})))

      (let [finalize! (fn [inode version size checksum]
                        (meta/update! metastore bucket object
                                      {:inode inode
                                       :version version
                                       :size size
                                       :checksum checksum
                                       :storageclass "standard"
                                       :acl "private"
                                       :atime (iso8601-timestamp)
                                       :metadata {"content-type" content-type}})
                        (when old-version
                          (blob/delete! blobstore inode old-version))
                        (send! (-> (response)
                                   (header "ETag" (str "\"" checksum "\""))
                                   (request-id request))
                               (:chan request)))]
        (blob/append-stream! blobstore inode version body finalize!)))))

(defn get-bucket-uploads
  "List current uploads"
  [{:keys [bucket] :as request} bucketstore regions]
  (let [{:keys [region]}    (bucket/by-name bucketstore bucket)
        {:keys [metastore]} (get-region regions region)]
    (-> (meta/list-uploads metastore bucket)
        (xml/list-multipart-uploads bucket)
        (xml-response)
        (request-id request)
        (send! (:chan request)))))

(defn initiate-upload
  "Start a new upload"
  [{:keys [bucket object params] :as request} bucketstore regions]
  (let [{:keys [region]}    (bucket/by-name bucketstore bucket)
        {:keys [metastore]} (get-region regions region)
        uploadid            (uuid/random)
        content-type        (get-in request
                                    [:headers "content-type"]
                                    "binary/octet-stream")]
    (meta/initiate-upload! metastore bucket object
                           uploadid {"content-type" content-type})
    (-> (xml/initiate-multipart-upload bucket object uploadid)
        (xml-response)
        (request-id request)
        (send! (:chan request)))))

(defn abort-upload
  "Abort an ongoing upload"
  [{:keys [bucket object params] :as request} bucketstore regions]
  (let [{:keys [region]}                    (bucket/by-name bucketstore bucket)
        {:keys [metastore storage-classes]} (get-region regions region)
        blobstore                           (get storage-classes :standard)
        {:keys [uploadid]}                  params]
    (doseq [{:keys [inode version]}
            (meta/list-upload-parts metastore bucket object
                                    (parse-uuid uploadid))]
      (blob/delete! blobstore inode version))
    (meta/abort-multipart-upload! metastore bucket object uploadid)
    (-> (response)
        (request-id request)
        (status 204)
        (send! (:chan request)))))

(defn get-upload-parts
  "Retrieve upload parts"
  [{:keys [body bucket object params] :as request} bucketstore regions]
  (let [{:keys [region]}    (bucket/by-name bucketstore bucket)
        {:keys [metastore]} (get-region regions region)
        {:keys [uploadid]}  params]
    (-> (meta/list-upload-parts metastore bucket object (parse-uuid uploadid))
        (xml/list-upload-parts uploadid bucket object)
        (xml-response)
        (request-id request)
        (send! (:chan request)))))

(defn yield-finalizer
  "Closure for updating an upload reference once its data is written out"
  [metastore request bucket object partno upload]
  (fn [inode version size checksum]
    (debug "uploading part with details: " bucket object upload size checksum)
    (meta/update-part! metastore bucket object
                       (parse-uuid upload)
                       (Long/parseLong partno)
                       {:inode    inode
                        :version  version
                        :modified (iso8601-timestamp)
                        :size     size
                        :checksum checksum})
    (send! (-> (response)
               (header "ETag" checksum)
               (request-id request))
           (:chan request))))

(defn put-object-part
  "Insert a new part in a multi-part upload"
  [{:keys [body bucket object] :as request} bucketstore regions]
  (let [{:keys [region]}                    (bucket/by-name bucketstore bucket)
        {:keys [metastore storage-classes]} (get-region regions region)
        {:keys [partnumber uploadid]}       (:params request)
        blobstore                           (get storage-classes :standard)
        version                             (uuid/time-based)
        inode                               (uuid/random)
        finalize!                           (yield-finalizer
                                             metastore request
                                             bucket object
                                             partnumber uploadid)]
    (blob/append-stream! blobstore inode version body finalize!)))

(defn complete-upload
  "To complete an upload, all parts are read and streamed to
   a new inode which will aggregate all content from parts.

   Work is spread across two threads and three communication
   channels are used for synchronisation:

   - A piped input stream.
   - A promise for the content's checksum
   - A lamina channel for chunks

   As soon as a request comes in, a 200 response is emitted,
   whose body is the piped input stream.

   A thread is started which reads from all parts and pushes
   chunks to the lamina channel. While chunks are pushed to
   the channel, whitespace is also emitted on the piped input
   stream to ensure the connection won't be hung up.

   The second thread which is started reads chunks from the lamina
   channel and appends them to a new inode. Once it is properly
   streamed, an object is finalized and made available and its
   checksum is sent to the created promise.

   Once all parts have been streamed with no errors, an XML
   payload is sent to the piped input stream to acknowledge success,
   which relies on the promised being delivered to send back the
   overall checksum in the payload.
"
  [{:keys [bucket object] :as request} bucketstore regions]
  (let [{:keys [region]}                    (bucket/by-name bucketstore bucket)
        {:keys [metastore storage-classes]} (get-region regions region)
        blobstore                           (get storage-classes :standard)
        inode                               (uuid/random)
        version                             (uuid/time-based)
        upload                              (-> request
                                                :params
                                                :uploadid
                                                parse-uuid)
        [is os]                             (piped-input-stream)
        body-stream                         (channel)
        push-str                            (fn [^String s]
                                              (.write os (.getBytes s))
                                              (.flush os))
        etag                                (promise)]

    (future

      (doseq [part (meta/list-upload-parts metastore bucket object upload)]

        (debug "streaming part: " (:partno part))
        (push-str "\n")
        (blob/stream!
         blobstore (:inode part) (:version part)
         (fn [chunks]
           (when chunks
             (doseq [{:keys [payload]} chunks]
               (push-str " ")
               (enqueue body-stream (->channel-buffer payload)))))))

      (close body-stream)
      (debug "all streams now flushed, waiting for etag")
      (push-str (xml/complete-multipart-upload bucket object @etag))
      (.close os))

    (future
      (blob/append-stream! blobstore inode version body-stream
                           (fn [_ _ size checksum]
                             (meta/update! metastore bucket object
                                    {:inode inode
                                     :version version
                                     :size size
                                     :checksum checksum
                                     :atime (iso8601-timestamp)
                                     :metadata {"content-type"
                                                "binary/octet-stream"}
                                     :storageclass "standard"
                                     :acl "private"})
                             (deliver etag checksum)
                             ;; successful completion, now cleanup!
                             (doseq [part (meta/list-upload-parts
                                           metastore bucket object upload)]
                               (blob/delete! blobstore
                                             (:inode part) (:version part)))
                             (meta/abort-multipart-upload!
                              metastore bucket object upload))))

    (-> (response is)
        (content-type "application/xml")
        (request-id request)
        (send! (:chan request)))))

(defn head-object
  "Retrieve object information"
  [{:keys [bucket object] :as request} bucketstore regions]
  (let [{:keys [region]} (bucket/by-name bucketstore bucket)
        {:keys [metastore]}        (get-region regions region)
        {:keys [atime size checksum] :as payload} (meta/fetch metastore bucket object)]
    (-> (response)
        (request-id request)
        (content-type "application/binary")
        (header "ETag" (str checksum))
        (header "Last-Modified" (str atime))
        (header "Content-Length" (str size))
        (send! (:chan request)))))

(defn get-object-acl
  "Retrieve and format object acl"
  [{:keys [bucket object] :as request} bucketstore regions]
  (let [{:keys [region]} (bucket/by-name bucketstore bucket)
        {:keys [metastore]}        (get-region regions region)]
    (-> (meta/fetch metastore bucket object)
        :acl
        (xml/default)
        (xml-response)
        (request-id request)
        (send! (:chan request)))))

(defn put-object-acl
  "Update object acl"
  [{:keys [bucket object body] :as request} bucketstore regions]
  (let [{:keys [region]} (bucket/by-name bucketstore bucket)
        {:keys [metastore]} (get-region regions region)
        acl              (slurp body)]
    (meta/update! metastore bucket object {:acl acl})
    (-> (response)
        (request-id request)
        (send! (:chan request)))))

(defn get-bucket-policy
  "Retrieve object policy: always fails for now"
  [request bucketstore regions]
  (throw (ex-info "no such bucket policy" {:type :no-such-bucket-policy
                                           :bucket (:bucket request)
                                           :status-code 404})))

(defn delete-object
  "Delete current revision of objects."
  [{:keys [bucket object] :as request} bucketstore regions]
  (let [{:keys [region versioned]}          (bucket/by-name bucketstore bucket)
        {:keys [metastore storage-classes]} (get-region regions region)
        {:keys [inode version]}             (meta/fetch metastore bucket object)
        blobstore                           (get storage-classes :standard)]

    ;; delete object
    (meta/delete! metastore bucket object)
    (blob/delete! blobstore inode version)
    (-> (response)
        (request-id request)
        (send! (:chan request)))))

(defn get-bucket-versioning
  "Retrieve bucket versioning configuration"
  [{:keys [bucket] :as request} bucketstore regions]
  (let [{:keys [versioned]} (bucket/by-name bucketstore bucket)]
    (-> (xml/get-bucket-versioning versioned)
        (xml-response)
        (request-id request)
        (send! (:chan request)))))

(defn put-bucket-versioning
  "No versioning support for now even though versions are stored"
  [{:keys [bucket] :as request} bucketstore regions]
  (-> (response)
      (request-id request)
      (send! (:chan request))))

(defn unknown
  "unknown operation"
  [request bucketstore regions]
  (-> (xml/unknown request)
      (xml-response)
      (status 400)
      (send! (:chan request))))

(def opmap
  "Map requests to handler with associated necessary
   permissions"
  {:get-service            {:handler get-service
                            :perms   [:authenticated]}
   :put-bucket             {:handler put-bucket
                            :perms   [[:memberof "authenticated-users"]]}
   :put-bucket-versioning  {:handler put-bucket-versioning
                            :perms   [[:bucket :WRITE]]}
   :get-bucket-versioning  {:handler get-bucket-versioning
                            :perms   [[:bucket :WRITE]]}
   :delete-bucket          {:handler delete-bucket
                            :perms   [[:memberof "authenticated-users"]
                                      [:bucket   :owner]]}
   :head-bucket            {:handler head-bucket
                            :perms [[:bucket :READ]]}
   :get-bucket             {:handler get-bucket
                            :perms   [[:bucket :READ]]}
   :get-bucket-acl         {:handler get-bucket-acl
                            :perms   [[:bucket :READ_ACP]]}
   :get-bucket-location    {:handler get-bucket-location
                            :perms  [[:bucket :READ]]}
   :put-bucket-acl         {:handler put-bucket-acl
                            :perms   [[:bucket :WRITE_ACP]]}
   :get-object             {:handler get-object
                            :perms   [[:object :READ]]}
   :head-object            {:handler head-object
                            :perms   [[:object :READ]]}
   :put-object             {:handler put-object
                            :perms   [[:bucket :WRITE]]}
   :delete-object          {:handler delete-object
                            :perms   [[:bucket :WRITE]]}
   :get-object-acl         {:handler get-object-acl
                            :perms   [[:object :READ_ACP]]}
   :put-object-acl         {:handler put-object-acl
                            :perms   [[:object :WRITE_ACP]]}
   :get-bucket-policy      {:handler get-bucket-policy
                            :perms   [[:bucket :READ_ACP]]}
   :post-object-uploads    {:handler initiate-upload
                            :perms   [[:bucket :WRITE]]}
   :put-object-uploadid    {:handler put-object-part
                            :perms   [[:bucket :WRITE]]}
   :delete-object-uploadid {:handler abort-upload
                            :perms   [[:bucket :WRITE]]}
   :post-object-uploadid   {:handler complete-upload
                            :perms   [[:bucket :WRITE]]}
   :get-object-uploadid    {:handler get-upload-parts
                            :perms   [[:bucket :WRITE]]}
   :get-bucket-uploads     {:handler get-bucket-uploads
                            :perms   [[:bucket :READ]]}})

(defn ex-handler
  "Wrap exceptions and report them correctly"
  [request exception]
  (-> (xml-response (xml/exception request exception))
      (exception-status (ex-data exception))
      (request-id request)
      (send! (:chan request)))
  nil)

(defn dispatch
  "Dispatch operation"
  [{:keys [operation] :as request} bucketstore regions]
  (when request
    (debug "handling operation: " operation)
    (let [{:keys [handler perms] :or {handler unknown}} (get opmap operation)]
      (try (authorize request perms bucketstore regions)
           (handler request bucketstore regions)
           (catch Exception e
             (when-not (:type (ex-data e))
               (error e "caught exception during operation"))
             (ex-handler request e))))))
