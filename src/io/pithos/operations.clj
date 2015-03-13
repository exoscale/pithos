(ns io.pithos.operations
  "The operations namespace maps an operation as figured out when
   preparing a request and tries to service it.

   This namespace is where most of the actual work in exposing S3
   functionality happens."
  (:require [io.pithos.response     :refer [header response status
                                            xml-response request-id
                                            content-type exception-status]]
            [io.pithos.util         :refer [piped-input-stream
                                            parse-uuid
                                            iso8601->rfc822
                                            ->channel-buffer]]
            [clojure.core.async     :refer [go chan >! <! put! close!]]
            [clojure.tools.logging  :refer [debug info warn error]]
            [clojure.string         :refer [split lower-case]]
            [io.pithos.util         :as util]
            [io.pithos.store        :as store]
            [io.pithos.bucket       :as bucket]
            [io.pithos.meta         :as meta]
            [io.pithos.blob         :as blob]
            [io.pithos.xml          :as xml]
            [io.pithos.system       :as system]
            [io.pithos.desc         :as desc]
            [io.pithos.stream       :as stream]
            [io.pithos.perms        :as perms]
            [io.pithos.acl          :as acl]
            [io.pithos.cors         :as cors]
            [io.pithos.reporter     :as reporter]
            [qbits.alia.uuid        :as uuid]))

(defn assoc-targets
  "Each operation has a primary target, fetch details early on has assoc
   them in the operations map"
  [{:keys [bucket object] {:keys [uploadid]} :params :as req} system target]
  (case target
    :bucket (assoc req :bd (bucket/bucket-descriptor system bucket))
    :object (assoc req :od (desc/object-descriptor system bucket object))
    :upload (assoc req
              :od (desc/object-descriptor system bucket object)
              :upload-id (parse-uuid uploadid))
    req))

(defn get-metadata
  "Retrieve metadata from a request's headers"
  [headers]
  (let [valid? (fn [x]
                 (or (#{"content-type" "content-encoding"
                        "content-disposition" "content-language"
                        "expires" "cache-control"}
                      x)
                     (re-find #"^x-amz-meta-" x)))]
    (reduce merge
            {"content-type" "application/binary"}
            (filter (comp valid? key) headers))))

(defn get-source
  "The AWS API provides a way to specify an object's source in
   headers. This function isolates the handling of extracting
   the source when provided and validating that it is suitable."
  [{:keys [headers authorization]} system]
  (let [update-metadata? (some->> (get headers "x-amz-metadata-directive" "")
                                  (lower-case)
                                  ((partial = "replace")))]
    (if-let [source (get headers "x-amz-copy-source")]
      ;; we're dealing with a copy object request
      (if-let [[prefix s-bucket s-object] (split (if (.startsWith source "/")
                                                   source
                                                   (str "/" source))
                                                 #"/"
                                                 3)]
        (if (seq prefix)
          (throw (ex-info "invalid" {:type :invalid-request :status-code 400}))
          (when (perms/authorize {:bucket s-bucket
                                  :object s-object
                                  :authorization authorization}
                                 [[:bucket :READ]]
                                 system)
            (let [src (desc/object-descriptor system s-bucket s-object)]
              (when-not (desc/init-version src)
                (throw (ex-info "no such key" {:type :no-such-key
                                               :key  s-object})))
              [src (if update-metadata?
                     (get-metadata headers)
                     (:metadata src))])))
        (throw (ex-info "invalid" {:type :invalid-request :status-code 400})))
      [nil (get-metadata headers)])))

(defn parse-int
  "When integers are supplied as arguments, parse them or
   error out"
  ([nickname default val]
     (if val
       (try
         (Long/parseLong val)
         (catch NumberFormatException e
           (throw (ex-info (str  "invalid value for " (name nickname))
                           {:type :invalid-argument
                            :arg (name nickname)
                            :val val
                            :status-code 400}))))
       default))
    ([nickname val]
     (parse-int nickname nil val)))

(defn get-range
  "Fetch range information from headers. We ignore the total size
   when supplied using the Content-Range header. The end of the
   range may be ommitted, not the start."
  [od headers]
  (if-let [range-def (or (get headers "range")
                         (get headers "content-range"))]
    (->> range-def
         (re-find #"^bytes[ =](\d+)-(\d+)?")
         (#(or % (throw (ex-info "invalid range"
                                 {:type :invalid-argument
                                  :arg "range"
                                  :val range-def
                                  :status-code 400}))))
         (drop 1)
         (mapv #(or % (str (desc/size od))))
         (mapv (partial parse-int "range")))
    [0 (desc/size od)]))

(defn get-service
  "Lists all buckets for  tenant"
  [{{:keys [tenant]} :authorization :as request} system]
  (-> (bucket/by-tenant (system/bucketstore system) tenant)
      (xml/list-all-my-buckets)
      (xml-response)))

(defn put-bucket
  "Creates a bucket"
  [{{:keys [tenant]} :authorization :keys [bucket] :as request} system]
  (let [target-acl (perms/header-acl tenant (:headers request))]
    (store/create! (system/bucketstore system) tenant bucket
                   {:acl target-acl})
    (-> (response)
        (header "Location" (str "/" bucket))
        (header "Connection" "close"))))

(defn delete-bucket
  "Deletes a bucket, only possible if the bucket isn't empty. The bucket
   should also be checked for in-progress uploads."
  [{:keys [bd bucket] :as request} system]
  (let [{:keys [keys]} (meta/prefixes (bucket/metastore bd)
                                      bucket {:max-keys 1})]
    (when (pos? (count keys))
      (throw (ex-info "bucket not empty" {:type :bucket-not-empty
                                          :bucket bucket
                                          :status-code 409})))
    (store/delete! (system/bucketstore system) bucket)
    (-> (response)
        (status 204))))

(defn get-bucket
  "List bucket content. It's worth noting that S3 has no actual concept of
   directories, instead, a delimiter can be supplied, in which case results will
   be split between contents and prefixes"
  [{:keys [bd params bucket] :as request} system]
  (let [params   (update-in params [:max-keys]
                            (partial parse-int "max-keys" 1000))
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

(defn get-bucket-cors
  "Retrieve bucket CORS configuration"
  [{:keys [bucket]} system]
  (let [cors (or (some-> (bucket/by-name (system/bucketstore system) bucket)
                         :cors
                         read-string)
                 [])]
    (-> cors
        (cors/as-xml)
        (xml-response))))

(defn delete-bucket-cors
  "Destroy any previous CORS configuration"
  [{:keys [bucket]} system]
  (store/update! (system/bucketstore system) bucket {:cors nil})
  (status (response) 204))

(defn put-bucket-cors
  "Update bucket CORS configuration"
  [{:keys [bucket body]} system]
  (let [cors (-> (slurp body) (cors/xml->cors) (pr-str))]
    (debug "got cors: " cors)
    (store/update! (system/bucketstore system) bucket {:cors cors})
    (response)))

(defn put-bucket-acl
  "Update bucket acl, ACLs may be supplied in three different ways:

   - canned ACLs through the `x-amz-acl` header
   - explicit header ACLs with the `x-amz-acl-grant-*` headers
   - an XML body conforming to the AWS S3 format

  If several ACL method submission methods are used in the same request,
  canned ACLs take precedence over explicit header ACLs which take
  precedence over an XML body
  "
  [{:keys [bucket body headers authorization] :as request} system]
  (let [header-acl (if (perms/has-header-acl? header)
                     (perms/header-acl (:tenant authorization) headers))
        acl        (or header-acl
                       (-> (slurp body) (acl/xml->acl) (pr-str)))]
    (store/update! (system/bucketstore system) bucket {:acl acl})
    (response)))

(defn get-bucket-acl
  "Retrieve and display bucket ACL as xml"
  [{:keys [bucket bd] {:keys [tenant]} :authorization :as request} system]
  (let [acl (or (some-> (bucket/by-name (system/bucketstore system) bucket)
                        :acl
                        read-string)
                {:FULL_CONTROL [{:ID tenant}]})]
    (->  acl
         (acl/as-xml)
         (xml-response))))

(defn get-bucket-uploads
  "List current uploads"
  [{:keys [bucket bd] :as request} system]
  (-> (meta/list-uploads (bucket/metastore bd) bucket)
      (xml/list-multipart-uploads bucket)
      (xml-response)))

(defn options-object
  "Answer an empty object for any OPTIONS request"
  [request system]
  (-> (response)
      (status 204)))

(defn initiate-upload
  "Start a new upload"
  [{:keys [od bucket object params authorization] :as request} system]
  (let [metadata            (get-metadata (:headers request))
        upload-id           (uuid/random)
        target-acl          (perms/header-acl (:bd request)
                                              (:tenant authorization)
                                              (:headers request))]
    (meta/initiate-upload! (bucket/metastore od)
                           bucket
                           object
                           upload-id
                           (merge metadata
                                  {"initiated"    (util/iso8601-timestamp)
                                   "acl"          target-acl}))
    (-> (xml/initiate-multipart-upload bucket object upload-id)
        (xml-response))))


(defn get-object-acl
  "Retrieve and format object acl"
  [{:keys [od bucket object] {:keys [tenant]} :authorization} system]
  (let [acl (or (some-> (store/fetch (bucket/metastore od) bucket object)
                        :acl
                        read-string)
                {:FULL_CONTROL [{:ID tenant}]})]
    (->  acl
         (acl/as-xml)
         (xml-response))))

(defn put-object-acl
  "Update object acl, ACLs may be supplied in three different ways:

   - canned ACLs through the `x-amz-acl` header
   - explicit header ACLs with the `x-amz-acl-grant-*` headers
   - an XML body conforming to the AWS S3 format

  If several ACL method submission methods are used in the same request,
  canned ACLs take precedence over explicit header ACLs which take
  precedence over an XML body
  "
  [{:keys [od bd headers bucket object body authorization] :as request} system]
  (let [header-acl (if (perms/has-header-acl? headers)
                     (perms/header-acl bd headers (:tenant authorization)))
        acl        (or header-acl
                       (-> (slurp body) (acl/xml->acl) (pr-str)))]
    (store/update! (bucket/metastore od) bucket object {:acl acl})
    (response)))

(defn put-or-delete-bucket-policy
  "Dummy handler for bucket policy updates."
  [request system]
  (status (response) 204))

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

(defn delete-bucket-lifecycle
  [request system]
  (-> (response)
      (status 204)))

(defn put-bucket-lifecycle
  [request system]
  (-> (response)
      (status 204)))

(defn get-bucket-lifecycle
  [request system]
  (-> (xml/bucket-lifecycle)
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
      (content-type (desc/content-type od))
      (header "ETag" (str "\"" (desc/checksum od) "\""))
      (header "Last-Modified" (iso8601->rfc822 (str (:atime od))))
      (header "Content-Length" (str (desc/size od)))
      (update-in [:headers] (partial merge (:metadata od)))))

(defn post-bucket-delete
  "Delete multiple objects based on input"
  [{:keys [bucket object body] :as request} system]
  (let [paths (-> (slurp body) (xml/xml->delete))]
    (doseq [path paths
            :let [od (desc/object-descriptor system bucket path)]]
      (reporter/report-all! (system/reporters system)
                            {:type   :delete
                             :bucket bucket
                             :object path
                             :size   (desc/size od)})
      (store/delete! (bucket/metastore od) bucket path)
      (store/delete! (desc/blobstore od) od (desc/version od)))
    (-> (xml/delete-objects paths)
        (xml-response))))

(defn delete-object
  "Delete current revision of objects."
  [{:keys [od bucket object] :as request} system]
  (reporter/report-all! (system/reporters system)
                        {:type  :delete
                         :bucket bucket
                         :object object
                         :size   (desc/size od)})
  (store/delete! (bucket/metastore od) bucket object)
  (store/delete! (desc/blobstore od) od (desc/version od))
  (-> (response)
      (status 204)))

(defn get-object
  "Retrieve object. A response is sent immediately, whose body
  is a piped input stream. The connected outputstream will be fed data
  on a different thread."
  [{:keys [od bucket object headers] :as request} system]
  (when-not (desc/init-version od)
    (throw (ex-info "no such key" {:type :no-such-key
                                   :status-code 404
                                   :bucket bucket
                                   :key object})))

  (let [[is os]     (piped-input-stream)
        add-headers #(doseq [[k v] (:metadata od)] (header % k v))
        range       (get-range od headers)]

    (debug "will stream " (desc/inode od) (desc/version od))
    (future ;; XXX: set up a dedicated threadpool
      (try
        (stream/stream-to od os range)
        (catch Exception e
          (error e "could not completely write out: "))))
    (-> (response is)
        (content-type (desc/content-type od))
        (header "Content-Length" (- (last range) (first range)))
        (header "Last-Modified" (iso8601->rfc822 (str (:atime od))))
        (header "ETag" (str "\"" (desc/checksum od) "\""))
        (update-in [:headers] (partial merge (:metadata od))))))

(defn put-object
  "Accept data for storage. The body of this function is a bit messy and
   needs to be reworked.

   There are two radically different upload scenario, a standard upload
   where the body of the request needs to be stored and requests containing
   an `x-amz-copy-source header` which contains a reference to another object
   to copy.

   In this case a new inode is created and data copied over, if permissions are
   sufficient for a copy.

   Otherwise, data is streamed in."
  [{:keys [od body bucket object authorization] :as request} system]
  (let [dst         od
        previous    (desc/init-version dst)
        ctype       (get (:headers request) "content-type")
        target-acl  (perms/header-acl (:bd request)
                                      (:tenant authorization)
                                      (:headers request))
        [src meta]  (get-source request system)]

    (if src
      ;; avoid copies when source and dest are the same
      (when (or (not= (desc/inode src) (desc/inode dst))
                (not= (desc/version src) previous))
        (stream/stream-copy src dst))
      (do
        (when previous
          (desc/increment! dst))
        (stream/stream-from body od)))

    ;; if a previous copy existed, kill it
    (when (and previous (not= previous (desc/version dst)))
      (reporter/report-all! (system/reporters system)
                            {:type   :delete
                             :bucket bucket
                             :object object
                             :size   (desc/init-size dst)})
      (store/delete! (desc/blobstore dst) dst previous))

    (doseq [[k v] meta]
      (desc/col! dst k v))
    (desc/col! dst :acl target-acl)
    (desc/save! dst)
    (reporter/report-all! (system/reporters system)
                          {:type   :put
                           :bucket bucket
                           :object object
                           :size   (desc/size dst)})

    (-> (if src
          (-> (xml/copy-object (desc/checksum dst)
                               (str (:atime dst)))
              (xml-response))
          (response))
        (header "ETag" (str "\"" (desc/checksum dst) "\"")))))

(defn abort-upload
  "Abort an ongoing upload"
  [{:keys [od upload-id bucket object] :as request} system]
  (doseq [{:keys [inode version]}
          (meta/list-upload-parts (bucket/metastore od)
                                  bucket
                                  object
                                  upload-id)]
    (store/delete! (desc/blobstore od) inode version))
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
  [{:keys [od upload-id body bucket object authorization] :as request} system]
  (let [{:keys [partnumber]} (:params request)
        pd                   (desc/part-descriptor system bucket object
                                                   upload-id partnumber)
        [src _]              (get-source request system)]

    (if src
      (stream/stream-copy src pd)
      (stream/stream-from body pd))

    (desc/save! pd)
    (-> (response)
        (header "ETag" (str "\"" (desc/checksum pd) "\"")))))

(defn complete-upload
  "To complete an upload, all parts are read and streamed to
   a new inode which will aggregate all content from parts.

   The body of the response is a channel which will be fed keepalive bytes
   while the parts are streamed. Once all parts have been streamed a final
   XML payload is pushed out, described the results of the operation.
"
  [{:keys [bucket object upload-id od body] :as request} system]
  (let [ch        (chan)
        previous  (desc/init-version od)
        push-str  (fn [type]
                    (put! ch (case type :block "\n" :chunk " " type)))
        etag      (promise)
        details   (meta/get-upload-details (bucket/metastore od)
                                           bucket object upload-id)
        metadata  (-> (:metadata details) (dissoc "acl" "initiated"))
        inparts   (xml/xml->multipart (slurp body))]
    (push-str "<?xml version= \"1.0\" encoding= \"UTF-8\"?>")
    (future
      (try
        (when previous
          (desc/increment! od))
        (let [allparts (desc/part-descriptors system bucket object upload-id)
              parts    (for [{:keys [part etag]} inparts
                             :let [finder #(and (= (desc/part %) part) %)
                                   part   (some finder allparts)]]
                         (if (and part (= (desc/checksum part) etag))
                           part
                           (throw (ex-info "invalid part definition"
                                           {:type :invalid-part
                                            :status-code 400}))))
              od       (stream/stream-copy-parts (vec parts) od push-str)]

          (desc/col! od :acl (get-in details [:metadata "acl"]))
          (desc/col! od :content-type
                     (get-in details [:metadata "content-type"]))
          (doseq [[k v] metadata]
            (desc/col! od k v))
          (desc/save! od)
          (reporter/report-all! (system/reporters system)
                                {:type   :put
                                 :bucket bucket
                                 :object object
                                 :size   (desc/size od)})
          (doseq [part allparts]
            (push-str :block)
            (store/delete! (desc/blobstore part) part (desc/version part)))

          (meta/abort-multipart-upload! (bucket/metastore od)
                                        bucket
                                        object
                                        upload-id)


          (when (and previous (not= previous (desc/version od)))
            (push-str :block)
            (reporter/report-all! (system/reporters system)
                                  {:type   :delete
                                   :bucket bucket
                                   :object object
                                   :size   (desc/init-size od)})
            (store/delete! (desc/blobstore od) od previous))

          (debug "all streams now flushed")
          (let [body (xml/complete-multipart-upload bucket object
                                                    (desc/checksum od))
                ;; some parsers have difficulty handling leading
                ;; spaces before the xml def, so trim it.
                ;; 38 corresponds to the length of
                ;; "<?xml version= \"1.0\" encoding= \"UTF-8\"?>"
                trimmed (.substring body 38)]
            (push-str trimmed)))
        (catch Exception e
          (error e "error in multipart completion"))
        (finally
          (close! ch))))
    (-> (response ch)
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
  {:get-service             {:handler get-service
                             :target  :service
                             :perms   [[:memberof "authenticated-users"]]}
   :put-bucket              {:handler put-bucket
                             :perms   [[:memberof "authenticated-users"]]}
   :put-bucket-versioning   {:handler put-bucket-versioning
                             :target  :bucket
                             :perms   [[:bucket :WRITE]]}
   :get-bucket-versioning   {:handler get-bucket-versioning
                             :target  :bucket
                             :perms   [[:bucket :WRITE]]}
   :delete-bucket           {:handler delete-bucket
                             :target  :bucket
                             :perms   [[:memberof "authenticated-users"]
                                       [:bucket   :owner]]}
   :delete-bucket-lifecycle {:handler delete-bucket-lifecycle
                             :target  :bucket
                             :perms  [[:memberof "authenticated-users"]
                                      [:bucket   :owner]]}
   :put-bucket-lifecycle    {:handler put-bucket-lifecycle
                             :target  :bucket
                             :perms  [[:memberof "authenticated-users"];
                                      [:bucket   :owner]]}
   :get-bucket-lifecycle    {:handler get-bucket-lifecycle
                             :target  :bucket
                             :perms  [[:memberof "authenticated-users"]
                                      [:bucket   :owner]]}
   :head-bucket             {:handler head-bucket
                             :target  :bucket
                             :perms [[:bucket :READ]]}
   :get-bucket              {:handler get-bucket
                             :target  :bucket
                             :perms   [[:bucket :READ]]}
   :get-bucket-cors         {:handler get-bucket-cors
                             :target  :bucket
                             :perms   [[:bucket :READ]]}
   :delete-bucket-cors      {:handler delete-bucket-cors
                             :target  :bucket
                             :perms   [[:bucket :WRITE]]}
   :put-bucket-cors         {:handler put-bucket-cors
                             :target  :bucket
                             :perms   [[:bucket :WRITE]]}
   :get-bucket-acl          {:handler get-bucket-acl
                             :target  :bucket
                             :perms   [[:bucket :READ_ACP]]}
   :put-bucket-acl          {:handler put-bucket-acl
                             :target  :bucket
                             :perms   [[:bucket :WRITE_ACP]]}
   :get-bucket-location     {:handler get-bucket-location
                             :target  :bucket
                             :perms   [[:bucket :READ]]}
   :put-bucket-policy       {:handler put-or-delete-bucket-policy
                             :target  :bucket
                             :perms   [[:bucket :WRITE]]}
   :delete-bucket-policy    {:handler put-or-delete-bucket-policy
                             :target  :bucket
                             :perms   [[:bucket :WRITE]]}
   :get-bucket-policy       {:handler get-bucket-policy
                             :target  :bucket
                             :perms   [[:bucket :READ_ACP]]}
   :get-bucket-uploads      {:handler get-bucket-uploads
                             :perms   [[:bucket :READ]]
                             :target  :bucket}
   :options-object          {:handler options-object
                             :target  :object
                             :cors?   true
                             :perms   [[:object :READ]]}
   :options-bucket          {:handler options-object
                             :target  :object
                             :cors?   true
                             :perms   [[:bucket :READ]]}
   :post-bucket-delete      {:handler post-bucket-delete
                             :target  :bucket
                             :perms   [[:bucket :WRITE]]}
   :get-object              {:handler get-object
                             :target  :object
                             :cors?   true
                             :perms   [[:object :READ]]}
   :head-object             {:handler head-object
                             :target  :object
                             :cors?   true
                             :perms   [[:object :READ]]}
   :put-object              {:handler put-object
                             :target  :object
                             :cors?   true
                             :perms   [[:bucket :WRITE]]}
   :delete-object           {:handler delete-object
                             :target  :object
                             :cors?   true
                             :perms   [[:bucket :WRITE]]}
   :get-object-acl          {:handler get-object-acl
                             :target  :object
                             :perms   [[:object :READ_ACP]]}
   :put-object-acl          {:handler put-object-acl
                             :target  :object
                             :perms   [[:object :WRITE_ACP]]}
   :post-object-uploads     {:handler initiate-upload
                             :target  :object
                             :perms   [[:bucket :WRITE]]}
   :put-object-uploadid     {:handler put-object-part
                             :target  :upload
                             :perms   [[:bucket :WRITE]]}
   :delete-object-uploadid  {:handler abort-upload
                             :target  :upload
                             :perms   [[:bucket :WRITE]]}
   :post-object-uploadid    {:handler complete-upload
                             :target  :upload
                             :perms   [[:bucket :WRITE]]}
   :get-object-uploadid     {:handler get-upload-parts
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
   (let [{:keys [handler perms target cors?]}    (get opmap operation)
         {:keys [bucket headers request-method]} request
         origin                                  (get headers "origin")
         handler                                 (or handler unknown)
         resp
         (try (perms/authorize request perms system)
              (-> request
                  (assoc-targets system target)
                  (handler system)
                  (request-id request))
              (catch Exception e
                (when-not (:type (ex-data e))
                  (error e "caught exception during operation"))
                (ex-handler request e)))]

     ;; If an "Origin" header is present and we are asked to handle
     ;; CORS rules, process them.
     (if-let [rules (and cors?
                         origin
                         (some-> (bucket/by-name (system/bucketstore system)
                                                 bucket)
                                 :cors
                                 read-string))]
       (update-in resp [:headers] merge
                  (cors/matches? rules headers request-method))
       resp))))
