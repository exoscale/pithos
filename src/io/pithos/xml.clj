(ns io.pithos.xml
  "This namespace provides plumbing to output XML as well as templates.
  Our intent here is to avoid having to deal with presentation elsewhere,
  instead, data structures should be emitted by other subsystems, this
  namespace will then take care of making them suitable for sending out
  on the wire."
  (:require [clojure.string        :as s]
            [io.pithos.sig         :as sig]
            [clojure.data.xml      :refer [->Element emit-str parse-str]]
            [clojure.zip           :refer [xml-zip]]
            [clojure.data.zip.xml  :refer [xml-> xml1-> text]]
            [clojure.pprint        :refer [pprint]]
            [clojure.tools.logging :refer [error]]
            [clj-time.core         :refer [now]]
            [io.pithos.util        :refer [iso8601]]))

(defn format-exception
  [^Exception e]
  (cond
    (ex-data e)
    (str e)

    (instance? com.datastax.driver.core.exceptions.DriverException e)
    "an unexpected storage error occured"

    (instance? java.lang.NullPointerException e)
    "an unexpected server error occured"

    :else
    "an unexpected error occured"))

(defn xml->delete
  [src]
  (try
    (let [xml-tree (xml-zip (parse-str src))
          paths    (xml-> xml-tree
                          :Object
                          :Key
                          text)]
      (vec (flatten paths)))
    (catch clojure.lang.ExceptionInfo e
      (throw e))
    (catch Exception e
      (throw (ex-info "invalid XML in body"
                      {:type :invalid-delete-xml
                       :status-code 400})))))

(defn xml->multipart
  [src]
  (try
    (let [xml-tree   (xml-zip (parse-str src))
          unquote    #(s/replace % "\"" "")
          node->part #(hash-map :part (Long/parseLong
                                       (xml1-> % :PartNumber text))
                                :etag (unquote (xml1-> % :ETag text)))]

      (xml-> xml-tree :Part node->part))
    (catch clojure.lang.ExceptionInfo e
      (throw e))
    (catch Exception e
      (throw (ex-info "invalid XML in body"
                      {:type :invalid-multipart-xml
                       :original e
                       :status-code 400})))))

(defn seq->xml
  "A small [hiccup](https://github.com/weavejester/hiccup) like sequence to
XML emitter.

The following sequence:

    [:ListAllMyBucketsResult {:xmlns \"http://foo\"}
      [:Owner [:ID \"foo\"]
              [:DisplayName \"bar\"]]
      [:Foo]
      [:AccessControlList
        [:Grant
          [:Grantee {:xmlns:xsi \"http//bar\"
                     :xsi:type  \"CanonicalUser\"}
            [:Owner
              [:ID \"owner1\"]
              [:DisplayName \"display1\"]]]
          [:Permission \"Full-Control\"]]]]

Will produce an XML AST equivalent to:

     <?xml version=\"1.0\" encoding=\"UTF-8\"?>
     <ListAllMyBucketsResult xmlns=\"http://foo\">
      <Owner>
         <ID>foo</ID>
         <DisplayName>bar</DisplayName>
       </Owner>
       <Foo/>
       <AccessControlList>
        <Grant>
          <Grantee xmlns:xsi=\"http//bar\" xsi:type=\"CanonicalUser\">
            <Owner>
              <ID>owner1</ID>
              <DisplayName>display1</DisplayName>
            </Owner>
          </Grantee>
          <Permission>Full-Control</Permission>
        </Grant>
      </AccessControlList>
    </ListAllMyBucketsResult>

"
  [input]
  (letfn [(seq-nodes->xml [node]
            (map tag-nodes->xml node))
          (tag-nodes->xml [[tag & nodes]]
            (let [attrs (if (map? (first nodes)) (first nodes) {})
                  nodes (if (map? (first nodes)) (rest nodes) nodes)]
              (if (every? sequential? nodes)
                (let [seq-nodes (->> nodes
                                     (filter (comp vector? first))
                                     (mapcat seq-nodes->xml))
                      tag-nodes (->> nodes
                                     (filter (complement (comp vector? first)))
                                     (remove empty?)
                                     (map tag-nodes->xml))]
                  (->Element (name tag) attrs
                             (vec (concat seq-nodes tag-nodes))))
                (->Element (name tag) attrs (first nodes)))))]
    (tag-nodes->xml input)))

(defn seq->xmlstr
  "Given a valid input for `seq->xml` output an xml string"
  [s]
  (emit-str (seq->xml (vec (remove nil? s)))))

;; Some common elements for our templates

(def ^{:doc "Shortcut for main XML namespace"} xml-ns
  {:xmlns "http://s3.amazonaws.com/doc/2006-03-01/"})


;; XML Templates, based on the above functions

(defn unknown
  "Template used when the operation could not be inferred"
  [{:keys [operation]}]
  (seq->xmlstr
   [:UnknownAction xml-ns
    [:Action [:Code ((fnil name "no operation provided") operation)]]]))

(defn default
  "Debug template to at least show some output"
  [something]
  (seq->xmlstr
   [:Output xml-ns [:Payload (with-out-str (pprint something))]]))

(defn list-all-my-buckets
  "Template for list-all-my-bucket-results"
  [[bucket :as buckets]]
  (seq->xmlstr
   [:ListAllMyBucketsResult xml-ns
    [:Owner [:ID (:tenant bucket)] [:DisplayName (:tenant bucket)]]
    (apply vector :Buckets
           (for [{:keys [bucket created]} buckets]
             [:Bucket
              [:Name bucket]
              [:CreationDate created]]))]))

(defn list-bucket
  "Template for the list-bucket operation response"
  [{:keys [marker truncated? next-marker keys prefixes]}
   {:keys [tenant bucket]}
   {:keys [prefix delimiter max-keys]}]
  (seq->xmlstr
   (apply vector
          :ListBucketResult xml-ns
          [:Name bucket]
          [:Prefix prefix]
          [:MaxKeys (str max-keys)]
          [:Delimiter delimiter]
          [:IsTruncated (str truncated?)]
          (when marker
            [:Marker marker])
          (when truncated?
            [:NextMarker (:object (last keys))])
          (when (seq prefixes)
            (vec
             (for [prefix prefixes] [:CommonPrefixes [:Prefix prefix]])))
          (for [{:keys [atime object size checksum] :or {size 0}} keys]
            [:Contents
             [:Key object]
             [:LastModified atime]
             [:ETag (str "\"" checksum "\"") ]
             [:Size (str size)]
             [:Owner
              [:ID tenant]
              [:DisplayName tenant]]
             [:StorageClass "Standard"]]))))

(defn initiate-multipart-upload
  "Tempalte for the initiate-multipart-upload response"
  [bucket object inode]
  (seq->xmlstr
   [:InitiateMultipartUploadResult xml-ns
    [:Bucket bucket]
    [:Key object]
    [:UploadId (str inode)]]))

(defn list-multipart-uploads
  "Template for the list-multipart-uploads response"
  [uploads bucket prefix]
  (seq->xmlstr
   [:ListMultipartUploadsResult xml-ns
    [:Bucket bucket]
    [:KeyMarker]
    [:UploadIdMarker]
    [:MaxUploads 1000]
    [:IsTruncated "false"]
    [:Prefix prefix]
    (for [{:keys [object upload metadata]} uploads]
      [:Upload
       [:Key object]
       [:Initiated (get metadata "initiated")]
       [:UploadId (str upload)]])]))

(defn list-upload-parts
  "Template for the list multipart upload parts response"
  [parts upload bucket object]
  (seq->xmlstr
   (apply vector :ListPartsResult xml-ns
          [:Bucket bucket]
          (for [{:keys [partno modified checksum size]} parts]
            [:Part
             [:PartNumber (str partno)]
             [:LastModified modified]
             [:ETag (str "\"" checksum "\"")]
             [:Size (str size)]]))))

(defn multipart-upload-part-copy
  [etag atime]
  (seq->xmlstr
   [:CopyPartResult
    [:ETag (str "\"" etag "\"")]
    [:LastModified atime]]))

(defn complete-multipart-upload
  "Template for the complete multipart upload response"
  [bucket object etag]
  (seq->xmlstr
   [:CompleteMultipartUploadResult xml-ns
    [:Bucket bucket]
    [:Key object]
    [:Location (format "http://%s.s3.amazonaws.com/%s" bucket object)]
    [:ETag (str "\"" etag "\"")]]))

(defn bucket-location
  [location]
  (seq->xmlstr
   [:LocationConstraint xml-ns
    location]))

(defn bucket-lifecycle
  []
  (seq->xmlstr
   [:LifecycleConfiguration xml-ns]))

(defn copy-object
  "Template for the copy object response"
  [etag atime]
  (seq->xmlstr
   [:CopyObjectResult xml-ns
    [:LastModified atime]
    [:ETag (str "\"" etag "\"")]]))

(defn delete-objects
  [objects]
  (seq->xmlstr
    [:DeleteResult xml-ns
     (for [object objects]
       [:Deleted [:Key (str object)]])]))

(defn bucket-requestpayment
  []
  (seq->xmlstr
   [:RequestPaymentConfiguration xml-ns
    [:Payer "BucketOwner"]]))

(defn get-bucket-versioning
  "Template for the get bucket versioning response"
  [versioned?]
  (seq->xmlstr
   [:VersioningConfiguration xml-ns
    [:Status (if versioned? "Enabled" "Suspended")]]))

(defn empty-tag-list
  "Placeholder tag list"
  []
  (seq->xmlstr
   [:Tagging
    [:TagSet]]))

(defn exception
  "Dispatch on the type of exception we got and apply appropriate template.
   Thankfully, we have a nice error message list in the S3 documentation:

   http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html"
  [request exception]
  (let [{:keys [type] :or {type :generic} :as payload} (ex-data exception)
        reqid (str (:reqid request))]
    (seq->xmlstr
     (case type
       :invalid-request
       [:Error
        [:Code "InvalidRequest"]
        [:Message "Not implemented yet"]
        [:RequestId reqid]
        [:HostId reqid]]
       :access-denied
       [:Error
        [:Code "AccessDenied"]
        [:Message "Access Denied"]
        [:RequestId reqid]
        [:HostId reqid]]
       :signature-does-not-match
       [:Error
        [:Code "SignatureDoesNotMatch"]
        [:Message (str "The request signature we calculated does not match "
                       "the signature you provided. "
                       "Check your key and signing method.")]
        [:RequestId reqid]
        [:HostId reqid]
        [:StringToSignBytes
         (->> payload
              :to-sign
              .getBytes seq (map (partial format "%02x")) (s/join " "))]
        [:StringToSign (:to-sign payload)]]
       :expired-request
       [:Error
        [:Code "AccessDenied"]
        [:Message "Request has expired"]
        [:Expires (:expires payload)]
        [:ServerTime (iso8601 (now))]
        [:RequestId reqid]
        [:HostId reqid]]
       :no-such-upload
       [:Error
        [:Code "NoSuchUpload"]
        [:Message "The specified upload-id does not exist for this key."]
        [:Key (str (:key payload))]
        [:Upload (str (:upload payload))]
        [:RequestId reqid]
        [:HostId reqid]]
       :no-such-key
       [:Error
        [:Code "NoSuchKey"]
        [:Message "The specified key does not exist."]
        [:Key (:key payload)]
        [:RequestId reqid]
        [:HostId reqid]]
       :no-such-bucket
       [:Error
        [:Code "NoSuchBucket"]
        [:Message "The specified bucket does not exist"]
        [:BucketName (:bucket payload)]
        [:RequestId reqid]
        [:HostId reqid]]
       :no-such-bucket-policy
       [:Error
        [:Code "NoSuchBucketPolicy"]
        [:Message "The bucket policy does not exist"]
        [:RequestId reqid]
        [:HostId reqid]
        [:BucketName (:bucket payload)]]
       :bucket-not-empty
       [:Error
        [:Code "BucketNotEmpty"]
        [:Message "The bucket you tried to delete is not empty"]
        [:BucketName (:bucket payload)]
        [:HostId reqid]
        [:RequestId reqid]]
       :invalid-argument
       [:Error
        [:Code "InvalidArgument"]
        [:Message "Invalid Argument"]
        [:ArgumentName (:arg payload)]
        [:ArgumentValue (:val payload)]
        [:HostId reqid]
        [:RequestId reqid]]
       :upload-policy-violation
       [:Error
        [:Code "UploadPolicyViolation"]
        [:Message (str "Upload request violates upload policy")]
        [:RequestId reqid]
        [:HostId reqid]
        [:Field (str (:field payload))]
        [:Value (str (:value payload))]
        [:Expected "XXXX"]]
       :invalid-acl-xml
       [:Error
        [:Code "MalformedACLError"]
        [:Message (str "The XML you provided was not well-formed "
                       "or did not validate against our published schema.")]
        [:RequestId reqid]
        [:HostId reqid]]
       :invalid-cors-xml
       [:Error
        [:Code "MalformedXML"]
        [:Message (str "The XML you provided was not well-formed "
                       "or did not validate against our published schema.")]
        [:RequestId reqid]
        [:HostId reqid]]
       :cors-not-enabled
       [:Error
        [:Code "AccessForbidden"]
        [:Message "CORS Response: CORS is not enabled for this Bucket"]
        [:RequestId reqid]
        [:HostId reqid]]
       :bucket-already-exists
       [:Error
        [:Code "BucketAlreadyExists"]
        [:Message
         (str "The requested bucket name is not available. "
              "The bucket namespace is shared by all users of the system. "
              "Please select a different name and try again.")]
        [:BucketName (:bucket payload)]
        [:RequestId reqid]
        [:HostId reqid]]
       :no-such-cors
       [:Error
        [:Code "NoSuchCORSConfiguration"]
        [:BucketName (:bucket payload)]
        [:Message "The CORS configuration does not exist"]]
       :no-such-lifecycle-config
       [:Error
        [:Code "NoSuchLifecycleConfiguration"]
        [:Message "The lifecycle configuration does not exist"]
        [:BucketName (:bucket payload)]]
       :forbidden
       [:Error
        [:Code "Forbidden"]
        [:Message "Forbidden"]
        [:RequestId reqid]
        [:HostId reqid]]
       [:Error
        [:Code "Unknown"]
        [:Message (format-exception exception)]
        [:RequestId reqid]
        [:HostId reqid]]))))
