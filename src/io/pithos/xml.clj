(ns io.pithos.xml
  "This namespace provides plumbing to output XML as well as templates.
  Our intent here is to avoid having to deal with presentation elsewhere,
  instead, data structures should be emitted by other subsystems, this
  namespace will then take care of making them suitable for sending out
  on the wire."
  (:require [clojure.data.xml :refer [->Element emit-str indent-str]]
            [clojure.pprint   :refer [pprint]]
            [clojure.string   :as s]
            [io.pithos.sig    :as sig]))

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
  (letfn [(tag-nodes->xml [[tag & nodes]]
            (let [attrs (if (map? (first nodes)) (first nodes) {})
                  nodes (if (map? (first nodes)) (rest nodes) nodes)]
              (->Element (name tag)
                         attrs
                         (if (every? sequential? nodes)
                           (mapv tag-nodes->xml (remove empty? nodes))
                           (first nodes)))))]
    (tag-nodes->xml input)))

(defn seq->xmlstr
  "Given a valid input for `seq->xml` output an xml string"
  [s]
  (indent-str (seq->xml s)))

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
           (for [{:keys [bucket]} buckets]
             [:Bucket
              [:Name bucket]
              [:CreationDate "2013-09-12T16:16:38.000Z"]]))]))

(defn list-bucket
  "Template for the list-bucket operation response"
  [[files prefixes] {:keys [tenant bucket]} {:keys [prefix delimiter]}]
  (seq->xmlstr
   (apply vector
          :ListBucketResult xml-ns
          [:Name bucket]
          [:Prefix prefix]
          [:MaxKeys (str 100)]
          [:Delimiter delimiter]
          [:IsTruncated "false"]
          (for [{:keys [object size] :or {size 0}} files]
            [:Contents
             [:Key object]
             [:LastModified "2013-09-15T20:52:35.000Z"]
             [:ETag "41d8cd98f00b204e9800998ecf8427"]
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
  [uploads bucket]
  (seq->xmlstr
   [:ListMultipartUploadsResult xml-ns
    [:Bucket bucket]
    [:KeyMarker]
    [:UploadIdMarker]
    (apply vector :Uploads
           (for [{:keys [object upload]} uploads]
             [:Upload
              [:Key object]
              [:UploadId (str upload)]]))]))

(defn list-upload-parts
  "Template for the list multipart upload parts response"
  [parts upload bucket object]
  (seq->xmlstr
   (apply vector :ListPartsResult xml-ns
          [:Bucket bucket]
          (for [{:keys [partnumber modified hash size]} parts]
            [:Part
             [:PartNumber partnumber]
             [:LastModified modified]
             [:ETag hash]
             [:Size (str size)]]))))

(defn complete-multipart-upload
  [bucket object etag]
  (seq->xmlstr
   [:CompleteMultipartUploadResult xml-ns
    [:Bucket bucket]
    [:Key object]
    [:Location (format "http://%s.s3.amazonaws.com/%s" bucket object)]
    [:ETag etag]]))

(defn exception
  "Dispatch on the type of exception we got and apply appropriate template.
   Thankfully, we have a nice error message list in the S3 documentation:

   http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html"
  [request exception]
  (let [{:keys [type] :or {type :generic} :as payload} (ex-data exception)
        reqid (str (:reqid request))]
    (seq->xmlstr
     (case type
       :access-denied
       [:Error
        [:Code "AccessDenied"]
        [:Message "Access Denied"]
        [:RequestId reqid]
        [:HostId reqid]]
       :signature-does-not-match
       [:Error
        [:Code "SignatureDoesNotMatch"]
        [:Message "The request signature we calculated does not match the signature you provided. Check your key and signing method."]
        [:RequestId reqid]
        [:HostId reqid]
        [:ExpectedSignature (:expected payload)]
        [:StringToSignBytes
         (->> payload
              :to-sign
              .getBytes seq (map (partial format "%02x")) (s/join " "))]
        [:StringToSign (:to-sign payload)]]
       :no-such-bucket
       [:Error
        [:Code "NoSuchBucket"]
        [:Message "The specified bucket does not exist"]
        [:BucketName (:bucket payload)]
        [:RequestId reqid]
        [:HostId reqid]]
       :bucket-already-exists
       [:Error
        [:Code "BucketAlreadyExists"]
        [:Message "The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again."]
        [:BucketName (:bucket payload)]
        [:RequestId reqid]
        [:HostId reqid]]
       [:Error
        [:Code "Unknown"]
        [:Message (str exception)]
        [:RequestId reqid]
        [:HostId reqid]]))))
