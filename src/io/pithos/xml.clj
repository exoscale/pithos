(ns io.pithos.xml
  (:require [clojure.data.xml :refer [->Element emit-str indent-str]]
            [clojure.pprint   :refer [pprint]]
            [clojure.string   :as s]
            [io.pithos.sig    :as sig]))

(defn e
  ([x]
     (e x {} nil))
  ([x attrs & elements]
     (->Element (name x) attrs (flatten elements))))

(def xml-ns {:xmlns "http://s3.amazonaws.com/doc/2006-03-01/"})

(defn list-all-my-buckets
  [[bucket :as buckets]]
  (indent-str
   (e :ListAllMyBucketsResult
      xml-ns
      (e :Owner {}
         (e :ID {} (:tenant bucket))
         (e :DisplayName {} (:tenant bucket)))
      (e :Buckets {}
         (doall
          (for [{:keys [bucket]} buckets]
            (e :Bucket {}
               (e :Name {} bucket)
               (e :CreationDate {} "2013-09-12T16:16:38.000Z"))))))))

(defn unknown
  [{:keys [action]}]
  (indent-str
   (e :UnknownAction
      xml-ns
      (e :Action {}
         (e :Code {} (name action))))))

(defn default
  [something]
  (indent-str
   (e :Output xml-ns (e :Payload {} (with-out-str (pprint something))))))

(defn list-bucket
  [tenant prefix delimiter max-keys bucket files prefixes]
  (indent-str
   (e :ListBucketResult {}
      (e :Name {} bucket)
      (e :Prefix {} prefix)
      (e :MaxKeys {} (str max-keys))
      (e :Delimiter {} delimiter)
      (e :IsTruncated {} "false")
      (for [{:keys [path size] :or {size 0}} files]
        (e :Contents {}
           (e :Key {} path)
           (e :LastModified {} "2013-09-15T20:52:35.000Z")
           (e :ETag {} "41d8cd98f00b204e9800998ecf8427")
           (e :Size {} (str size))
           (e :Owner {}
              (e :ID {} tenant)
              (e :DisplayName {} tenant))
           (e :StorageClass {} "Standard")))
      (for [prefix prefixes]
        (e :CommonPrefixes {}
           (e :Prefix {} prefix))))))

(defn exception
  [request exception]
  (let [{:keys [type] :or {type :generic} :as payload} (ex-data exception)
        reqid (str (:reqid request))]
    (indent-str
     (case type
       :signature-does-not-match
       (e :Error {}
          (e :Code {} "SignatureDoesNotMatch")
          (e :Message {} "The request signature we calculated does not match the signature you provided. Check your key and signing method.")
          (e :ExpectedSignature {} (:expected payload))
          (e :StringToSignBytes {} 
             (->> payload
                  :to-sign
                  .getBytes seq (map (partial format "%02x")) (s/join " ")))
          (e :StringToSign {} (:to-sign payload)))
       :no-such-bucket
       (e :Error {}
          (e :Code {} "NoSuchBucket")
          (e :Message {} "The specified bucket does not exist")
          (e :BucketName {} (:bucket payload))
          (e :RequestId {} reqid)
          (e :HostId {} reqid))
       :bucket-already-exists
       (e :Error {}
          (e :Code {} "BucketAlreadyExists")
          (e :Message {} "The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again.")
          (e :BucketName {} (:bucket payload))
          (e :RequestId {} reqid)
          (e :HostId {} reqid))
       (e :Error {}
          (e :Code {} "Unknown")
          (e :Message {} (str exception)))))))
