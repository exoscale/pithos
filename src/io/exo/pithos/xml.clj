(ns io.exo.pithos.xml
  (:require [clojure.data.xml  :refer [->Element emit-str indent-str]]
            [clojure.string    :as s]
            [io.exo.pithos.sig :as sig]))

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
  [{:keys [type request to-sign expected] :or {type :generic} :as payload}]
  (indent-str
   (case type
     :invalid-signature
     (e :Error {}
        (e :Code {} "SignatureDoesNotMatch")
        (e :Message {} "The request signature we calculated does not match the signature you provided. Check your key and signing method.")
        (e :ExpectedSignature {} expected)
        (e :StringToSignBytes {} 
           (->> to-sign
                .getBytes seq (map (partial format "%02x")) (s/join " ")))
        (e :StringToSign {} to-sign))
     (e :Error {}
        (e :Code {} "Unknown")
        (e :Message {} (str (:exception payload)))))))
