(ns io.pithos.operations-test
  (:require [clojure.test         :refer :all]
            [clojure.java.io      :as io]
            [clojure.core.async   :as a]
            [io.pithos.desc       :as desc]
            [io.pithos.bucket     :as bucket]
            [io.pithos.meta       :as meta]
            [io.pithos.blob       :as blob]
            [io.pithos.store      :as store]
            [io.pithos.reporter   :as reporter]
            [io.pithos.sig        :as sig]
            [io.pithos.api        :as api]
            [io.pithos.system     :as system]
            [io.pithos.xml        :as xml]
            [io.pithos.operations :refer [get-range]]
            [io.pithos.util       :refer [inc-prefix iso8601->rfc822
                                          iso8601-timestamp
                                          md5-init md5-sum md5-update]]
            [clojure.data.xml     :refer [->Element emit-str parse-str]]
            [clojure.zip          :refer [xml-zip]]
            [clojure.data.zip.xml :refer [xml-> xml1-> text]]
            [clojure.pprint       :refer [pprint]]))

(deftest range-test
  (let [desc   (reify desc/BlobDescriptor (size [this] 1024))
        in-out ["no headers"    {}                                  [false 0  1024]
                "range"         {"range" "bytes=10-90"}             [true 10 91]
                "content-range" {"content-range" "bytes 10-90/200"} [true 10 91]
                "exceed-range"  {"range" "bytes=10-1026"}           [true 10 1024]
                "begin-range1"  {"range" "bytes=10-"}               [true 10 1024]
                "begin-range2"  {"content-range" "bytes 10-/200"}   [true 10 1024]
                "priority"      {"range"         "bytes=1-"
                                 "content-range" ""}                [true 1 1024]]]
    (doseq [[nickname headers output] (partition 3 in-out)]
      (testing (str "valid output for " nickname)
        (is (= output (get-range desc headers)))))))

(deftest range-exception-test
  (let [d (reify desc/BlobDescriptor (size [this] 1024))]
    (testing "out of bounds ranges"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"unsatisfiable range"
                            (get-range d {"range" "bytes=1027-2"}))))
    (testing "malformed ranges"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"unsatisfiable range"
                            (get-range d {"range" "blah"})))
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"unsatisfiable range"
                            (get-range d {"range" "bytes=500-bla"}))))))
(defn atom-bucket-store
  [state]
  (reify
    store/Convergeable
    (converge! [this])
    store/Crudable
    (create! [this tenant bucket columns]
      (let [columns (merge {:created (iso8601-timestamp)
                            :region  :myregion
                            :tenant  tenant
                            :bucket bucket}
                           columns)]
        (swap! state assoc-in [:buckets bucket] columns)))
    (delete! [this bucket]
      (swap! state update-in [:buckets] dissoc bucket))
    (update! [this bucket columns]
      (swap! state update-in [:buckets bucket] merge columns))
    bucket/Bucketstore
    (by-tenant [this tenant]
      (filter (comp (partial = tenant) :tenant) (:buckets @state)))
    (by-name [this bucket]
      (get-in @state [:buckets bucket]))))

(defn atom-meta-store
  [state]
  (reify
    store/Convergeable
    (converge! [this])
    store/Crudable
    (fetch [this bucket object fail?]
      (or
       (get-in @state [:objects bucket object])
       (and fail? (throw (ex-info "no such object" {})))))
    (fetch [this bucket object]
      (store/fetch this bucket object false))
    (update! [this bucket object columns]
      (swap! state update-in [:objects bucket object] merge columns))
    (delete! [this bucket object]
      (swap! state update-in [:objects bucket] dissoc object))

    meta/Metastore
    (prefixes [this bucket params]
      (meta/get-prefixes
       (fn [prefix marker limit init?]
         (let [>pred   #(or (= (:object %) (or marker prefix))
                            (not (.startsWith (or (:object %) "")
                                              (or marker prefix ""))))
               <pred   #(or (empty? prefix)
                            (neg? (compare (:object %) (inc-prefix prefix))))]
           (->> (get-in @state [:objects bucket])
                (sort-by :object)
                (drop-while >pred)
                (take-while <pred)
                (take limit))))
       params))

    (abort-multipart-upload! [this bucket object upload]
      (swap! state update-in [:uploads bucket object] dissoc upload))
    (update-part! [this bucket object upload partno columns]
      (swap! state update-in [:uploads bucket object upload :parts partno]
             merge (assoc columns :partno partno)))
    (initiate-upload! [this bucket object upload metadata]
      (swap! state assoc-in [:uploads bucket object upload :meta] metadata))
    (get-upload-details [this bucket object upload]
      (get-in @state [:uploads bucket object upload]))
    (list-uploads [this bucket]
      (get-in @state [:uploads bucket]))
    (list-object-uploads [this bucket object]
      (get-in @state [:uploads bucket object]))
    (list-upload-parts [this bucket object upload]
      (vals (get-in @state [:uploads bucket object upload :parts])))))

(defn atom-blob-store
  [state max-chunk-size max-block-chunks]
  (reify
    store/Convergeable
    (converge! [this])
    store/Crudable
    (delete! [this inode version]
      (swap! state update-in [:inodes] dissoc [inode version]))
    blob/Blobstore

    (blocks [this od]
      (sort-by :block
               (get-in @state [:inodes [(desc/inode od) (desc/version od)]
                               :blocks])))
    (max-chunk [this] max-chunk-size)
    (chunks [this od block offset]
      (drop-while
       (comp #(< % offset) :offset)
       (sort-by :offset
                (get-in @state [:inodes [(desc/inode od) (desc/version od)]
                                :chunks block]))))
    (boundary? [this block offset]
      (>= offset (+ block (* max-chunk-size max-block-chunks))))
    (start-block! [this od block]
      (swap! state update-in
             [:inodes [(desc/inode od) (desc/version od)] :blocks]
             conj {:block block}))
    (chunk! [this od block offset chunk]
      (let [size (- (.limit chunk) (.position chunk))]
        (swap! state update-in
               [:inodes [(desc/inode od) (desc/version od)] :chunks block]
               conj {:offset offset :payload chunk :chunksize size})
        size))))

(defn atom-reporter
  [state]
  (reify
    reporter/Reporter
    (report! [this event]
      (swap! state update-in [:reports] conj event))))

;; keystores are expected to behave as maps, so can be plain maps

(defn atom-system
  [uri state keys max-chunk-size max-block-chunks]
  (let [bucketstore (atom-bucket-store state)
        metastore   (atom-meta-store state)
        blobstore   (atom-blob-store state max-chunk-size max-block-chunks)
        reporter    (atom-reporter state)
        keystore    keys]
    (system/system-descriptor
     {:bucketstore bucketstore
      :keystore keystore
      :options {:service-uri uri
                :default-region :myregion}
      :regions {:myregion {:metastore metastore
                           :storage-classes {:standard blobstore}}}
      :reporters [reporter]})))

(defn signer
  [key]
  (fn [request]
    (if-not (:anonymous! request)
      (let [sig (sig/sign-request request key)]
        (assoc-in request [:headers "authorization"]
                  (format "AWS %s:%s" key sig)))
      request)))

(deftest integration-test

  (let [state    (atom {})
        key      "AKIAIOSFODNN7EXAMPLE"
        keystore {key {:secret key
                       :tenant "foo@example.com"}}
        system   (atom-system "blob.example.com" state keystore 16384 1024)
        handler  (comp (api/executor system) (signer (name key)))
        date!    (comp iso8601->rfc822 iso8601-timestamp)
        sum      #(-> (md5-init)
                      (md5-update (.getBytes %) 0 (count %))
                      (md5-sum))]

    (testing "put to inexistant bucket yields 404"
      (let [resp (handler {:request-method :put
                           :headers {"host" "batman.blob.example.com"
                                     "date" (date!)}
                           :sign-uri "/batman/foo.txt"
                           :uri "/foo.txt"
                           :body (java.io.ByteArrayInputStream. (.getBytes "foobar"))})]
        (is (= (:status resp) 404))))

    (testing "invalid bucket names"
      (doseq [bname ["foo bar" "foo@bar" "foo\nbar" "foo%bar" ""
                     "thisbucketnameiswaytoolongbecauseitismorethan63characterslongwhichistoolong"]]
        (let [resp (handler {:request-method :put
                             :headers {"host" (format "%s.blob.example.com" bname)
                                       "date" (date!)}
                             :sign-uri (format "/%s/" bname)
                             :uri "/"})]
          (= (:status resp) 400))))

    (testing "put bucket"
      (handler {:request-method :put
                :headers {"host" "batman.blob.example.com"
                          "date" (date!)}
                :sign-uri "/batman/"
                :uri "/"})

      (handler {:request-method :put
                :headers {"host" "foo.blob.example.com"
                          "date" (date!)}
                :sign-uri "/foo/"
                :uri "/"})

      (is (= #{"batman" "foo"} (-> @state :buckets keys set)))

      (is (= (pr-str {:FULL_CONTROL [{:ID "foo@example.com"
                                      :DisplayName "foo@example.com"}]})
             (-> @state :buckets (get "batman") :acl))))

    (testing "put to wrong bucket yields 404"
      (let [resp (handler {:request-method :put
                           :anonymous! true
                           :headers {"host" "batman.blob.example.com"
                                     "date" (date!)}
                           :sign-uri "/batman/foo.txt"
                           :uri "/foo.txt"
                           :body (java.io.ByteArrayInputStream. (.getBytes "foobar"))})]
        (is (= (:status resp) 403))))

    (testing "remove bucket"
      (handler {:request-method :delete
                :headers {"host" "foo.blob.example.com"
                          "date" (date!)}
                :sign-uri "/foo/"
                :uri "/"})

      (is (= #{"batman"} (-> @state :buckets keys set))))

    (testing "put object"
      (handler {:request-method :put
                :headers {"host" "batman.blob.example.com"
                          "date" (date!)}
                :sign-uri "/batman/foo.txt"
                :uri "/foo.txt"
                :body (java.io.ByteArrayInputStream. (.getBytes "foobar"))})
      (is (= (sum "foobar")
             (get-in @state [:objects "batman" "foo.txt" :checksum])))

      (is (= 6 (get-in @state [:objects "batman" "foo.txt" :size])))

      (is (= (-> @state :reports first)
             {:type :put :bucket "batman" :object "foo.txt" :size 6})))

    (testing "put object with metadata"
      (handler {:request-method :put
                :headers {"host" "batman.blob.example.com"
                          "x-amz-meta-foo" "bar"
                          "date" (date!)}
                :sign-uri "/batman/foo-meta.txt"
                :uri "/foo-meta.txt"
                :body (java.io.ByteArrayInputStream. (.getBytes "foobar"))})
      (is (= (sum "foobar")
             (get-in @state [:objects "batman" "foo-meta.txt" :checksum])))

      (is (= "bar"
             (get-in @state [:objects "batman" "foo-meta.txt"
                             :metadata "x-amz-meta-foo"])))

      (is (= 6 (get-in @state [:objects "batman" "foo.txt" :size])))

      (let [resp (handler {:request-method :head
                           :headers {"host" "batman.blob.example.com"
                                     "date" (date!)}
                           :sign-uri "/batman/foo-meta.txt"
                           :uri "/foo-meta.txt"})]
        (is (= (get-in resp [:headers "x-amz-meta-foo"]) "bar"))))

    (testing "get object"
      (let [response (handler {:request-method :get
                               :headers {"host" "batman.blob.example.com"
                                         "date" (date!)}
                               :sign-uri "/batman/foo.txt"
                               :uri "/foo.txt"})]
        (is (= (:status response) 200))
        (is (= (slurp (:body response)) "foobar"))))

    (testing "put object with prefix"
      (handler {:request-method :put
                :headers {"host" "batman.blob.example.com"
                          "date" (date!)}
                :sign-uri "/batman/blah/foo.txt"
                :uri "/blah/foo.txt"
                :body (java.io.ByteArrayInputStream. (.getBytes "foobar"))})

      (is (= (sum "foobar")
             (get-in @state [:objects "batman" "blah/foo.txt" :checksum]))))

    (testing "get bucket"
      (let [response (handler {:request-method :get
                               :headers {"host" "batman.blob.example.com"
                                         "date" (date!)}
                               :sign-uri "/batman/"
                               :uri      ""})]
        (println (:body response))
        (is (= (:status response) 200))))

    (testing "get bucket with 0 max-keys"
      (let [response (handler {:request-method :get
                               :headers {"host" "batman.blob.example.com"
                                         "date" (date!)}
                               :sign-uri "/batman/"
                               :query-string "max-keys=0&delimiter=/"
                               :uri      ""})]
        (println (:body response))
        (is (= (:status response) 200))))

    (testing "get bucket with 1 max-keys"
      (let [response (handler {:request-method :get
                               :headers {"host" "batman.blob.example.com"
                                         "date" (date!)}
                               :sign-uri "/batman/"
                               :query-string "max-keys=1&delimiter=/"
                               :uri      ""})]

        (println (:body response))
        (is (= (:status response) 200))))

    (testing "get object range"
      (let [response (handler {:request-method :get
                               :headers {"host" "batman.blob.example.com"
                                         "content-range" "bytes=0-2"
                                         "date" (date!)}
                               :sign-uri "/batman/foo.txt"
                               :uri "/foo.txt"})]
        (is (= (:status response) 206))
        (is (= (slurp (:body response)) "foo"))))

    (testing "object copy"
      (let [d    (date!)
            resp  (handler {:request-method :put
                            :headers {"host" "batman.blob.example.com"
                                      "date" d
                                      "x-amz-copy-source" "/batman/foo.txt"}
                            :sign-uri "/batman/foo-copy.txt"
                            :uri "/foo-copy.txt"})

            date (get-in @state [:objects "batman" "foo-copy.txt" :atime])]
        (is (= (sum "foobar")
               (get-in @state [:objects "batman" "foo-copy.txt" :checksum])))
        (let [payload (format (str "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                                   "<CopyObjectResult xmlns=\"%s\">"
                                   "<LastModified>%s</LastModified>"
                                   "<ETag>\"%s\"</ETag></CopyObjectResult>")
                              (:xmlns xml/xml-ns) date (sum "foobar"))]
          (is (= payload (:body resp))))))

    (testing "multipart upload"
      (reset! state {})
      (handler {:request-method :put
                :headers {"host" "batman.blob.example.com"
                          "date" (date!)}
                :sign-uri "/batman/"
                :uri "/"})


      (let [upload-id (-> (handler {:request-method :post
                                    :headers {"host" "batman.blob.example.com"
                                              "date" (date!)}
                                    :sign-uri "/batman/foo-multi.txt?uploads"
                                    :query-string "uploads"
                                    :uri "/foo-multi.txt"})
                          :body
                          (parse-str)
                          (xml-zip)
                          (xml1-> :UploadId text))]
        (handler {:request-method :put
                  :headers {"host" "batman.blob.example.com"
                            "date" (date!)}
                  :sign-uri (str "/batman/foo-multi.txt?partNumber=1&uploadId="
                                 upload-id)
                  :query-string (str "uploadId=" upload-id "&partNumber=1")
                  :uri "/foo-multi.txt"
                  :body (java.io.ByteArrayInputStream. (.getBytes "foo"))})
        (handler {:request-method :put
                  :headers {"host" "batman.blob.example.com"
                            "date" (date!)}
                  :sign-uri (str "/batman/foo-multi.txt?partNumber=2&uploadId="
                                 upload-id)
                  :query-string (str "uploadid=" upload-id "&partnumber=2")
                  :uri "/foo-multi.txt"
                  :body (java.io.ByteArrayInputStream. (.getBytes "bar"))})
        (->>
         (handler {:request-method :post
                   :headers {"host" "batman.blob.example.com"
                             "date" (date!)}
                   :sign-uri (str "/batman/foo-multi.txt?uploadId=" upload-id)
                   :uri "/foo-multi.txt"
                   :query-string (str "uploadid=" upload-id)
                   :body (java.io.ByteArrayInputStream.
                          (.getBytes
                           (str
                            "<CompleteMultipartUpload>"
                            "<Part><PartNumber>1</PartNumber><ETag>"
                            (sum "foo") "</ETag></Part>"
                            "<Part><PartNumber>2</PartNumber><ETag>"
                            (sum "bar") "</ETag></Part>"
                            "</CompleteMultipartUpload>")))})
         :body
         (a/reduce str "")
         (a/<!!))

        (is (= (sum "foobar")
               (get-in @state [:objects "batman" "foo-multi.txt" :checksum])))))

    (testing "invalid multipart upload"

      (reset! state {})

      (handler {:request-method :put
                :headers {"host" "batman.blob.example.com"
                          "date" (date!)}
                :sign-uri "/batman/"
                :uri "/"})

      (let [upload-id (-> (handler {:request-method :post
                                    :headers {"host" "batman.blob.example.com"
                                              "date" (date!)}
                                    :sign-uri "/batman/foo-multi.txt?uploads"
                                    :query-string "uploads"
                                    :uri "/foo-multi.txt"})
                          :body
                          (parse-str)
                          (xml-zip)
                          (xml1-> :UploadId text))
            resp      (handler {:request-method :put
                                :headers {"host" "batman.blob.example.com"
                                          "date" (date!)}
                                :sign-uri (str "/batman/foo-multi-else.txt?partNumber=1&uploadId="
                                               upload-id)
                                :query-string (str "uploadId=" upload-id "&partNumber=1")
                                :uri "/foo-multi-else.txt"
                                :body (java.io.ByteArrayInputStream. (.getBytes "foo"))})]
        (is (= (:status resp) 404))))


    (testing "cors headers with OPTIONS"
      (reset! state {})
      (handler {:request-method :put
                :headers {"host" "batman.blob.example.com"
                          "date" (date!)}
                :sign-uri "/batman/"
                :uri "/"})

      (let [response (handler {:request-method :put
                               :headers {"host" "batman.blob.example.com"
                                         "date" (date!)}
                               :sign-uri "/batman/?cors"
                               :uri "/"
                               :query-string "cors"
                               :body (java.io.ByteArrayInputStream.
                                      (.getBytes
                                       (slurp (io/resource "cors1.xml"))))})]
        (is (= (:status response) 200)))

      (let [response (handler {:request-method :options
                               :headers {"host" "batman.blob.example.com"
                                         "date" (date!)
                                         "access-control-request-method" "GET"
                                         "origin" "http://batman.example.com"}
                               :sign-uri "/batman/foo.txt"
                               :uri "/foo.txt"})]
        (is (= (:status response) 204))
        (is (= (get-in response [:headers "Access-Control-Allow-Origin"])
               "http://batman.example.com"))))

    (testing "cors headers with GET"
      (let [response (handler {:request-method :put
                               :headers {"host" "batman.blob.example.com"
                                         "date" (date!)}
                               :sign-uri "/batman/?cors"
                               :uri "/"
                               :query-string "cors"
                               :body (java.io.ByteArrayInputStream.
                                      (.getBytes
                                       (slurp (io/resource "cors1.xml"))))})]
        (is (= (:status response) 200)))

      (let [response (handler {:request-method :put
                               :headers {"host" "batman.blob.example.com"
                                         "date" (date!)}
                               :sign-uri "/batman/foo.txt"
                               :uri "/foo.txt"
                               :body (java.io.ByteArrayInputStream.
                                      (.getBytes "foobar"))})]
        (is (= (:status response) 200)))

      (let [cnt      (-> "form-upload1.txt" io/resource io/input-stream slurp count)
            body     (-> "form-upload1.txt" io/resource io/input-stream)
            response (handler {:request-method :post
                               :anonymous! true
                               :headers {"host" "batman.blob.example.com"
                                         "content-type" "multipart/form-data; boundary=9431149156168"
                                         "content-length" (str cnt)}
                               :sign-uri "/batman/"
                               :uri "/"
                               :body body})]
        (is (= (:status response) 201)))

      (let [response (handler {:request-method :get
                               :headers {"host" "batman.blob.example.com"
                                         "date" (date!)
                                         "origin" "http://batman.example.com"}
                               :sign-uri "/batman/qux"
                               :uri "/qux"})]
        (is (= (:status response) 200))
        (is (= (slurp (:body response)) "not much to say.")))

      (let [cnt      (-> "form-upload2.txt" io/resource io/input-stream slurp count)
            body     (-> "form-upload2.txt" io/resource io/input-stream)
            response (handler {:request-method :post
                               :anonymous! true
                               :headers {"host" "batman.blob.example.com"
                                         "content-type" "multipart/form-data; boundary=9431149156168"
                                         "content-length" (str cnt)}
                               :sign-uri "/batman/"
                               :uri "/"
                               :body body})]
        (is (= (:status response) 403)))

      (let [response (handler {:request-method :get
                               :headers {"host" "batman.blob.example.com"
                                         "date" (date!)
                                         "origin" "http://batman.example.com"}
                               :sign-uri "/batman/foo.txt"
                               :uri "/foo.txt"})]
        (is (= (:status response) 200))
        (is (= (get-in response [:headers "Access-Control-Allow-Origin"])
               "http://batman.example.com")))

      (let [response (handler {:request-method :get
                               :headers {"host" "batman.blob.example.com"
                                         "date" (date!)
                                         "origin" "http://batman.example.com"}
                               :sign-uri "/batman/foobar.txt"
                               :uri "/foobar.txt"})]
        (is (= (:status response) 404))
        (is (= (get-in response [:headers "Access-Control-Allow-Origin"])
               "http://batman.example.com"))))

    (testing "response overrides"

      (let [response (handler {:request-method :get
                               :query-string "response-content-type=application/json&response-content-disposition=blargh"
                               :headers {"host" "batman.blob.example.com"
                                         "date" (date!)
                                         "origin" "http://batman.example.com"}
                               :sign-uri "/batman/foo.txt?response-content-disposition=blargh&response-content-type=application/json"
                               :uri "/foo.txt"})]
        (is (= (:status response 200)))
        (is (= (get-in response [:headers "Content-Type"]) "application/json"))
        (is (= (get-in response [:headers "Content-Disposition"]) "blargh"))))

    ))
