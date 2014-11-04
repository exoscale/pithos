(ns io.pithos.operations-test
  (:require [clojure.test         :refer :all]
            [io.pithos.desc       :as desc]
            [io.pithos.bucket     :as bucket]
            [io.pithos.meta       :as meta]
            [io.pithos.blob       :as blob]
            [io.pithos.store      :as store]
            [io.pithos.reporter   :as reporter]
            [io.pithos.sig        :as sig]
            [io.pithos.api        :as api]
            [io.pithos.system     :as system]
            [io.pithos.operations :refer [get-range]]
            [io.pithos.util       :refer [inc-prefix iso8601->rfc822
                                          iso8601-timestamp]]))

(deftest range-test
  (let [desc   (reify desc/BlobDescriptor (size [this] 1024))
        in-out ["no headers"    {}                                  [0  1024]
                "range"         {"range" "bytes=10-90"}             [10 90]
                "content-range" {"content-range" "bytes 10-90/200"} [10 90]
                "begin-range1"  {"range" "bytes=10-"}               [10 1024]
                "begin-range2"  {"content-range" "bytes 10-/200"}   [10 1024]
                "priority"      {"range"         "bytes=1-"
                                 "content-range" ""}                [1 1024]]]
    (doseq [[nickname headers output] (partition 3 in-out)]
      (testing (str "valid output for " nickname)
        (is (= output (get-range desc headers)))))))

(deftest range-exception-test
  (let [d (reify desc/BlobDescriptor (size [this] :none))]
    (testing "malformed ranges"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"invalid range"
                            (get-range d {"range" "blah"})))
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"invalid value"
                            (get-range d {"range" "bytes=500-bla"}))))))
(defn atom-bucket-store
  [state]
  (reify
    store/Convergeable
    (converge! [this])
    store/Crudable
    (create! [this tenant bucket columns]
      (swap! state assoc-in [:buckets bucket]
             (assoc columns :tenant tenant :bucket bucket)))
    (delete! [this bucket]
      (swap! state update-in [:buckets] dissoc bucket))
    (update! [this bucket columns]
      (swap! state update-in [:buckets] merge columns))
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
       (fn [prefix marker limit]
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
             merge columns))
    (initiate-upload! [this bucket object upload metadata]
      (swap! state assoc-in [:uploads bucket object upload :meta] metadata))
    (get-upload-details [this bucket object upload]
      (get-in @state [:uploads bucket object upload]))
    (list-uploads [this bucket]
      (get-in @state [:uploads bucket]))
    (list-object-uploads [this bucket object]
      (get-in @state [:uploads bucket object]))
    (list-upload-parts [this bucket object upload]
      (get-in @state [:uploads bucket object upload :parts]))))

(defn atom-blob-store
  [state max-chunk-size max-block-chunks]
  (reify
    store/Convergeable
    (converge! [this])
    store/Crudable
    (delete! [this inode version]
      (swap! state update-in [:inodes] (dissoc [inode version])))
    blob/Blobstore

    (blocks [this od]
      (sort (get-in @state [:inodes [(desc/inode od) (desc/version od)]
                            :blocks])))
    (max-chunk [this] max-chunk-size)
    (chunks [this od block offset]
      (drop-while
       (comp #(< % offset) :offset)
       (sort-by :offset
                (get-in @state [:inodes [(desc/inode od) (desc/version od)]
                                :chunks block]))))
    (boundary? [this block offset]
      (>= offset (+ block (max-chunk-size max-block-chunks))))
    (start-block! [this od block]
      (swap! state update-in
             [:inodes [(desc/inode od) (desc/version od)] :blocks]
             conj block))
    (chunk! [this od block offset chunk]
      (swap! @state [:inodes [(desc/inode od) (desc/version od)] :chunks block]
             conj {:offset offset :chunk chunk :chunksize
                   (- (.limit chunk) (.position chunk))}))))

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
    (let [sig (sig/sign-request request key key)]
      (assoc-in request [:headers "authorization"]
                (format "AWS %s:%s" key sig)))))

(deftest integration-test

  (let [state    (atom {})
        key      "AKIAIOSFODNN7EXAMPLE"
        keystore {key {:secret key
                       :tenant "foo@example.com"}}
        system   (atom-system "blob.example.com" state keystore 16384 1024)
        handler  (comp (api/executor system) (signer (name key)))
        date!    (comp iso8601->rfc822 iso8601-timestamp)]

    (testing "get service 1"
      (handler {:request-method :put
                :headers {"host" "batman.blob.example.com"
                          "date" (date!)}
                :sign-uri "/batman/"
                :uri "/"})
      (is (= #{"batman"} (-> @state :buckets keys set))))))
