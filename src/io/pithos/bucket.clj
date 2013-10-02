(ns io.pithos.bucket
  (:require [qbits.alia      :refer [execute with-session]]
            [qbits.hayt      :refer [select where set-columns
                                     delete update limit]]
            [io.pithos.store :as store]))

(defprotocol BucketStore
  (by-tenant [this tenant])
  (by-name [this bucket])
  (create! [this tenant bucket columns])
  (update! [this bucket columns])
  (delete! [this bucket]))

(defn bucket-by-tenant-q
  [tenant]
  (select :bucket (where {:tenant tenant})))

(defn fetch-bucket-q
  [bucket]
  (select :bucket (where {:bucket bucket}) (limit 1)))

(defn update-bucket-q
  [bucket columns]
  (update :bucket
          (set-columns columns)
          (where {:bucket bucket})))

(defn delete-bucket-q
  [bucket]
  (delete :bucket (where {:bucket bucket})))

(defn cassandra-bucket-store
  [config]
  (let [session (store/cassandra-store config)]
    (reify BucketStore
      (by-tenant [this tenant]
        (execute session (bucket-by-tenant-q tenant)))
      (by-name [this bucket]
        (first
         (execute session (fetch-bucket-q bucket))))
      (create! [this tenant bucket columns]
        (if-let [[details] (seq (execute session (fetch-bucket-q bucket)))]
          (when (not= tenant (:tenant details))
            (throw (ex-info 
                    "bucket already exists"
                    {:type :bucket-already-exists
                     :bucket bucket
                     :status-code 409})))
          (execute session
                   (update-bucket-q bucket 
                                    (merge columns {:tenant tenant})))))
      (update! [this bucket columns]
        (execute session (update-bucket-q bucket columns)))
      (delete! [this bucket]
        (with-session session
          (if-let [info (seq (execute (fetch-bucket-q bucket)))]
            (execute (delete-bucket-q bucket))
            (throw (ex-info "bucket not found"
                            {:type        :no-such-bucket
                             :status-code 404
                             :bucket      bucket}))))))))
