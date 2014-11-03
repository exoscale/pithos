(ns io.pithos.bucket
  "The bucketstore stores ring-global information on bucket
   ownership. It contains a single column-family and an
   accompanying index."
  (:refer-clojure :exclude [update])
  (:require [qbits.alia      :refer [execute]]
            [qbits.hayt      :refer [select where set-columns
                                     create-table create-index
                                     column-definitions index-name
                                     delete update limit]]
            [io.pithos.util  :refer [iso8601-timestamp]]
            [io.pithos.system :as system]
            [io.pithos.store :as store]))

(defprotocol Bucketstore
  "The bucketstore contains the schema migration function,
   two bucket lookup functions and CRUD signatures"
  (by-tenant [this tenant])
  (by-name [this bucket]))

(defprotocol BucketDescriptor
  (region [this])
  (versioned? [this]))

(defprotocol RegionDescriptor
  (metastore [this])
  (storage-classes [this]))

;; ring-global metastore

(def bucket-table
  "Bucket properties"
  (create-table
   :bucket
   (column-definitions {:bucket       :text
                        :created      :text
                        :tenant       :text
                        :region       :text
                        :acl          :text
                        :cors         :text
                        :website      :text
                        :policy       :text
                        :versioned    :boolean
                        :primary-key  :bucket})))

(def bucket_tenant-index
  "Index bucket on tenant"
  (create-index
   :bucket
   :tenant
   (index-name :bucket_tenant)))

(defn bucket-by-tenant-q
  "Cassandra query for bucket by tenant"
  [tenant]
  (select :bucket (where [[= :tenant tenant]])))

(defn fetch-bucket-q
  "Cassandra query for bucket by name"
  [bucket]
  (select :bucket (where [[= :bucket bucket]]) (limit 1)))

(defn update-bucket-q
  "Bucket creation or update"
  [bucket columns]
  (update :bucket
          (set-columns columns)
          (where [[= :bucket bucket]])))

(defn delete-bucket-q
  "Bucket destruction"
  [bucket]
  (delete :bucket (where [[= :bucket bucket]])))

(defn cassandra-bucket-store
  "Given a cluster configuration, reify an instance of Bucketstore.
   The cassandra bucket store suffers from a design flaw since last
   write-wins might yield a success response for a bucket which will
   later be claimed.

   This can be fixed with the following strategies:

   - writing a bucket store that targets an SQL DB instead of cassandra
   - using lightweight transactions
   - wrap ownership around a zookeeper lock

   If you care deeply about bucket ownership, I'd suggest looking into
   the above options"
  [{:keys [default-region] :as config}]
  (let [session (store/cassandra-store config)]
    (reify
      store/Convergeable
      (converge! [this]
        (execute session bucket-table)
        (execute session bucket_tenant-index))
      store/Crudable
      (create! [this tenant bucket columns]
        (if-let [[details] (seq (execute session (fetch-bucket-q bucket)))]
          (when (not= tenant (:tenant details))
            (throw (ex-info
                    "bucket already exists"
                    {:type :bucket-already-exists
                     :bucket bucket
                     :status-code 409})))
          (let [acl {:FULL_CONTROL [{:ID tenant}]}]
            (execute session
                     (update-bucket-q bucket
                                      (merge {:region default-region
                                              :created (iso8601-timestamp)
                                              :acl     (pr-str acl)}
                                             columns
                                             {:tenant tenant}))))))
      (update! [this bucket columns]
        (execute session (update-bucket-q bucket columns)))
      (delete! [this bucket]
        (if-let [info (seq (execute session (fetch-bucket-q bucket)))]
          (execute session (delete-bucket-q bucket))
          (throw (ex-info "bucket not found"
                          {:type        :no-such-bucket
                           :status-code 404
                           :bucket      bucket}))))
      Bucketstore
      (by-tenant [this tenant]
        (execute session (bucket-by-tenant-q tenant)))
      (by-name [this bucket]
        (first
         (execute session (fetch-bucket-q bucket)))))))

(defn get-region
  "Fetch the regionstore from regions"
  [system region]
  (or (get (system/regions system) region)
      (throw (ex-info (str "could not find region: " region)
                      {:status-code 500}))))

(defn bucket-descriptor
  [system bucket]
  (let [bucketstore                         (system/bucketstore system)
        details                             (by-name bucketstore bucket)]
    (if details
      (let [{:keys [versioned region bucket]}   details
            {:keys [metastore storage-classes]} (get-region system region)]
        (reify
          BucketDescriptor
          (versioned? [this] versioned)
          (region [this] region)
          RegionDescriptor
          (metastore [this] metastore)
          (storage-classes [this] storage-classes)
          clojure.lang.ILookup
          (valAt [this k]
            (get details k))
          (valAt [this k def]
            (get details k def))))
      (throw (ex-info "bucket not found"
                      {:type :no-such-bucket
                       :status-code 404
                       :bucket bucket})))))
