(ns io.pithos.desc
  "Provide easy access to descriptors for entities in pithos"
  (:require [io.pithos.bucket  :as bucket]
            [io.pithos.meta    :as meta]
            [io.pithos.system  :as system]
            [io.pithos.store   :as store]
            [io.pithos.util    :as util]
            [qbits.alia.uuid   :as uuid]
            [clojure.tools.logging :refer [debug]]))

(defprotocol BlobDescriptor
  (size [this])
  (checksum [this])
  (inode [this])
  (version [this])
  (blobstore [this]))

(defprotocol ObjectDescriptor
  (init-version [this])
  (init-size [this])
  (metadata [this] [this key] [this key def])
  (content-type [this])
  (clear! [this])
  (col! [this field val])
  (increment! [this])
  (save! [this]))

(defprotocol PartDescriptor
  (part [this]))

(defn part-descriptors
  [system bucket object upload]
  (let [bucketstore                (system/bucketstore system)
        regions                    (system/regions system)
        {:keys [region versioned]} (bucket/by-name bucketstore bucket)
        {:keys [metastore
                storage-classes]}  (bucket/get-region system region)
        meta                       (store/fetch metastore bucket object false)
        parts                      (meta/list-upload-parts metastore bucket
                                                           object upload)
        ;; XXX: should support several storage classes
        blobstore                 (get storage-classes :standard)]
    (vec
     (for [part parts]
       (reify
         bucket/BucketDescriptor
         (region [this] (get regions region))
         bucket/RegionDescriptor
         (metastore [this] metastore)
         PartDescriptor
         (part [this] (:partno part))
         BlobDescriptor
         (size [this] (:size part))
         (checksum [this] (:checksum part))
         (inode [this] (:inode part))
         (version [this] (:version part))
         (blobstore [this] blobstore))))))

(defn object-descriptor
  [system bucket object]
  (let [bucketstore               (system/bucketstore system)
        regions                   (system/regions system)
        {:keys [region tenant]}   (bucket/by-name bucketstore bucket)
        {:keys [metastore
                storage-classes]} (bucket/get-region system region)
        meta                      (or (store/fetch metastore bucket object false)
                                      ; when the object doesn't exist, inherit
                                      ; from the bucket ACL to avoid returning
                                      ; unexpected 403s
                                      {:acl (:acl (bucket/bucket-descriptor
                                                    system bucket))})
        inode                     (or (:inode meta) (uuid/random))
        version                   (or (:version meta) (uuid/time-based))
        ;; XXX: should support several storage classes
        blobstore                 (get storage-classes :standard)
        cols                      (atom {:metadata (:metadata meta)})]
    (reify
      bucket/BucketDescriptor
      (region [this] (get regions region))
      bucket/RegionDescriptor
      (metastore [this] metastore)
      BlobDescriptor
      (size [this] (or (:size @cols) (:size meta)))
      (checksum [this] (or (:checksum @cols) (:checksum meta)))
      (inode [this] (or (:inode @cols) inode))
      (version [this] (or (:version @cols) version ))
      (blobstore [this] blobstore)
      ObjectDescriptor
      (init-version [this] (:version meta))
      (init-size [this] (:size meta))
      (metadata [this] (:metadata meta))
      (metadata [this key] (metadata this key nil))
      (metadata [this key def]
        (or (get-in meta [:metadata (name key)])
            (get-in @cols [:metadata (name key)])
            def))
      (content-type [this]
        (metadata this "content-type" "application/binary"))
      (clear! [this]
        (swap! cols assoc :metadata {}))
      (col! [this field val]
        (if (#{:acl :atime :storageclass :size :checksum :inode :version} field)
          (swap! cols assoc (keyword field) val)
          (swap! cols assoc-in [:metadata (name field)] val)))
      (increment! [this]
        (col! this :version (uuid/time-based)))
      (save! [this]
        (let [ts   (util/iso8601-timestamp)
              meta (-> meta
                       (merge {:inode inode
                               :version version
                               :atime ts})
                       (merge @cols)
                       (dissoc :bucket :object))]
          (when-not (and (:inode meta)
                         (:version meta)
                         (:size meta)
                         (:checksum meta))
            (error "trying to write incomplete metadata"
                   (pr-str meta))
            (throw (ex-info "bad metadata" {:type :incomplete-metadata
                                            :status-code 500
                                            :meta (pr-str meta)})))
          (store/update! metastore bucket object meta)
          (swap! cols assoc :atime ts)))
      clojure.lang.ILookup
      (valAt [this k]
        (get (merge meta {:tenant tenant :inode inode :version version} @cols)
             k))
      (valAt [this k def]
        (get (merge meta {:tenant tenant :inode inode :version version} @cols)
             k
             def)))))


(defn part-descriptor
  [system bucket object upload-id partnumber]
  (let [bucketstore               (system/bucketstore system)
        regions                   (system/regions system)
        {:keys [region]}          (bucket/by-name bucketstore bucket)
        {:keys [metastore
                storage-classes]} (bucket/get-region system region)
        meta                      (store/fetch metastore bucket object false)
        inode                     (uuid/random)
        version                   (uuid/time-based)
        ;; XXX: should support several storage classes
        blobstore                 (get storage-classes :standard)
        cols                      (atom {})
        part                      (Long/parseLong partnumber)]
    (or
     (meta/get-upload-details metastore bucket object upload-id)
     (throw (ex-info "no such upload" {:type :no-such-upload
                                       :status-code 404
                                       :key object
                                       :upload upload-id})))
    (reify
      bucket/BucketDescriptor
      (region [this] (get regions region))
      bucket/RegionDescriptor
      (metastore [this] metastore)
      BlobDescriptor
      (size [this] (or (:size @cols) (:size meta)))
      (checksum [this] (or (:checksum @cols) (:checksum meta)))
      (inode [this] (or (:inode @cols) inode))
      (version [this] (or (:version @cols) version))
      (blobstore [this] blobstore)
      ObjectDescriptor
      (col! [this field val]
        (if (#{:size :checksum :inode :version} field)
          (swap! cols assoc (keyword field) val)
          (swap! cols assoc-in [:metadata (name field)] val)))
      (save! [this]
        (let [meta (-> {:inode inode :version version}
                       (merge @cols)
                       (merge {:modified (util/iso8601-timestamp)}))]
          (meta/update-part! metastore bucket object upload-id part meta))))))
