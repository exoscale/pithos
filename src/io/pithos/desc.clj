(ns io.pithos.desc
  "Provide easy access to descriptors for entities in pithos"
  (:require [io.pithos.bucket  :as bucket]
            [io.pithos.meta    :as meta]
            [io.pithos.system  :as system]
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
  (metadata [this] [this key] [this key def])
  (content-type [this])
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
        meta                       (meta/fetch metastore bucket object false)
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
         (storage-classes [this] storage-classes)
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
        {:keys [region]}          (bucket/by-name bucketstore bucket)
        {:keys [metastore
                storage-classes]} (bucket/get-region system region)
        meta                      (meta/fetch metastore bucket object false)
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
      (storage-classes [this] storage-classes)
      BlobDescriptor
      (size [this] (or (:size @cols) (:size meta)))
      (checksum [this] (or (:checksum @cols) (:checksum meta)))
      (inode [this] (or (:inode @cols) inode))
      (version [this] (or (:version @cols) version ))
      (blobstore [this] blobstore)
      ObjectDescriptor
      (init-version [this] (:version meta))
      (metadata [this] (:metadata meta))
      (metadata [this key] (metadata this key nil))
      (metadata [this key def]
        (or (get-in meta [:metadata (name key)])
            (get-in @cols [:metadata (name key)])
            def))
      (content-type [this]
        (metadata this "content-type" "application/binary"))
      (col! [this field val]
        (if (#{:acl :atime :storageclass :size :checksum :inode :version} field)
          (swap! cols assoc (keyword field) val)
          (swap! cols assoc-in [:metadata (name field)] val)))
      (increment! [this]
        (col! this :version (uuid/time-based)))
      (save! [this]
        (let [meta (-> meta
                       (merge {:inode inode
                               :version version
                               :atime (util/iso8601-timestamp)})
                       (merge @cols)
                       (dissoc :bucket :object))]
          (meta/update! metastore bucket object meta)))
      clojure.lang.ILookup
      (valAt [this k]
        (get (merge meta {:inode inode :version version} @cols)
             k))
      (valAt [this k def]
        (get (merge meta {:inode inode :version version} @cols)
             k
             def)))))


(defn part-descriptor
  [system bucket object upload-id partnumber]
  (let [bucketstore               (system/bucketstore system)
        regions                   (system/regions system)
        {:keys [region]}          (bucket/by-name bucketstore bucket)
        {:keys [metastore
                storage-classes]} (bucket/get-region system region)
        meta                      (meta/fetch metastore bucket object false)
        inode                     (or (:inode meta) (uuid/random))
        version                   (or (:version meta) (uuid/time-based))
        ;; XXX: should support several storage classes
        blobstore                 (get storage-classes :standard)
        cols                      (atom {})
        part                      (Long/parseLong partnumber)]
    (reify
      bucket/BucketDescriptor
      (region [this] (get regions region))
      bucket/RegionDescriptor
      (metastore [this] metastore)
      (storage-classes [this] storage-classes)
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
