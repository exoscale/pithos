(ns io.pithos.schema
  (:require [qbits.hayt            :refer :all]
            [qbits.alia            :refer [execute]]
            [clojure.tools.logging :refer [info error]]
            [io.pithos.bucket      :as bucket]
            [io.pithos.meta        :as meta]))


;; region blobstore

(def inode_blocks-table
 (create-table
  :inode_blocks
  (column-definitions {:inode       :uuid
                       :version     :timeuuid
                       :block       :bigint
                       :primary-key [[:inode :version] :block]})))

(def block-table
 (create-table
  :block
  (column-definitions {:inode       :uuid
                       :version     :timeuuid
                       :block       :bigint
                       :offset      :bigint
                       :chunksize   :int
                       :payload     :blob
                       :primary-key [[:inode :version :block] :offset]})))

(def region-blobstore-schema
  [inode_blocks-table block-table])

(defn converge-schema
  [{:keys [bucketstore regions] :as config}]
  (info "converging all schemas...")
  (info "converging metastore schema")
  (try
    (bucket/converge! bucketstore)

    (doseq [[region {:keys [metastore storage-classes]}] regions]
      (info "converging metastore for region " region)

      (meta/converge! metastore)

      (doseq [[storage-class blobstore] storage-classes]
        (info "converging blobstore for region and storage-class "
              region storage-class)

        (doseq [schema region-blobstore-schema]
          (execute blobstore schema))))
    
    (catch Exception e
      (error e "cannot create schema")))
  (System/exit 0))
