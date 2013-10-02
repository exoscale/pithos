(ns io.pithos.schema
  (:require [qbits.hayt            :refer :all]
            [qbits.alia            :refer [execute]]
            [clojure.tools.logging :refer [info error]]))

;; ring-global metastore
(def bucket-table
    (create-table 
     :bucket
     (column-definitions {:bucket       :text
                          :tenant       :text
                          :region       :text
                          :acl          :text
                          :cors         :text
                          :website      :text
                          :policy       :text
                          :primary-key  :bucket})))

(def bucket_tenant-index
    (create-index
     :bucket
     :tenant
     (index-name :bucket_tenant)))

;; region metastore

(def object-table
 (create-table
  :object
  (column-definitions {:bucket      :text
                       :object      :text
                       :inode       :uuid
                       :acl          :text
                       :primary-key [:bucket :object]})))

(def inode-table
 (create-table
  :inode
  (column-definitions {:inode        :uuid
                       :draft        :boolean
                       :version      :timeuuid
                       :atime        :timestamp
                       :checksum     :text
                       :multi        :boolean
                       :storageclass :text
                       :metadata     (coll-type :map :text :text)
                       :primary-key  [[:inode :draft] :version]})))

(def upload-table
 (create-table
  :upload
  (column-definitions {:upload      :uuid
                       :bucket      :text
                       :object      :text
                       :inode       :uuid
                       :size        :bigint
                       :sendsum     :text
                       :recvsum     :text
                       :primary-key [[:bucket :object :upload] :inode]})))

(def object_uploads-table
 (create-table
  :object_uploads
  (column-definitions {:bucket      :text
                       :object      :text
                       :upload      :uuid
                       :primary-key [[:bucket :object] :upload]})))

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

(def metastore-schema
  [bucket-table bucket_tenant-index])

(def region-metastore-schema
  [object-table inode-table upload-table object_uploads-table])

(def region-blobstore-schema
  [inode_blocks-table block-table])

(defn converge-schema
  [{:keys [metastore regions] :as config}]
  (info "converging all schemas...")
  (info "converging metastore schema")
  (try
    (doseq [schema metastore-schema]
      (execute metastore schema))

    (doseq [[region {:keys [metastore storage-classes]}] regions]
      (info "converging metastore for region " region)
      (doseq [schema region-metastore-schema]
        (execute metastore schema))
      (doseq [[storage-class blobstore] storage-classes]
        (info "converging blobstore for region and storage-class "
              region storage-class)
        (doseq [schema region-blobstore-schema]
          (execute blobstore schema))))
    
    (catch Exception e
      (error e "cannot create schema")))
  (System/exit 0))
