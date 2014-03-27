(ns io.pithos.schema
  "Namespace holding a single action which installs the schema"
  (:require [qbits.hayt            :refer :all]
            [qbits.alia            :refer [execute]]
            [clojure.tools.logging :refer [info error]]
            [io.pithos.bucket      :as bucket]
            [io.pithos.meta        :as meta]
            [io.pithos.blob        :as blob]))

(defn converge-schema
  "Loops through all storage layers and calls converge! on them"
  [{:keys [bucketstore regions] :as config}]
  (info "converging all schemas...")
  (try
    (info "converging bucketstore schema")
    (bucket/converge! bucketstore)

    (doseq [[region {:keys [metastore storage-classes]}] regions]
      (info "converging metastore for region " region)
      (meta/converge! metastore)

      (doseq [[storage-class blobstore] storage-classes]
        (info "converging blobstore for region and storage-class "
              region storage-class)
        (blob/converge! blobstore)))

    (catch Exception e
      (error e "cannot create schema")))
  (System/exit 0))
