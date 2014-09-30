(ns io.pithos.schema
  "Namespace holding a single action which installs the schema"
  (:require [clojure.tools.logging :refer [info error]]
            [io.pithos.system      :as system]
            [io.pithos.bucket      :as bucket]
            [io.pithos.meta        :as meta]
            [io.pithos.blob        :as blob]))

(defn converge-schema
  "Loops through all storage layers and calls converge! on them"
  ([system exit?]
     (info "converging all schemas...")
     (try
       (info "converging bucketstore schema")
       (bucket/converge! (system/bucketstore system))

       (doseq [region (system/regions system)
               :let [[region {:keys [metastore storage-classes]}] region]]
         (info "converging metastore for region " region)
         (meta/converge! metastore)

         (doseq [[storage-class blobstore] storage-classes]
           (info "converging blobstore for region and storage-class "
                 region storage-class)
           (blob/converge! blobstore)))

       (catch Exception e
         (error e "cannot create schema"))
       (finally
         (when exit? (System/exit 0)))))
  ([system]
     (converge-schema system true)))
