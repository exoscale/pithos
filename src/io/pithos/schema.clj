(ns io.pithos.schema
  "Namespace holding a single action which installs the schema"
  (:require [clojure.tools.logging :refer [info error]]
            [io.pithos.system      :as system]
            [io.pithos.store       :as store]))

(defn converge-schema
  "Loops through all storage layers and calls converge! on them"
  ([system exit?]
     (info "converging all schemas...")
     (try
       (info "converging bucketstore schema")
       (store/converge! (system/bucketstore system))

       (doseq [region (system/regions system)
               :let [[region {:keys [metastore storage-classes]}] region]]
         (info "converging metastore for region " region)
         (store/converge! metastore)

         (doseq [[storage-class blobstore] storage-classes]
           (info "converging blobstore for region and storage-class "
                 region storage-class)
           (store/converge! blobstore))
         (when exit? (System/exit 0)))
       (catch Exception e
          (error e "cannot create schema")
          (when exit? (System/exit 1)))))
  ([system]
     (converge-schema system true)))
