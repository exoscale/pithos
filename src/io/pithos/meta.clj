(ns io.pithos.meta
  (:require [qbits.alia      :refer [execute]]
            [qbits.hayt      :refer [select where set-columns columns
                                     delete update limit order-by coll-type
                                     create-table column-definitions
                                     create-index index-name]]
            [io.pithos.util  :refer [inc-prefix]]
            [io.pithos.store :as store]))

(defprotocol Metastore
  (converge! [this])
  (fetch [this bucket object] [this bucket object fail?])
  (prefixes [this bucket params])
  (finalize! [this bucket key version size checksum])
  (update! [this bucket object columns])
  (delete! [this bucket object])
  (abort-multipart-upload! [this bucket object upload])
  (update-part! [this bucket object upload partno columns])
  (initiate-upload! [this bucket object upload metadata])
  (list-uploads [this bucket])
  (list-object-uploads [this bucket object])
  (list-upload-parts [this bucket object upload]))

;; schema definition

(def object-table
 (create-table
  :object
  (column-definitions {:bucket       :text
                       :object       :text
                       :inode        :uuid
                       :version      :timeuuid
                       :atime        :text
                       :size         :bigint
                       :checksum     :text
                       :storageclass :text
                       :acl          :text
                       :metadata     (coll-type :map :text :text)
                       :primary-key  [:bucket :object]})))

(def object_inode-index
  (create-index
   :object
   :inode
   (index-name :object_inode)))

(def upload-table
 (create-table
  :upload
  (column-definitions {:upload      :uuid
                       :version     :uuid
                       :bucket      :text
                       :object      :text
                       :inode       :uuid
                       :partno      :int
                       :modified    :text
                       :size        :bigint
                       :checksum    :text
                       :primary-key [[:bucket :object :upload] :partno]})))

(def upload_bucket-index
  (create-index
   :upload
   :bucket
   (index-name :upload_bucket)))

(def object_uploads-table
 (create-table
  :object_uploads
  (column-definitions {:bucket      :text
                       :object      :text
                       :upload      :uuid
                       :metadata    (coll-type :map :text :text)
                       :primary-key [[:bucket :object] :upload]})))


;; CQL Queries

(defn abort-multipart-upload-q
  [bucket object upload]
  (delete :object_uploads (where {:bucket bucket
                                  :object object
                                  :upload upload})))

(defn delete-upload-parts-q
  [bucket object upload]
  (delete :upload (where {:bucket bucket
                          :object object
                          :upload upload})))

(defn initiate-upload-q
  [bucket object upload metadata]
  (update :object_uploads
          (set-columns {:metadata metadata})
          (where {:bucket bucket
                  :object object
                  :upload upload})))

(defn update-part-q
  [bucket object upload partno columns]
  (update :upload
          (set-columns columns)
          (where {:bucket bucket
                  :object object
                  :upload upload
                  :partno partno})))

(defn list-uploads-q
  [bucket]
  (select :upload (where {:bucket bucket})))

(defn list-upload-parts-q
  [bucket object upload]
  (select :upload (where {:bucket bucket :object object :upload upload})))

(defn list-object-uploads-q
  [bucket object]
  (select :object_uploads (where {:bucket bucket :object object})))

(defn fetch-object-q
  [bucket prefix]
  (let [object-def    [[:bucket bucket]]
        next-prefix (inc-prefix prefix)
        prefix-def  [[:object [:>= prefix]] [:object [:< next-prefix]]]]
    (select :object (where (cond-> object-def
                                 (seq prefix) (concat prefix-def))))))

(defn fetch-object-inodes-q
  [bucket object version]
  (select :object_inodes (where {:bucket  bucket
                                 :object  object
                                 :version version})))

(defn get-object-q
  [bucket object]
  (select :object
          (where {:bucket bucket :object object})
          (limit 1)))

(defn update-object-q
  [bucket object columns]
  (update :object
          (set-columns columns)
          (where {:bucket bucket :object object})))

(defn delete-object-q
  [bucket object]
  (delete :object (where {:bucket bucket :object object})))

;; utility functions

(defn filter-content
  [objects prefix delimiter]
  (let [pat (re-pattern (str "^" prefix "[^\\" delimiter "]*$"))]
    (filter (comp (partial re-find pat) :object) objects)))

(defn filter-prefixes
  [objects prefix delimiter]
  (let [pat (re-pattern
             (str "^(" prefix "[^\\" delimiter "]*\\" delimiter ").*$"))]
    (->> (map (comp second (partial re-find pat) :object) objects)
         (remove nil?)
         (set))))

(defn cassandra-meta-store
  [config]
  (let [session (store/cassandra-store config)]
    (reify Metastore
      (converge! [this]
        (execute session object-table)
        (execute session upload-table)
        (execute session object_uploads-table)
        (execute session object_inode-index)
        (execute session upload_bucket-index))
      (fetch [this bucket object fail?]
        (or
         (first (execute session (get-object-q bucket object)))
         (when fail?
           (throw (ex-info "no such key" {:type :no-such-key
                                          :status-code 404
                                          :key object})))))
      (fetch [this bucket object]
        (fetch this bucket object true))
      (prefixes [this bucket {:keys [prefix delimiter max-keys]}]
        (let [objects  (execute session (fetch-object-q bucket prefix))
              prefixes (if delimiter
                         (filter-prefixes objects prefix delimiter)
                         #{})
              contents (if delimiter
                         (filter-content objects prefix delimiter)
                         objects)]
         [(remove prefixes contents) prefixes]))
      (initiate-upload! [this bucket object upload metadata]
        (execute session (initiate-upload-q bucket object upload metadata)))
      (abort-multipart-upload! [this bucket object upload]
        (execute session (abort-multipart-upload-q bucket object upload))
        (execute session (delete-upload-parts-q bucket object upload)))
      (update-part! [this bucket object upload partno columns]
        (execute session (update-part-q bucket object upload partno columns)))
      (list-uploads [this bucket]
        (execute session (list-uploads-q bucket)))
      (list-object-uploads [this bucket object]
        (execute session (list-object-uploads-q bucket object)))
      (list-upload-parts [this bucket object upload]
        (execute session (list-upload-parts-q bucket object upload)))
      (update! [this bucket object columns]
        (execute session (update-object-q bucket object columns)))
      (delete! [this bucket object]
        (execute session (delete-object-q bucket object))))))
