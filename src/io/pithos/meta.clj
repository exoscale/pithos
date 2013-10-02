(ns io.pithos.meta
  (:require [qbits.alia      :refer [execute]]
            [qbits.hayt      :refer [select where set-columns columns
                                     delete update limit order-by coll-type
                                     create-table column-definitions]]
            [io.pithos.util  :refer [inc-prefix]]
            [io.pithos.store :as store]))

(defprotocol Metastore
  (converge! [this])
  (fetch [this bucket object])
  (prefixes [this bucket params])
  (finalize! [this inode version size checksum])
  (update! [this bucket object columns])
  (delete! [this bucket object]))

;; schema definition

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
                       :version      :timeuuid
                       :atime        :timestamp
                       :checksum     :text
                       :size         :bigint
                       :multi        :boolean
                       :storageclass :text
                       :metadata     (coll-type :map :text :text)
                       :primary-key  [:inode :version]})))

(def upload-table
 (create-table
  :upload
  (column-definitions {:upload      :uuid
                       :bucket      :text
                       :object      :text
                       :inode       :uuid
                       :size        :bigint
                       :checksum    :text
                       :primary-key [[:bucket :object :upload] :inode]})))

(def object_uploads-table
 (create-table
  :object_uploads
  (column-definitions {:bucket      :text
                       :object      :text
                       :upload      :uuid
                       :primary-key [[:bucket :object] :upload]})))


;; CQL Queries

(defn fetch-object-q
  [bucket prefix]
  (let [object-def    [[:bucket bucket]]
        next-prefix (inc-prefix prefix)
        prefix-def  [[:object [:>= prefix]] [:object [:< next-prefix]]]]
    (select :object (where (cond-> object-def
                                 (seq prefix) (concat prefix-def))))))

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
  (let [pat (re-pattern (str "^" prefix "[^\\" delimiter "]+$"))]
    (filter (comp (partial re-find pat) :object) objects)))

(defn filter-prefixes
  [objects prefix delimiter]
  (let [pat (re-pattern
             (str "^(" prefix "[^\\" delimiter "]+\\" delimiter ").*$"))]
    (->> (map (comp second (partial re-find pat) :object) objects)
         (remove nil?)
         (set))))

(defn cassandra-meta-store
  [config]
  (let [session (store/cassandra-store config)]
    (reify Metastore
      (converge! [this]
        (execute session object-table)
        (execute session inode-table)
        (execute session upload-table)
        (execute session object_uploads-table))
      (fetch [this bucket object]
        (first (execute session (get-object-q bucket object))))
      (prefixes [this bucket {:keys [prefix delimiter max-keys]}]
        (let [objects  (execute session (fetch-object-q bucket prefix))
              prefixes (if delimiter
                         (filter-prefixes objects prefix delimiter)
                         #{})
              contents (if delimiter
                         (filter-content objects prefix delimiter)
                         objects)]
         [(remove prefixes contents) prefixes]))
      (update! [this bucket object columns]
        (execute session (update-object-q bucket object columns)))
      (delete! [this bucket object]
        (execute session (delete-object-q bucket object))))))
