(ns io.pithos.meta
  (:require [qbits.alia      :refer [execute]]
            [qbits.hayt      :refer [select where set-columns columns
                                     delete update limit order-by coll-type
                                     create-table column-definitions]]
            [io.pithos.util  :refer [inc-prefix]]
            [io.pithos.store :as store]))

(defprotocol MetaStore
  (converge! [this])
  (fetch [this bucket object])
  (prefixes [this bucket params])
  (update! [this bucket object columns])
  (delete! [this bucket object])
  (published? [this inode]))

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


;; queries

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

(defn published-versions-q
  [inode]
  (select :inode
          (columns :version)
          (where {:inode inode
                  :published true})
          (order-by [:version :desc])
          (limit 1)))

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
    (reify MetaStore
      (converge! [this]
        (execute session object-table)
        (execute session inode-table)
        (execute session upload-table)
        (execute session object_uploads-table))
      (fetch [this bucket object]
        (first (execute session (get-object-q bucket object))))
      (prefixes [this bucket {:keys [prefix delimiter max-keys hidden]}]
        (let [raw      (execute session (fetch-object-q bucket prefix))
              objects  (if hidden raw (filter (partial published? this) raw))
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
        (execute session (delete-object-q bucket object)))
      (published? [this inode]
        (seq (execute session (published-versions-q inode)))))))
