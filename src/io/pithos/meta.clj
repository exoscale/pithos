(ns io.pithos.meta
  "The metastore is region-local and stores details of bucket content
   (bucket contents are region-local as well)."
  (:refer-clojure :exclude [update])
  (:require [qbits.alia      :as a]
            [qbits.hayt      :refer [select where set-columns columns
                                     delete update limit map-type
                                     create-table column-definitions
                                     create-index index-name]]

            [clojure.tools.logging :refer [debug]]
            [clojure.set     :refer [union]]
            [io.pithos.util  :refer [inc-prefix string->pattern]]
            [io.pithos.store :as store]))

(defprotocol Metastore
  "All necessary functions to manipulate bucket metadata"
  (prefixes [this bucket params])
  (abort-multipart-upload! [this bucket object upload])
  (update-part! [this bucket object upload partno columns])
  (initiate-upload! [this bucket object upload metadata])
  (get-upload-details [this bucket object upload])
  (list-uploads [this bucket prefix])
  (list-object-uploads [this bucket object])
  (list-upload-parts [this bucket object upload]))

;; schema definition

(def object-table
  "Objects are keyed by bucket and object and contain
   several direct properties as well as a map of additional
   schema-less properties"
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
                       :metadata     (map-type :text :text)
                       :primary-key  [:bucket :object]})))

(def object_inode-index
  "Objects are indexed by inode"
  (create-index
   :object
   :inode
   (index-name :object_inode)))

(def upload-table
  "Uploads are keyed by bucket, object and upload since several concurrent
   uploads can be performed"
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

(def object_uploads-table
  "Uploads are also referenced by object"
 (create-table
  :object_uploads
  (column-definitions {:bucket      :text
                       :object      :text
                       :upload      :uuid
                       :metadata    (map-type :text :text)
                       :primary-key [[:bucket :object] :upload]})))

(def upload_bucket-index
  "Uploads are indexed by bucket for easy lookup"
  (create-index
   :object_uploads
   :bucket
   (index-name :upload_bucket)))

;; CQL Queries

;; Note: Possible improvements, all of these are preparable, with the
;; exception of initiate-upload-q and update-part-q (unless we freeze
;; the fields to update making them parameters). A function taking a
;; session that returns a map of prepared queries could be invoked from
;; cassandra-meta-store, destructured in a let via {:keys [...]} then
;; used with execute in that scope.

(defn abort-multipart-upload-q
  "Delete an upload reference"
  [bucket object upload]
  (delete :object_uploads (where [[= :bucket bucket]
                                  [= :object object]
                                  [= :upload upload]])))

(defn delete-upload-parts-q
  "Delete all upload parts"
  [bucket object upload]
  (delete :upload (where [[= :bucket bucket]
                          [= :object object]
                          [= :upload upload]])))

(defn initiate-upload-q
  "Create an upload reference"
  [bucket object upload metadata]
  (update :object_uploads
          (set-columns {:metadata metadata})
          (where [[= :bucket bucket]
                  [= :object object]
                  [= :upload upload]])))

(defn update-part-q
  "Update an upload part's properties"
  [bucket object upload partno columns]
  (update :upload
          (set-columns columns)
          (where [[= :bucket bucket]
                  [= :object object]
                  [= :upload upload]
                  [= :partno partno]])))

(defn list-uploads-q
  "List all uploads by bucket"
  [bucket]
  (select :object_uploads (where [[= :bucket bucket]])))

(defn list-upload-parts-q
  "List all parts of an upload"
  [bucket object upload]
  (select :upload (where [[= :bucket bucket]
                          [= :object object]
                          [= :upload upload]])))

(defn get-upload-details-q
  [bucket object upload]
  (select :object_uploads (where [[= :bucket bucket]
                                  [= :object object]
                                  [= :upload upload]])))

(defn list-object-uploads-q
  "List all uploads of an object"
  [bucket object]
  (select :object_uploads (where [[= :bucket bucket]
                                  [= :object object]])))

(defn fetch-object-q
  "List objects"
  [bucket prefix marker max init?]
  (let [object-def  [[= :bucket bucket]]
        next-prefix (when (seq prefix) (inc-prefix prefix))]
    (select :object
            (cond
              (empty? prefix)
              (where [[= :bucket bucket]
                      [> :object (or marker "")]])

              init?
              (where [[= :bucket bucket]
                      [>= :object marker]
                      [< :object next-prefix]])

              :else
              (where [[= :bucket bucket]
                      [> :object marker]
                      [< :object next-prefix]]))
            (limit max))))

(defn get-object-q
  "Fetch object properties"
  [bucket object]
  (select :object
          (where [[= :bucket bucket]
                  [= :object object]])
          (limit 1)))

(defn update-object-q
  "Update object properties"
  [bucket object columns]
  (update :object
          (set-columns columns)
          (where [[= :bucket bucket]
                  [= :object object]])))

(defn delete-object-q
  "Delete an object"
  [bucket object]
  (delete :object (where [[= :bucket bucket]
                          [= :object object]])))

;; ### Utility functions

(defn filter-keys
  "Keep only contents in a list of objects"
  [objects prefix delimiter]
  (if (seq objects)
    (let [prefix (or prefix "")
          suffix (if delimiter (str "[^\\" (string->pattern delimiter) "]") ".")
          pat    (str "^" (string->pattern prefix) suffix "*$")
          keep?  (comp (partial re-find (re-pattern pat)) :object)]
      (filter keep? objects))
    objects))

(defn filter-prefixes
  "Keep only prefixes from a list of objects"
  [objects prefix delim]
  (set
   (when (and (seq delim) (seq objects))
     (let [prefix   (or (string->pattern prefix) "")
           delim    (string->pattern delim)
           regex    (re-pattern
                     (str "^(" prefix "[^\\" delim "]*\\" delim ").*$"))
           ->prefix (comp second
                          (partial re-find regex)
                          :object)]
       (remove nil? (map ->prefix objects))))))

(defn normalize-params
  [{:keys [delimiter] :as params}]
  (if (seq delimiter)
    params
    (dissoc params :delimiter)))

(defn get-prefixes
  "Paging logic for keys"
  [fetcher {:keys [prefix delimiter max-keys marker]}]
  (loop [objects    (fetcher prefix (or marker prefix) max-keys true)
         prefixes   #{}
         keys       []]
    (let [prefixes (if delimiter
                     (union prefixes (filter-prefixes objects prefix delimiter))
                     #{})
          new-keys  (remove prefixes (filter-keys objects prefix delimiter))
          keys      (concat keys new-keys)
          found     (count (concat keys prefixes))
          next      (:object (last objects))
          trunc?    (boolean (seq next))]
      (if (or (>= found max-keys) (not trunc?))
        (-> {:keys       keys
             :prefixes   prefixes
             :truncated? trunc?}
            (cond-> (and delimiter trunc?)
                    (assoc :next-marker next
                           :marker (or marker ""))))
        (recur (fetcher prefix next max-keys false) prefixes keys)))))

(defn cassandra-meta-store
  "Given a cluster configuration, reify an instance of Metastore"
  [{:keys [read-consistency write-consistency] :as config}]
  (let [copts   (dissoc config :read-consistency :write-consistency)
        session (store/cassandra-store copts)
        rdcty   (or (some-> read-consistency keyword) :quorum)
        wrcty   (or (some-> write-consistency keyword) :quorum)
        read!   (fn [query] (a/execute session query {:consistency rdcty}))
        write!  (fn [query] (a/execute session query {:consistency wrcty}))]
    (reify
      store/Convergeable
      (converge! [this]
        (write! object-table)
        (write! upload-table)
        (write! object_uploads-table)
        (write! object_inode-index)
        (write! upload_bucket-index))
      store/Crudable
      (fetch [this bucket object fail?]
        (or
         (first (read! (get-object-q bucket object)))
         (when fail?
           (throw (ex-info "no such key" {:type :no-such-key
                                          :status-code 404
                                          :key object})))))
      (fetch [this bucket object]
        (store/fetch this bucket object true))
      (update! [this bucket object columns]
        (write! (update-object-q bucket object columns)))
      (delete! [this bucket object]
        (write! (delete-object-q bucket object)))
      Metastore
      (prefixes [this bucket params]
        (get-prefixes
         (fn [prefix marker limit init?]
           (when (and (number? limit) (pos? limit))
             (read! (fetch-object-q bucket prefix marker limit init?))))
         (normalize-params params)))
      (initiate-upload! [this bucket object upload metadata]
        (write! (initiate-upload-q bucket object upload metadata)))
      (abort-multipart-upload! [this bucket object upload]
        (write! (abort-multipart-upload-q bucket object upload))
        (write! (delete-upload-parts-q bucket object upload)))
      (update-part! [this bucket object upload partno columns]
        (write! (update-part-q bucket object upload partno columns)))
      (get-upload-details [this bucket object upload]
        (first
         (read! (get-upload-details-q bucket object upload))))
      (list-uploads [this bucket prefix]
        (filter #(.startsWith (:object %) prefix)
                (read! (list-uploads-q bucket))))
      (list-object-uploads [this bucket object]
        (read! (list-object-uploads-q bucket object)))
      (list-upload-parts [this bucket object upload]
        (read! (list-upload-parts-q bucket object upload))))))
