(ns io.pithos.blob
  (:import java.util.UUID
           java.nio.ByteBuffer)
  (:require [clojure.java.io       :as io]
            [io.pithos.store       :as store]
            [qbits.alia.uuid       :as uuid]
            [qbits.alia            :refer [execute with-session]]
            [qbits.hayt            :refer [select where columns order-by
                                           insert values limit delete
                                           create-table column-definitions]]
            [io.pithos.util        :refer [md5-init md5-update md5-text-sum]]
            [clojure.tools.logging :refer [debug info]]))


(defprotocol Blobstore
  (converge! [this])
  (append-stream! [this inode version stream finalize!])
  (stream! [this inode version handler])
  (delete! [this inode version]))


;; CQL Schema
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


;;
;; start declaring CQL queries

(defn get-block-q
  [inode version order]
  (select :inode_blocks
          (columns :block)
          (where {:inode inode :version version})
          (order-by [:block order])
          (limit 1)))

(defn set-block-q
  [inode version block]
  (insert :inode_blocks
          (values {:inode inode :version version :block block})))

(defn last-chunk-q
  [inode version block]
  (select :block
          (where {:inode inode :version version :block block})
          (order-by [:offset :desc])
          (limit 1)))

(defn get-chunk-q
  [inode version block offset]
  (select :block
          (where {:inode inode
                  :version version
                  :block block
                  :offset [:>= offset]})
          (order-by [:offset :asc])))

(defn set-chunk-q
  [inode version block offset size chunk]
  (insert :block
          (values {:inode inode
                   :version version
                   :block block
                   :offset offset
                   :chunksize size
                   :payload chunk})))

(defn delete-blockref-q
  [inode version]
  (delete :inode_blocks (where {:inode inode :version version})))

(defn delete-block-q
  [inode version block]
  (delete :block (where {:inode inode :version version :block block})))

;; end query 

(defn get-chunk
  [max-chunk stream hash]
  (let [bb (doto (ByteBuffer/allocate max-chunk) (.position 0))
        br (.read stream (.array bb))]
    (when-not (= br -1)
      (.limit bb br)
      (md5-update hash (.array bb) 0 br)
      (.position bb 0))))

(defn last-chunk
  [inode version block]
  (first (execute (last-chunk-q inode version block))))

(defn put-chunk!
  [chunk inode version block offset]
  (let [size (.limit chunk)]
    (execute (set-chunk-q inode version block offset size chunk))
    size))

(defn last-block
  [inode version]
  (first
   (execute
    (get-block-q inode version :desc))))

(defn set-block!
  [inode version block]
  (execute
   (set-block-q inode version block)))

(defn bump!
  [inode]
  (let [version (uuid/time-based)]
    ;;
    ;; XXX: this needs to be looked into
    ;; we very well might be able to get
    ;; by just yielding a version.
    ;;
    ;; some tricks might need to be applied in 
    ;; the GC thread
    version))

(defn cassandra-blob-store
  [{:keys [max-chunk max-block-chunks] :as config}]
  (let [session   (store/cassandra-store config)
        max-block (* max-chunk max-block-chunks)]
    (reify Blobstore
      (converge! [this]
        (with-session session
          (execute inode_blocks-table)
          (execute block-table)))
      (stream! [this ino version handler]
        (with-session session
          (doseq [{:keys [block offset]}
                  (execute (get-block-q ino version :asc))]
            (doseq [chunk (execute (get-chunk-q ino version block offset))]
              (handler chunk)))))
      (delete! [this ino version]
        (with-session session
          (doseq [{block :block} (execute (get-block-q ino version :asc))]
            (execute (delete-block-q ino version block)))
          (execute (delete-blockref-q ino version))))
      (append-stream! [this ino version stream finalize!]
        (with-session session
          (let [hash                       (md5-init)
                {:keys [block]}            (last-block ino version)
                {:keys [offset chunksize]} (last-chunk
                                            ino version (or block 0))]

            (loop [offset (+ (or offset 0) (or chunksize 0))
                   block  (or block 0)]

              ;; we're starting on a new block boundary
              ;; declare it
              (when (= offset block)
                (set-block! ino version offset))
              (debug "writing at offset " offset " for block " block)
              
              (if (>= (- offset block) max-block)

                ;; we have reached our block's maximum capacity, roll-over
                (recur offset offset)

                (if-let [size (some-> (get-chunk max-chunk stream hash)
                                      (put-chunk! ino version block offset))]

                  ;; we read data, advance to next offset
                  (recur (+ offset size) block)
                  
                  ;; no more data to read
                  ;; compute checksum and notify finalizer if applicable
                  (when finalize!
                    (finalize! ino version offset ;; here, offset is size
                               (md5-text-sum hash))))))))))))

