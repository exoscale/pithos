(ns io.pithos.blob
  "Blobstore interaction. This is one of the four storage protocols.
   Storage protocols are split even though they mostly target cassandra
   because it allows:

   - Easy implementation of the protocol targetting different DBs
   - Splitting data in different keyspace with different replication props

   Implementations may be swapped in the configuration file, as described
   in the documentation for the `io.pithos.config' namespace.

   The Blobstore is the storage layer concerned with actually storing data.
   Its operations are purely commutative and never deal with filenames, since
   that responsibility lies within the _Metastore_ (see `io.pithos.meta`).

   The storage layout is rather simple:

   - Data is stored in inodes
   - An inode has a list of blocks
   - Blocks contain a list of chunks

   The maximum size of chunks in blocks and the payload size in chunks
   are configurable. This approach allows storage of large files spread
   accross many rows.

   To ensure metadata operations are decoupled from storage, the protocol
   relies on callbacks in a few places.

"
  (:import java.util.UUID
           java.nio.ByteBuffer)
  (:require [clojure.java.io       :as io]
            [io.pithos.store       :as store]
            [io.pithos.desc        :as d]
            [qbits.alia.uuid       :as uuid]
            [qbits.alia            :refer [execute]]
            [qbits.hayt            :refer [select where columns order-by
                                           insert values limit delete count*
                                           create-table column-definitions]]
            [io.pithos.util        :refer [md5-update md5-sum md5-init]]
            [clojure.tools.logging :refer [debug info error]]))

;;
;; A word on storage protocols
;; ---------------------------
;;
;; All storage protocols expose functions to produce side-effects
;; and a `converge!` function whose role is to apply the schema


(defprotocol Blobstore
  "The blobstore protocol, provides methods to read and write data
   to inodes, as well as a schema migration function.
   "
  (converge! [this])
  (delete! [this inode version])
  (blocks [this od])
  (max-chunk [this])
  (chunks [this od block offset])
  (start-block! [this od block offset])
  (chunk! [this od block offset chunk])
  (boundary? [this block offset]))

;; CQL Schema
(def inode_blocks-table
  "List of blocks found in an inode, keyed by inode and version"
 (create-table
  :inode_blocks
  (column-definitions {:inode       :uuid
                       :version     :timeuuid
                       :block       :bigint
                       :size        :bigint
                       :primary-key [[:inode :version] :block]})))

(def block-table
  "A block is keyed by inode version and first offset in the block.
   This means that the next block is always:

        last-block[block] + last-block[size]

   blocks contain a list of offset, chunksize and payload (a byte-buffer)
   which contain the actual data being stored. chunksize is set in the
   configuration."
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
  "Fetch list of blocks in an inode."
  [inode version order]
  (select :inode_blocks
          (columns :block)
          (where [[= :inode inode]
                  [= :version version]])
          (order-by [:block order])))

(defn set-block-q
  "Add a block to an inode."
  [inode version block size]
  (insert :inode_blocks
          (values {:inode inode :version version
                   :block block :size size})))

(defn get-chunk-q
  "Fetch a specific chunk in a block."
  ([inode version block offset]
     (select :block
             (where [[= :inode inode]
                     [= :version version]
                     [= :block block]
                     [>= :offset offset]])
             (order-by [:offset :asc])))
  ([inode version block offset max]
     (select :block
             (where [[= :inode inode]
                     [= :version version]
                     [= :block block]
                     [>= :offset offset]])
             (limit max))))

(defn set-chunk-q
  "Set a chunk in a block."
  [inode version block offset size chunk]
  (insert :block
          (values {:inode inode
                   :version version
                   :block block
                   :offset offset
                   :chunksize size
                   :payload chunk})))

(defn delete-blockref-q
  "Remove all blocks in an inode."
  [inode version]
  (delete :inode_blocks (where [[= :inode inode]
                                [= :version version]])))

(defn delete-block-q
  "Delete a specific inode block."
  [inode version block]
  (delete :block (where [[= :inode inode]
                         [= :version version]
                         [= :block block]])))

(defn cleanup-block-q
  [inode version block]
  (select :block (columns (count*))
          (where [[= :inode inode]
                  [= :version version]
                  [= :block block]])))

(defn cassandra-blob-store
  "cassandra-blob-store, given a maximum chunk size and maximum
   number of chunks per block and cluster configuration details,
   will create a cassandra session and reify a Blobstore instance
   "
  [{:keys [max-chunk max-block-chunks] :as config}]
  (let [session   (store/cassandra-store config)
        bs        (* max-chunk max-block-chunks)
        limit     100]
    (debug "got max-chunk " max-chunk "and max-block-chunks " max-block-chunks)
    (reify Blobstore

      (converge! [this]

        ;;
        ;; execute creation querie
        (execute session inode_blocks-table)
        (execute session block-table))


      (blocks [this od]
        (let [ino (d/inode od)
              ver (d/version od)]
          (execute session (get-block-q ino ver :asc))))

      (max-chunk [this]
        max-chunk)

      (chunks [this od block offset]
        (let [ino (d/inode od)
              ver (d/version od)]
          (seq (execute session (get-chunk-q ino ver block offset max-block-chunks)))))


      ;;
      ;; Delete an inode.
      ;; Rather straightforward, deletes all blocks then all inodes_blocks
      ;;

      (delete! [this od version]
        (let [ino (if (= (class od) java.util.UUID) od (d/inode od))]
          (doseq [{block :block} (execute session (get-block-q ino version :asc))]
            (execute session (delete-block-q ino version block))
            (execute session (cleanup-block-q ino version block)))
          (execute session (delete-blockref-q ino version))))

      (boundary? [this block offset]
        (>= offset (+ block bs)))

      (start-block! [this od block offset]
        (execute session
                 (set-block-q (d/inode od) (d/version od) block offset)))

      (chunk! [this od block offset chunk]
        (let [size (- (.limit chunk) (.position chunk))]
          (execute session (set-chunk-q (d/inode od) (d/version od)
                                        block offset size chunk))
          size)))))
