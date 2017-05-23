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
            [qbits.alia            :as a]
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

(def absolute-chunk-limit
  "max block per chunk can be exceeded when small chunks are uploaded.
  set a large limit of chunks to retrieve from a block."
  524288)


(defprotocol Blobstore
  "The blobstore protocol, provides methods to read and write data
   to inodes, as well as a schema migration function.
   "
  (converge! [this])
  (delete! [this inode version])
  (blocks [this od])
  (max-chunk [this])
  (chunks [this od block offset])
  (start-block! [this od block])
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
                       :primary-key [[:inode :version] :block]})))

(def block-table
  "A block is keyed by inode version and first offset in the block.

   Blocks contain a list of offset, chunksize and payload (a byte-buffer)
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
  [inode version block]
  (insert :inode_blocks
          (values {:inode inode :version version :block block})))

(defn get-chunk-q
  "Fetch a specific chunk in a block."
  [inode version block offset max]
  (select :block
          (where [[= :inode inode]
                  [= :version version]
                  [= :block block]
                  [>= :offset offset]])
          (limit max)))

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

(defn cassandra-blob-store
  "cassandra-blob-store, given a maximum chunk size and maximum
   number of chunks per block and cluster configuration details,
   will create a cassandra session and reify a Blobstore instance
   "
  [{:keys [max-chunk max-block-chunks read-consistency write-consistency]
    :as   config}]
  (let [copts   (dissoc config :read-consistency :write-consistency)
        session (store/cassandra-store copts)
        rdcty   (or (some-> read-consistency keyword) :one)
        wrcty   (or (some-> write-consistency keyword) :quorum)
        read!   (fn [query] (a/execute session query {:consistency rdcty}))
        write!  (fn [query] (a/execute session query {:consistency wrcty}))
        bs      (* max-chunk max-block-chunks)
        limit     100]
    (debug "got max-chunk " max-chunk "and max-block-chunks " max-block-chunks)
    (reify
      store/Convergeable
      (converge! [this]
        (write! inode_blocks-table)
        (write! block-table))
      store/Crudable
      (delete! [this od version]
        (let [ino (if (= (class od) java.util.UUID) od (d/inode od))]
          (doseq [{block :block} (read! (get-block-q ino version :asc))]
            (write! (delete-block-q ino version block))
            (write! (delete-blockref-q ino version)))))
      Blobstore
      (blocks [this od]
        (let [ino (d/inode od)
              ver (d/version od)]
          (read! (get-block-q ino ver :asc))))

      (max-chunk [this]
        max-chunk)

      (chunks [this od block offset]
        (let [ino (d/inode od)
              ver (d/version od)]
          (seq (read! (get-chunk-q ino ver block offset
                                   absolute-chunk-limit)))))

      (boundary? [this block offset]
        (>= offset (+ block bs)))

      (start-block! [this od block]
        (write! (set-block-q (d/inode od) (d/version od) block)))

      (chunk! [this od block offset chunk]
        (let [size (- (.limit chunk) (.position chunk))]
          (write! (set-chunk-q (d/inode od) (d/version od)
                               block offset size chunk))
          size)))))
