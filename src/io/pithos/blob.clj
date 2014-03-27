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
            [qbits.alia.uuid       :as uuid]
            [aleph.formats         :as formats]
            [lamina.core           :refer [channel? map* on-drained receive-in-order]]
            [qbits.alia            :refer [execute with-session]]
            [qbits.hayt            :refer [select where columns order-by
                                           insert values limit delete
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
  (append-stream! [this inode version stream finalize!])
  (write-chunk! [this ino version hash buf block offset])
  (stream-block! [this ino version block handler])
  (stream! [this inode version handler])
  (delete! [this inode version]))


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
          (where {:inode inode :version version})
          (order-by [:block order])))

(defn set-block-q
  "Add a block to an inode."
  [inode version block size]
  (insert :inode_blocks
          (values {:inode inode :version version
                   :block block :size size})))

(defn last-chunk-q
  "Fetch the last chunk in a block."
  [inode version block]
  (select :block
          (where {:inode inode :version version :block block})
          (order-by [:offset :desc])
          (limit 1)))

(defn get-chunk-q
  "Fetch a specific chunk in a block."
  ([inode version block offset]
     (select :block
             (where {:inode inode
                     :version version
                     :block block
                     :offset [:>= offset]})
             (order-by [:offset :asc])))
  ([inode version block offset max]
     (select :block
             (where {:inode inode
                     :version version
                     :block block
                     :offset [:>= offset]})
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
  (delete :inode_blocks (where {:inode inode :version version})))

(defn delete-block-q
  "Delete a specific inode block."
  [inode version block]
  (delete :block (where {:inode inode :version version :block block})))

;; Data manipulation

(defn get-chunk
  "Convert a channel buffer to a byte-buffer, update md5sum with contents
   in the process."
  [max cb hash]

  (when (.readable cb)
    (let [btor (min (.readableBytes cb) max)
          bb   (doto (ByteBuffer/allocate btor) (.position 0))]
      (.readBytes cb bb)
      (md5-update hash (.array bb) 0 btor)
      (.position bb 0))))

(defn last-chunk
  "Fetch a block's last chunk"
  [session inode version block]
  (first (execute session (last-chunk-q inode version block))))

(defn put-chunk!
  "Insert a chunk in a block."
  [chunk session inode version block offset]
  (let [size (.limit chunk)]
    (execute session (set-chunk-q inode version block offset size chunk))
    size))

(defn last-block
  "Fetch last block from an inode"
  [session inode version]
  (first
   (execute session
    (get-block-q inode version :desc))))

(defn set-block!
  "Register a new block with a specific size"
  [session inode version block size]
  (execute session
   (set-block-q inode version block size)))


(defn cassandra-blob-store
  "cassandra-blob-store, given a maximum chunk size and maximum
   number of chunks per block and cluster configuration details,
   will create a cassandra session and reify a Blobstore instance
   "
  [{:keys [max-chunk max-block-chunks] :as config}]
  (let [session   (store/cassandra-store config)
        bs        (* max-chunk max-block-chunks)
        limit     100]
    (reify Blobstore

      (converge! [this]

        ;;
        ;; execute creation querie
        (with-session session
          (execute inode_blocks-table)
          (execute block-table)))


      ;;
      ;; The inclusion of stream-block! in the protocol is debatable
      ;; there could be an external yield-block-streamer closure
      ;;
      ;; stream-block! loops for each chunk in a block and calls
      ;; a local stream-chunks! function which calls a supplied
      ;; handler on chunks and yields the position of the next chunk
      ;;
      ;; chunks are fetched in batches of 100 at a time

      (stream-block! [this ino version block handler]
        (let [stream-chunks!
              (fn [offset]
                (when-let [chunks (seq
                                   (execute
                                    (get-chunk-q ino version block offset limit)))]
                  (handler chunks)
                  (last chunks)))]
          (try
            (loop [offset block]
              (when-let [{:keys [offset chunksize]} (stream-chunks! offset)]
                (recur (+ offset chunksize))))
            (catch Exception e
              (error e "something went wront during recur loop"))
            )))


      ;;
      ;; Successively call stream-block! on all blocks, once all blocks
      ;; have been gone through, call the handler with a nil argument to
      ;; indicate EOF
      ;;

      (stream! [this ino version handler]
        (with-session session
          (let [blocks (execute (get-block-q ino version :asc))]
            (doseq [{:keys [block]} blocks]
              (stream-block! this ino version block handler)))
          (handler nil)))


      ;;
      ;; Delete an inode.
      ;; Rather straightforward, deletes all blocks then all inodes_blocks
      ;;

      (delete! [this ino version]
        (with-session session
          (doseq [{block :block} (execute (get-block-q ino version :asc))]
            (execute (delete-block-q ino version block)))
          (execute (delete-blockref-q ino version))))

      ;; Writing to inodes is split in two functions:
      ;;
      ;; append-stream! expects an inode and version, a lamina channel
      ;; containing data and a function to be called once the channel's
      ;; data has been successfuly written out.
      ;;
      ;; write-chunk! does the actual writing out.
      ;;
      ;; The workflow might be a bit confusing, so here's a bit of a walk-through:
      ;;
      ;; - if the input stream is not a channel, just write out to chunks the
      ;;   input payload by calling write chunks
      ;;
      ;; - write-chunks! expects an inode, version, a md5-hash instance, a block
      ;;   and offset and the actual data buffer to write out:
      ;;
      ;;   - convert the input buffer to data ingestible by cassandra and update md5
      ;;   - write out the data
      ;;   - when on a block boundary (next offset is larger than block size) write out block
      ;;
      ;; - append-stream! writes chunks as they come in on a channel and call
      ;;   the finalizing function when all chunks have been written

      (write-chunk! [this ino version hash buf block offset]
        (loop [block  block
               offset offset]

          (if-let [sz (some-> (get-chunk max-chunk buf hash)
                              (put-chunk! session ino version block offset))]

            (let [offset (+ sz offset)]
              (if (>= offset (+ block bs))
                (do
                  ;; we are on a block boundary, write it out
                  (set-block! session ino version block offset)
                  (recur offset offset))
                (recur block offset)))

            (do (set-block! session ino version block offset)
                [block offset]))))

      (append-stream! [this ino version stream tell!]
        (let [hash (md5-init)
              f!   #(when tell! (tell! ino version % (md5-sum hash)))]

          (if (channel? stream)
            (let [pos (atom [0 0])
                  block  (atom 0)]
              (on-drained stream (comp f! second (partial deref pos)))
              (receive-in-order
               stream
               (fn [buf]
                 (let [res (apply write-chunk! this ino version hash buf @pos)]
                   (reset! pos res)
                   res))))

            (-> (write-chunk! this ino version hash stream 0 0) second f!)))))))
