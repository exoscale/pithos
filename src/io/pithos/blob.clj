(ns io.pithos.blob
  (:import java.util.UUID
           java.nio.ByteBuffer)
  (:require [clojure.java.io       :as io]
            [io.pithos.store       :as store]
            [qbits.alia.uuid       :as uuid]
            [aleph.formats         :as formats]
            [lamina.core           :refer [channel? map* on-drained receive-all]]
            [qbits.alia            :refer [execute with-session]]
            [qbits.hayt            :refer [select where columns order-by
                                           insert values limit delete
                                           create-table column-definitions]]
            [io.pithos.util        :refer [md5-safe md5-update md5-sum md5-init]]
            [clojure.tools.logging :refer [debug info error]]))


(defprotocol Blobstore
  (converge! [this])
  (lazy-stream! [this inode version])
  (append-stream! [this inode version stream finalize!])
  (write-chunk! [this ino version hash buf block offset])
  (stream-block! [this ino version block handler])
  (stream! [this inode version handler])
  (delete! [this inode version]))


;; CQL Schema
(def inode_blocks-table
 (create-table
  :inode_blocks
  (column-definitions {:inode       :uuid
                       :version     :timeuuid
                       :block       :bigint
                       :size        :bigint
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
  [inode version block size]
  (insert :inode_blocks
          (values {:inode inode :version version
                   :block block :size size})))

(defn last-chunk-q
  [inode version block]
  (select :block
          (where {:inode inode :version version :block block})
          (order-by [:offset :desc])
          (limit 1)))

(defn get-chunk-q
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
  [max cb hash]
  (debug "in getchunk!")
  (when (.readable cb)
    (let [btor (min (.readableBytes cb) max)
          bb   (doto (ByteBuffer/allocate btor) (.position 0))]
      (.readBytes cb bb)
      (md5-update hash (.array bb) 0 btor)
      (.position bb 0))))

(defn last-chunk
  [session inode version block]
  (first (execute session (last-chunk-q inode version block))))

(defn put-chunk!
  [chunk session inode version block offset]
  (debug "putting chunk at offset " offset)
  (let [size (.limit chunk)]
    (execute session (set-chunk-q inode version block offset size chunk))
    size))

(defn last-block
  [session inode version]
  (first
   (execute session
    (get-block-q inode version :desc))))

(defn set-block!
  [session inode version block size]
  (execute session
   (set-block-q inode version block size)))

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

(defn lazy-block
  [s ino ver lim block offset]
  (lazy-seq
   (when-let [chunks (seq (execute s (get-chunk-q ino ver block offset lim)))]
     (debug "lazy-seq iter, block " block ", got " (count chunks) " chunks")
     (let [{:keys [chunksize offset]} (last chunks)]
       (concat chunks (lazy-block s ino ver lim block (+ offset chunksize)))))))


(defn cassandra-blob-store
  [{:keys [max-chunk max-block-chunks] :as config}]
  (let [session   (store/cassandra-store config)
        bs        (* max-chunk max-block-chunks)
        limit     100]
    (reify Blobstore

      (converge! [this]
        (with-session session
          (execute inode_blocks-table)
          (execute block-table)))


      (lazy-stream! [this ino version]
        (mapcat (fn [{:keys [block]}]
                  (lazy-block session ino version limit block block))
                (execute session (get-block-q ino version :asc))))


      (stream-block! [this ino version block handler]
        (let [stream-chunks! 
              (fn [offset]
                (when-let [chunks (seq 
                                   (execute 
                                    (get-chunk-q ino version block offset limit)))]
                  (debug "first pass: " (count chunks) " chunks")
                  (handler chunks)
                  (last chunks)))]
          (debug "streaming block: " ino version block)
          (try
            (loop [offset block]
              (when-let [{:keys [offset chunksize]} (stream-chunks! offset)]
                (debug "will try getting more chunks, starting at: " (+ offset chunksize))
                (recur (+ offset chunksize))))
            (catch Exception e
              (error e "soemthing went wront during recur loop"))
            )))


      (stream! [this ino version handler]
        (with-session session
          (doseq [{:keys [block]} (execute (get-block-q ino version :asc))]
            (debug "sending out chunks for block " block)
            (stream-block! this ino version block handler))
          (handler nil)))


      (delete! [this ino version]
        (with-session session
          (doseq [{block :block} (execute (get-block-q ino version :asc))]
            (execute (delete-block-q ino version block)))
          (execute (delete-blockref-q ino version))))


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
              (receive-all
               stream
               (fn [buf] 
                 (let [res (apply write-chunk! this ino version hash buf @pos)]
                   (debug "wrote chunk and got back: " res)
                   (reset! pos res)
                   res))))
            
            (-> (write-chunk! this ino version hash stream 0 0) second f!)))))))
