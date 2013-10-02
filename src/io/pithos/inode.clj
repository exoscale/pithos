(ns io.pithos.inode
  (:import java.util.UUID
           java.nio.ByteBuffer)
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :refer [debug info]]
            [qbits.alia.uuid :as uuid]
            [qbits.alia      :refer [execute]]
            [qbits.hayt      :refer [select where columns order-by
                                     insert values limit delete]]))


;;
;; XXX: these should be fetched from configuration
;;
(def maxchunk (* 512 1024))      ;; 512k
(def maxblock (* 200 maxchunk))  ;; 100mb

;;
;; start declaring CQL queries
(defn get-block-q
  [inode version]
  (select :inode_blocks
          (columns :block)
          (where {:inode inode :version version})
          (order-by [:block :desc])
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
  [inode version block offset max-chunks-per-req]
  (select :block
          (where {:inode inode
                  :version version
                  :block block
                  :offset [:>= offset]})
          (order-by [:offset :asc])
          (limit max-chunks-per-req)))

(defn set-chunk-q
  [inode version block offset size chunk]
  (insert :block
          (values {:inode inode
                   :version version
                   :block block
                   :offset offset
                   :chunksize size
                   :payload chunk})))

(defn publish-q
  [inode version size checksum]
  (insert :inode
          (values {:inode inode
                   :version version
                   :size size
                   :checksum checksum
                   :published true})))
;; end query 

(defn get-chunk
  [stream hash]
  (let [bb (doto (ByteBuffer/allocate maxchunk) (.position 0))
        br (.read stream (.array bb))]
    (when-not (= br -1)
      (.limit bb br)
      (.update hash (.array bb) 0 br)
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
    (get-block-q inode version))))

(defn set-block!
  [inode version block]
  (execute
   (set-block-q inode version block)))

(defn finalize!
  [inode version size hash]
  (let [cksum (.toString (java.math.BigInteger. 1 (.digest hash)) 16)]
    (execute (publish-q inode version size cksum))
    cksum))

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

(defn append-stream
  [inode version stream]
  (let [hash (doto (java.security.MessageDigest/getInstance "MD5") (.reset))
        {:keys [block]} (last-block inode version)
        {:keys [offset chunksize]}
        (last-chunk inode version (or block 0))]

    (loop [offset (+ (or offset 0) (or chunksize 0))
           block  (or block 0)]

      ;; we're starting on a new block boundary
      ;; declare it
      (when (= offset block)
        (set-block! inode version offset))
      (debug "writing at offset " offset " for block " block)

      (if (>= (- offset block) maxblock)

        ;; we have reached our block's maximum capacity, roll-over
        (recur offset offset)

        (if-let [size (some-> (get-chunk stream hash)
                              (put-chunk! inode version block offset))]

          ;; we read data, advance to next offset
          (recur (+ offset size) block)

          ;; no more data to read, finalize and return
          (finalize! inode version offset hash))))))
