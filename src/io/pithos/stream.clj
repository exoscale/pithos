(ns io.pithos.stream
  "Read and write to cassandra from OutputStream and InputStream"
  (:import java.io.OutputStream
           java.io.InputStream
           java.nio.ByteBuffer
           org.eclipse.jetty.server.HttpInputOverHTTP
           javax.servlet.ReadListener)
  (:require [io.pithos.blob :as b]
            [io.pithos.desc :as d]
            [io.pithos.util :as u]
            [clojure.tools.logging :refer [debug error]]))

(defn chunk->ba
  "Chunks in pithos come back as bytebuffers and we
   need byte-arrays for outputstreams, this converts
   from the former to the latter.

   The underlying bytebuffers can be reused, which is why
   we need to watch respect the position and limit parameters
   given."
  [{:keys [payload]}]
  (let [array (.array payload)
        off   (.position payload)
        len   (- (.limit payload) off)]
    [array off len]))

(defn full-file?
  "Does a range specify the full file?"
  [od start end]
  (and (= start 0) (= end (d/size od))))

(defn within-range?
  "Is a chunk within the range expected"
  [{:keys [chunksize offset]} start end]
  (and (<= start (+ offset chunksize)) (<= offset end)))

(defn crop-chunk
  "This is the land of off-by-one errors, but bear with me:
   For a specific chunk, we have three streaming cases:

   - We need to stream all of it when it starts beyond the start
     offset of the range and it ends before the end offset of the range.
   - If the start offset is contained in this chunk but beyond the
     first byte, we need to start at the correct mark.
   - If the end offset is contained in this chunk but before the
     last byte, we need to stop at the correct mark.

   Here, we treat the last two cases as a single one, by
   computing a head and tail, and adapting the start offset
   as well as the length to stream in one go."
  [{:keys [offset chunksize] :as chunk} start end]
  (let [[array off len] (chunk->ba chunk)
        buf-start       offset
        buf-end         (+ offset chunksize)]
    (if (and (<= (+ offset chunksize) end) (>= offset start))
      ;; No cropping necessary.
      ;; Just pass the byte-buffer as-is.
      [array off len]
      ;; We need to crop, compute head and tail and infer
      ;; actual length from them.
      (let [head    (if (< buf-start start) (- start buf-start) 0)
            tail    (if (> buf-end end) (- buf-end end) 0)
            croplen (- len (+ head tail))]
        [array (+ off head) croplen]))))

(defn stream-file
  "Stream a whole file. Do not handle supplied ranges and
   just write out all chunks."
  [od ^OutputStream stream blob blocks]
  (doseq [{:keys [block]} blocks]
    (doseq [chunk (b/chunks blob od block block)
            :let [[array off len] (chunk->ba chunk)]]
      (.write stream array off len))))

(defn stream-range
  "Stream a range of bytes. Keep iterating on blocks until
   we reach the end, then only consider chunks in the
   supplied range, and optionally crop them before streaming out."
  [od ^OutputStream stream blob blocks start end]
  (doseq [{:keys [block]} blocks
          :while (<= block end)]
    (doseq [chunk (b/chunks blob od block block)
            :when (within-range? chunk start end)]
      (let [[array off len] (crop-chunk chunk start end)]
        (.write stream array off len)))))

(defn stream-to
  "Stream a range or a whole file."
  [od ^OutputStream stream [start end]]
  (debug "streaming range: " start end)
  (let [blob   (d/blobstore od)
        blocks (b/blocks blob od)]
    (try
      (if (full-file? od start end)
        (stream-file od stream blob blocks)
        (stream-range od stream blob blocks start end))
      (catch Exception e
        (error e "error during read"))
      (finally
        (debug "closing after read")
        (.flush stream)
        (.close stream))))
  od)

(defn stream-from
  "Given an input stream and an object descriptor, stream data from the
   input stream to the descriptor.

   Our current approach has the drawback of not enforcing blocksize
   requirements since we have no way of being notified when reaching a
   threshold."
  [^InputStream stream od]
  (let [blob   (d/blobstore od)
        hash   (u/md5-init)]
    (try
      (loop [block  0
             offset 0]
        (when (>= block offset)
          (debug "marking new block")
          (b/start-block! blob od block))

        (let [chunk-size (b/max-chunk blob)
              ba         (byte-array chunk-size)
              br         (.read stream ba)]
          (if (neg? br)
            (do
              (debug "negative write, read whole stream")
              (d/col! od :size offset)
              (d/col! od :checksum (u/md5-sum hash))
              od)
            (let [chunk  (ByteBuffer/wrap ba 0 br)
                  sz     (b/chunk! blob od block offset chunk)
                  offset (+ sz offset)]
              (u/md5-update hash ba 0 br)
              (if (b/boundary? blob block offset)
                (recur offset offset)
                (recur block offset))))))
      (catch Exception e
        (error e "error during write"))
      (finally
        (debug "closing after write")
        (.close stream)))))

(defn stream-copy-range
  [src dst range]
  (let [sblob  (d/blobstore src)
        dblob  (d/blobstore dst)
        blocks (b/blocks sblob src)]
    ::i-should-copy-stuff-here))

(defn stream-copy
  "Copy from one object descriptor to another."
  [src dst]
  (let [sblob  (d/blobstore src)
        dblob  (d/blobstore dst)
        blocks (b/blocks sblob src)]
    (doseq [{:keys [block]} blocks]
      (b/start-block! dblob dst block)
      (debug "found block " block)
      (loop [offset block]
        (when-let [chunks (seq (b/chunks sblob src block offset))]
          (doseq [chunk chunks
                  :let [offset (:offset chunk)]]
            (b/chunk! dblob dst block offset (:payload chunk)))
          (let [{:keys [offset chunksize]} (last chunks)]
            (recur (+ offset chunksize))))))
    (d/col! dst :size (d/size src))
    (d/col! dst :checksum (d/checksum src))
    dst))

(defn stream-copy-part-block
  "Copy a single part's block to a destination"
  [notifier dst hash part g-offset {:keys [block]}]
  (let [dblob      (d/blobstore dst)
        sblob      (d/blobstore part)
        real-block (+ g-offset block)]
    (debug "streaming block: " block)
    (b/start-block! dblob dst real-block)
    (notifier :block)
    (last
     (for [chunk (b/chunks sblob part block block)
           :let [offset      (:offset chunk)
                 payload     (:payload chunk)
                 real-offset (+ g-offset offset)]]
       (do
         (b/chunk! dblob dst real-block real-offset payload)
         (notifier :chunk)
         (let [pos (.position payload)
               sz  (.remaining payload)
               ba  (byte-array sz)]
           (.get payload ba)
           (.position payload pos)
           (u/md5-update hash ba 0 sz)
           (+ real-offset sz)))))))


(defn stream-copy-part
  "Copy a single part to a destination"
  [notifier dst [offset hash] part]
  (let [sblob  (d/blobstore part)
        blocks (b/blocks sblob part)]

    (debug "streaming part: " (d/part part))
    [(reduce (partial stream-copy-part-block notifier dst hash part)
             offset blocks)
     hash]))

(defn stream-copy-parts
  "Given a list of parts, stream their content to a destination inode"
  [parts dst notifier]
  (let [dblob   (d/blobstore dst)
        [size hash] (reduce (partial stream-copy-part notifier dst)
                            [0 (u/md5-init)] parts)]
    (d/col! dst :size size)
    (d/col! dst :checksum (u/md5-sum hash))
    (debug "stored size:" size "and checksum: " (u/md5-sum hash))
    dst))
