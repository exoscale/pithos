(ns io.exo.pithos.file
  (:require [qbits.alia      :refer [execute]]
            [qbits.hayt      :refer [select insert where values
                                     columns order-by limit]]
            [clojure.java.io :as io])
  (:import java.nio.ByteBuffer))

(comment
  (def maxchunk 2048)

  (defn put-file!
    [store id version stream]
    (let [hash (doto (java.security.MessageDigest/getInstance "MD5") (.reset))]
      (loop [offset 0]
        (let [bb (doto (ByteBuffer/allocate maxchunk) (.position 0))
              br (.read stream (.array bb))]
          (if (= br -1)
            (.toString (java.math.BigInteger. 1 (.digest hash)) 16)
            (do
              (.limit bb br)
              (.update hash (.array bb) 0 br)
              (.position bb 0)
              (execute
               store
               (insert :file
                       (values {:id id
                                :version version
                                :offset offset
                                :chunksize br
                                :payload bb})))
              (recur (+ offset br))))))))

  (defn get-file!
    [store id version stream]
    (try
      (let [chunks (execute
                    store
                    (select :file (where {:id id :version version})))]
        (doseq [{:keys [offset chunksize payload]} chunks
                :let [btow (- (.limit payload) (.position payload))
                      ba   (byte-array btow)]]
          (.get payload ba)
          (.write stream ba)))
      (catch Exception e
        (println "GOT EXCEPTION " e))))

  (defn get-size!
    [store id version]
    (let [{:keys [offset chunksize]}
          (first 
           (execute store
                    (select :file
                            (columns :offset :chunksize)
                            (where {:id id :version version})
                            (order-by [:offset :desc])
                            (limit 1))))]
      (+ offset chunksize)))

  (defn get-chunk!
    [store id version offset]
    (first
     (execute store
              (select :file
                      (columns :chunksize :payload)
                      (where {:id id :version version :offset offset})
                      (limit 1)))))

  (defn get-stream!
    [store id version]
    (piped-input-stream
     (fn [stream]
       (get-file! store id version stream))))

  (defn file-sum!
    [store id version]
    (let [hash (doto (java.security.MessageDigest/getInstance "MD5") (.reset))]
      (doseq [{:keys [payload]}
              (execute
               store
               (select :file 
                       (columns :payload) 
                       (where {:id id :version version})))
              :let [btow (- (.limit payload) (.position payload))
                    ba   (byte-array btow)]]
        (.get payload ba)
        (.update hash ba))
      (.toString (java.math.BigInteger. 1 (.digest hash)) 16)))
  )
