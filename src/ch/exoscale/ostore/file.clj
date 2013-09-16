(ns ch.exoscale.ostore.file
  (:require [qbits.alia      :refer [execute]]
            [qbits.hayt      :refer [select insert where values]]
            [clojure.java.io :as io])
  (:import java.nio.ByteBuffer))

(def maxchunk 2048)

(defn put-file!
  [store id version stream]
  (loop [offset 0]
    (let [bb (doto (ByteBuffer/allocate maxchunk) (.position 0))
          br (.read stream (.array bb))]
      (if (= br -1)
        offset
        (do
          (.limit bb br)
          (execute
           store
           (insert :file
                   (values {:id id
                            :version version
                            :offset offset
                            :chunksize br
                            :payload bb})))
          (recur (+ offset br)))))))

(defn get-file!
  [store id version stream]
  (doseq [{:keys [chunksize payload]}
          (execute
           store
           (select :file (where {:id id :version version})))
          :let [btow (- (.limit payload) (.position payload))
                ba   (byte-array btow)]]
    (.get payload ba)
    (.write stream ba))
  (.close stream))
