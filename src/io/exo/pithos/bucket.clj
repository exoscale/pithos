(ns io.exo.pithos.bucket
  (:require [qbits.alia :refer [execute]]
            [qbits.hayt :refer [select where set-columns delete update limit]]))

(defn fetch-bucket-q
  [^String tenant]
  (select :bucket (where {:tenant tenant})))

(defn get-bucket-q
  [^String tenant ^String bucket]
  (select :bucket (where {:tenant tenant :bucket bucket}) (limit 1)))

(defn update-bucket-q
  [^String tenant ^String bucket attrs tags]
  {:pre [(non-empty-strings? tenant bucket) (map? attrs) (set? tags)]}
  (update :bucket
          (set-columns {:attrs attrs :tags tags})
          (where {:tenant tenant :bucket bucket})))

(defn delete-bucket-q
  [^String tenant ^String bucket]
  (delete :bucket (where {:tenant tenant :bucket bucket})))

(defn fetch
  ([^String tenant]
     {:pre (non-empty-strings? tenant)}
     (execute (fetch-bucket-q tenant)))
  ([^String tenant ^String bucket]
     {:pre (non-empty-strings? tenant bucket)}
     (first
      (execute (get-bucket-q tenant bucket)))))

(defn update!
  [^String tenant ^String bucket 
   & {:keys [attrs tags] :or {attrs {} tags #{}}}]
  {:pre [(non-empty-strings? tenant bucket) (map? attrs) (set? tags)]}
  (execute (update-bucket-q tenant bucket attrs tags)))

(defn delete!
  [^String tenant ^String bucket]
  {:pre (non-empty-strings? tenant bucket)}
  (execute (delete-bucket-q tenant bucket)))
