(ns io.exo.pithos.bucket
  (:require [qbits.alia :refer [execute]]
            [qbits.hayt :refer [select where set-columns
                                delete update limit]]))

(defn fetch-bucket-q
  [tenant]
  (select :bucket (where {:tenant tenant})))

(defn get-bucket-q
  [tenant  bucket]
  (select :bucket (where {:tenant tenant :bucket bucket}) (limit 1)))

(defn update-bucket-q
  [tenant  bucket attrs tags]
  (update :bucket
          (set-columns {:attrs attrs :tags tags})
          (where {:tenant tenant :bucket bucket})))

(defn delete-bucket-q
  [tenant  bucket]
  (delete :bucket (where {:tenant tenant :bucket bucket})))

(defn fetch
  ([tenant]
     (execute (fetch-bucket-q tenant)))
  ([tenant  bucket]
     (first
      (execute (get-bucket-q tenant bucket)))))

(defn update!
  [tenant  bucket 
   & {:keys [attrs tags] :or {attrs {} tags #{}}}]
  (execute (update-bucket-q tenant bucket attrs tags)))

(defn delete!
  [tenant bucket]
  (if (seq (execute (get-bucket-q tenant bucket)))
    (execute (delete-bucket-q tenant bucket))
    (throw (ex-info "bucket not found" 
                    {:type        :no-such-bucket
                     :status-code 404
                     :tenant      tenant
                     :bucket      bucket}))))

