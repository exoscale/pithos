(ns io.exo.pithos.bucket
  (:require [qbits.alia :refer [execute]]
            [qbits.hayt :refer [select where set-columns
                                delete update limit]]))

(defn bucket-by-tenant-q
  [tenant]
  (select :bucket (where {:tenant tenant})))

(defn fetch-bucket-q
  [bucket]
  (select :bucket (where {:bucket bucket}) (limit 1)))

(defn get-absolute-bucket-q
  [bucket]
  (select :bucket (where {:bucket bucket}) (limit 1)))

(defn update-bucket-q
  [bucket columns]
  (update :bucket
          (set-columns columns)
          (where {:bucket bucket})))

(defn delete-bucket-q
  [tenant  bucket]
  (delete :bucket (where {:bucket bucket})))

(defn by-tenant
  [tenant]
  (execute (bucket-by-tenant-q tenant)))

(defn fetch
  [bucket]
  (first
   (execute (fetch-bucket-q bucket))))

(defn create!
  [tenant bucket attrs]
  (if-let [[details] (seq (execute (get-absolute-bucket-q bucket)))]
    (when (not= (:tenant details tenant) tenant)
      (throw (ex-info 
              "bucket already exists"
              {:type :bucket-already-exists
               :bucket bucket
               :status-code 409})))
    (execute (update-bucket-q bucket (merge attrs {:tenant tenant})))))

(defn update!
  [bucket attrs]
  (execute (update-bucket-q bucket attrs)))

(defn delete!
  [tenant bucket]
  (if-let [info (seq (execute (fetch-bucket-q bucket)))]
    (if (= tenant (:tenant info))
      (execute (delete-bucket-q tenant bucket))
      (throw (ex-info "not your bucket" {})))
    (throw (ex-info "bucket not found" 
                    {:type        :no-such-bucket
                     :status-code 404
                     :tenant      tenant
                     :bucket      bucket}))))

