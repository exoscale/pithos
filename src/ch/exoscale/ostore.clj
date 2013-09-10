(ns ch.exoscale.ostore
  (:require [ch.exoscale.ostore.bucket :as bucket]
            [ch.exoscale.ostore.file   :as file]
            [qbits.alia                :as alia]
            [qbits.hayt                :as h]))

(defn uuid []
  (java.util.uuid/randomUUID))

(defprotocol Common
  (list! [this] [this opts])
  (create! [this] [this opts])
  (delete! [this] [this opts])
  (details! [this] [this opts]))

(defprotocol Path
  (directory? [this])
  (read! [this instream] [this instream options])
  (write! [this outstream] [this outstream options]))

(defn path!
  [session orga {:keys [id] :as bucket} {:keys [path]}]
  (let [bucket_id id]
    (reify
      Common
      (details! [this]
        (a/execute
         (h/select :path
                   (h/where {:organisation organisation
                             :bucket_id bucket_id
                             :path path}))))
      Path
      (directory? [this]
        ()
        )))
  )

(defn bucket!
  [session organisation bucket]
  (let [{:keys [id organisation flags] :as bucket}
        (merge {:organisation organisation :id (uuid) :flags 0} bucket)]
    (reify Common
      (details! [this]
        (bucket!
         session
         orga
         (a/execute
          (h/select :bucket
                    (h/where {:organisation organisation :id id})))))
      (details! [this opts]
        (bucket!
         session
         orga
         (a/execute
          (h/update :bucket
                    (h/set-columns opts)
                    (h/where {:organisation organisation :id id})))))
      (create! [this opts]
        (details! this opts))
      (list! [this {:keys [path] :or {path "/"} :as opts}]
        (map (partial path! session)
             (a/execute
              (h/select :path
                        (h/where {:organisation orga
                                  :bucket_id id
                                  :path path}))))))))
(defn domain!
  [session organisation]
  (reify CommonP
    (details! [this]
      (a/execute
       (h/select :domain (where {:organisation organisation}))))
    (list! [this]
      (map (partial bucket session organisation)
           (a/execute
            (h/select :bucket (h/where {:organisation organisation})))))))
