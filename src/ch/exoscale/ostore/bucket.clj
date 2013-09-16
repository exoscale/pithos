(ns ch.exoscale.ostore.bucket
  (:require [qbits.alia :refer [connect cluster execute]]
            [qbits.hayt :refer [select where set-columns update limit]]
            [ch.exoscale.ostore.path :refer [map->Path]]))

(defrecord Bucket [store organization bucket attrs tags]
  ch.exoscale.ostore.Common
  (list! [this]
    (map (comp map->Path (partial merge {:store store}))
         (execute store (select :path
                                  (where {:organization organization
                                          :bucket bucket})))))
  (sync! [this]
    (execute 
     store
     (update :bucket
             (set-columns (select-keys this [:bucket :attrs :tags]))
             (where {:organization organization :bucket bucket})))))

(defn buckets!
  ([store organization]
     (map (comp map->Bucket (partial merge {:store store}))
          (execute store 
                   (select :bucket (where {:organization organization})))))
  ([store organization bucket]
     (->> (execute store 
                   (select :bucket
                           (where {:organization organization
                                   :bucket bucket})
                           (limit 1)))
          (first)
          (merge {:store store})
          (map->Bucket))))
