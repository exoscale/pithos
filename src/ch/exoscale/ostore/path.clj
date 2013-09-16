(ns ch.exoscale.ostore.path
  (:require [qbits.alia              :refer [connect cluster execute]]
            [qbits.hayt              :refer [select where set-columns
                                             update limit]]
            [ch.exoscale.ostore      :refer [sync! list! get! put! bump! del!]]
            [ch.exoscale.ostore.file :as file]))

(defrecord Path [store organization bucket path id version attrs tags]
  ch.exoscale.ostore.Common
  (sync! [this]
    (execute
     store
     (update :path
             (set-columns (select-keys this [:id :version :attrs :tags]))
             (where (select-keys this [:organization :bucket :path])))))
  (get! [this stream]
    (let [id      (or id (java.util.UUID/randomUUID))
          version (or version 0)
          p       (map->Path (assoc this :id id :version version))]
      (sync! p)
      (file/get-file! store id version stream)))
  (put! [this stream]
    (let [id      (or id (java.util.UUID/randomUUID))
          version (if version (inc version) 0)]
      ;; first, bump version
      (execute 
       store
       (update :path
               (set-columns {:version version :id id})
               (where (select-keys this [:organization :bucket :path]))))

      ;; then upload file
      (file/put-file! store id version stream)

      ;; return new record
      (assoc this :id id :version version))))

(defn path!
  [store organization bucket path]
  (->> (execute store
                (select :path
                        (where {:organization organization
                                :bucket bucket
                                :path path})
                        (limit 1)))
       (first)
       (merge {:store store})
       (map->Path)))

(defn filter-content
  [paths prefix delimiter]
  (let [pat (re-pattern (str "^" prefix "[^\\" delimiter "]+$"))]
    (filter (comp (partial re-find pat) :path) paths)))

(defn filter-prefixes
  [paths prefix delimiter]
  (let [pat (re-pattern
             (str "^(" prefix "[^\\" delimiter "]+\\" delimiter ").*$"))]
    (->> (map (comp second (partial re-find pat) :path) paths)
         (remove nil?)
         (set))))

(defn inc-path
  [p]
  (let [[c & s] (reverse p)
        reversed (conj s (-> c int inc char))]
    (apply str (reverse reversed))))

(defn paths!
  [store organization bucket {:keys [prefix delimiter max-keys]}]
  (let [path-def (if (and prefix (not (empty? prefix)))
                   {:organization organization
                    :bucket       bucket
                    :path         [:>= prefix]
                    'path         [:< (inc-path prefix)]}
                   {:organization organization
                    :bucket       bucket})
        paths    (->> (execute store (select :path (where path-def)))
                      (filter #(.startsWith (:path %) (or prefix ""))))
        prefixes (if delimiter (filter-prefixes paths prefix delimiter) #{})
        contents (if delimiter (filter-content paths prefix delimiter) paths)]
    [(remove prefixes contents) prefixes]))
