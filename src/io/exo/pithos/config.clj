(ns io.exo.pithos.config
  (:require [clj-yaml.core         :refer [parse-string]]
            [clojure.tools.logging :refer [error info debug]]
            [io.exo.pithos.util    :refer [to-bytes]]))

(def default-logging
  {:use "io.exo.pithos.logging/start-logging"
   :pattern "%p [%d] %t - %c - %m%n"
   :external false
   :console true
   :files  []
   :level  "info"
   :overrides {:io.exo.pithos "debug"}})

(def default-keystore
  {:use "io.exo.pithos.keystore/map->MapKeystore"})

(def default-metastore
  {:use "io.exo.pithos.store/cassandra-store"})

(def default-blobstore
  {:use "io.exo.pithos.store/cassandra-store"})

(def default-service
  {:host "127.0.0.1"
   :port 8080})

(def default-options
  {:service-uri "s3.amazonaws.com"
   :chunksize "256K"
   :maxsize   "10G"
   :reporting true
   :server-side-encryption true
   :multipart-upload true
   :masterkey-provisioning true
   :masterkey-access true})

(defn find-ns-var
  [s]
  (try
    (let [n (namespace (symbol s))]
      (require (symbol n))
      (find-var (symbol s)))
    (catch Exception _
      nil)))

(defn instantiate
  "Find a symbol pointing to a function of a single argument and
   call it"
  [class config]
  (if-let [f (find-ns-var class)]
    (f config)
    (throw (ex-info (str "no such namespace: " class)))))

(defn get-instance
  [{:keys [use] :as config} target]
  (debug "building " target " with " use)
  (instantiate (-> use name symbol) config))

(defn load-path
  "Try to find a pathname, on the command line, in
   system properties or the environment and load it."
  [path]
  (-> (or path
          (System/getProperty "pithos.configuration")
          (System/getenv "PITHOS_CONFIGURATION")
          "/etc/pithos/pithos.yaml")
      slurp 
      parse-string))

(defn get-storage-classes
  [storage-classes]
  (->> (for [[storage-class blobstore] storage-classes
             :let [blobstore (merge default-blobstore blobstore)]]
         [storage-class (get-instance blobstore :blobstore)])
       (reduce merge {})))

(defn get-region-stores
  [regions]
  (->> (for [[region {:keys [metastore storage-classes]}] regions
             :let [metastore (merge default-metastore metastore)]]
         [(name region)
          {:metastore       (get-instance metastore :metastore)
           :storage-classes (get-storage-classes storage-classes)}])
       (reduce merge {})))

(defn init
  [path quiet?]
  (try
    (when-not quiet?
      (println "starting with configuration: " path))
    (-> (load-path path)
        (update-in [:logging] (partial merge default-logging))
        (update-in [:logging] get-instance :logging)
        (update-in [:service] (partial merge default-service))
        (update-in [:options] (partial merge default-options))
        (update-in [:keystore] (partial merge default-keystore))
        (update-in [:keystore] get-instance :keystore)
        (update-in [:metastore] (partial merge default-metastore))
        (update-in [:metastore] get-instance :metastore)
        (update-in [:regions] get-region-stores)
        (as-> config (let [{:keys [regions options]} config]
                       (or (:default-region options) 
                           (throw (Exception. "no default region.")))
                       (when-not (get regions (:default-region options))
                         (throw (Exception. "no default region config.")))
                       config))
        (update-in [:options :chunksize] to-bytes :chunksize)
        (update-in [:options :maxsize] to-bytes :maxsize)
        (update-in [:service :max-body] to-bytes :max-body))
    (catch Exception e
      (when-not quiet?
        (println "invalid or incomplete configuration: " (str e)))
      (error e "invalid or incomplete configuration")
      (System/exit 1))))
