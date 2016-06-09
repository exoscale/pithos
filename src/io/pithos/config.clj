(ns io.pithos.config
  "This namespace parses YAML data into clojure forms which
   are then augmented with a mechanism initialy described here:

   http://bit.ly/1xRgOLb

   Default implementation for protocols are provided but can be overriden
   with the `use` keyword.
"
  (:require [clj-yaml.core         :refer [parse-string]]
            [clojure.tools.logging :refer [error info debug]]
            [io.pithos.util        :refer [to-bytes]]
            [unilog.config         :refer [start-logging!]]
            [raven.client          :refer [capture!]]
            [net.http.client       :refer [build-client]]))


(start-logging!
 {:pattern "%p [%d] %t - %c - %m%n"
  :external false
  :console true
  :files  []
  :level  "info"
  :overrides {}})

(def default-logging
  "Logging can be bypassed if a logback configuration is provided
    to the underlying JVM"
  {:use "org.spootnik.logconfig/start-logging!"
   :pattern "%p [%d] %t - %c - %m%n"
   :external false
   :console true
   :files  []
   :level  "info"
   :overrides {:io.pithos "debug"}})

(def default-keystore
  "keystore defaults to MapKeyStore"
  {:use "io.pithos.keystore/map-keystore"})

(def default-bucketstore
  "bucketstore defaults to cassandra"
  {:use "io.pithos.bucket/cassandra-bucket-store"})

(def default-metastore
  "metastore defaults to cassandra"
  {:use "io.pithos.meta/cassandra-meta-store"})

(def default-blobstore
  "blobstore defaults to cassandra, a max chunk of 512k
   and no more than 2048 chunks per block"
  {:use "io.pithos.blob/cassandra-blob-store"
   :max-chunk        "512k"
   :max-block-chunks 2048})

(def default-reporter
  "reporters default to logging"
  {:use "io.pithos.reporter/logging-reporter"})

(def default-service
  "The http service is exposed on localhost port 8080 by default"
  {:host "127.0.0.1"
   :port 8080})

(def default-options
  "Some default global options."
  {:service-uri "s3.amazonaws.com"
   :reporting true
   :server-side-encryption true
   :multipart-upload true
   :masterkey-provisioning true
   :masterkey-access true})

(defn find-ns-var
  "Go fetch a var in a namespace. Extracts the namespace and requires it,
   then finds the var"
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
    (throw (ex-info (str "no such namespace: " class) {}))))

(defn get-instance
  "Create instance by supplying config to the implementation specified
   in `use`"
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
  "Create instances of blobstores for all storage classes (in a region)"
  [storage-classes]
  (->> (for [[storage-class blobstore] storage-classes
             :let [blobstore (-> (merge default-blobstore blobstore)
                                 (update-in [:max-chunk] to-bytes :max-chunk))]]
         [storage-class (get-instance blobstore :blobstore)])
       (reduce merge {})))

(defn get-region-stores
  "Create instances for each region's metastore then create storage classes"
  [regions]
  (->> (for [[region {:keys [metastore storage-classes]}] regions
             :let [metastore (merge default-metastore metastore)]]
         [(name region)
          {:metastore       (get-instance metastore :metastore)
           :storage-classes (get-storage-classes storage-classes)}])
       (reduce merge {})))

(defn get-reporters
  [reporters]
  (for [reporter reporters
        :let [reporter (merge default-reporter reporter)]]
    (get-instance reporter :reporter)))

(defn get-sentry
  [sentry]
  (if sentry
    (let [client (build-client (:http sentry))]
      (fn [ev]
        (capture! client (:dsn sentry) ev)))
    (fn [& _]
      (debug "no sentry configuration, no capture done."))))

(defn parse-cors
  [rules]
  (let [->sym    (fn [s] (-> s name .toLowerCase keyword))
        sanitize (fn [{:keys [methods] :as rule}]
                   (assoc rule :methods (map ->sym methods)))]
    (mapv sanitize rules)))

(defn init
  "Parse YAML file, merge in defaults and then create instances
   where applicable"
  [path quiet?]
  (try
    (when-not quiet?
      (println "starting with configuration: " path))
    (let [opts (load-path path)]
      (info "setting up logging according to config")
      (start-logging! (merge default-logging (:logging opts)))
      (-> opts
          (update-in [:service] (partial merge default-service))
          (update-in [:options] (partial merge default-options))
          (update-in [:options :default-cors] parse-cors)
          (update-in [:keystore] (partial merge default-keystore))
          (update-in [:keystore] get-instance :keystore)
          (update-in [:bucketstore] (partial merge default-bucketstore))
          (update-in [:bucketstore] get-instance :bucketstore)
          (update-in [:reporters] get-reporters)
          (update-in [:sentry] get-sentry)
          (update-in [:regions] get-region-stores)))
    (catch Exception e
      (when-not quiet?
        (println "invalid or incomplete configuration: " (str e)))
      (error e "invalid or incomplete configuration")
      (System/exit 1))))
