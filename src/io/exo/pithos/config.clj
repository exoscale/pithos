(ns io.exo.pithos.config
  (:require [clj-yaml.core      :refer [parse-string]]
            [io.exo.pithos.util :refer [to-bytes]]))

(def default-logging
  {:use "io.exo.pithos.logging/start-logging"
   :pattern "%p [%d] %t - %c - %m%n"
   :external false
   :console true
   :files  []
   :level  "info"
   :overrides {:io.exo.pithos "debug"}})

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
  [{:keys [use] :as config}]
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

(defn init
  [path]
  (-> (load-path path)
      (update-in [:logging] (partial merge default-logging))
      (update-in [:logging] get-instance)
      (update-in [:service] (partial merge default-service))
      (update-in [:options] (partial merge default-options))
      (assoc-in [:logging] (partial merge  {}))
      (update-in [:keystore] get-instance)
      (update-in [:userstore] get-instance)
      (update-in [:datastore] get-instance)
      (update-in [:options :chunksize] to-bytes)
      (update-in [:options :maxsize] to-bytes)
      (update-in [:service :max-body] to-bytes)))
