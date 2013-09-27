(ns io.exo.pithos.config
  (:require [clj-yaml.core :refer [parse-string]]))

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
      (update-in [:keystore] get-instance)
      (update-in [:datastore] get-instance)))
