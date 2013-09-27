(ns io.exo.pithos.request
  (:require [clojure.string               :refer [lower-case join]]
            [clojure.pprint]
            [io.exo.pithos.sig            :refer [validate]]
            [io.exo.pithos.request.action :refer [yield-assoc-target
                                                  yield-assoc-operation]]
            [ring.util.codec              :as codec]
            [qbits.alia.uuid              :as uuid]))

(def known
  #{"acl"
    "cors"
    "delete"
    "delimiter"
    "lifecycle"
    "location"
    "logging"
    "marker"
    "max-keys"
    "notification"
    "policy"
    "prefix"
    "requestpayment"
    "response-cache-control"
    "response-content-disposition"
    "response-content-encoding"
    "response-content-language"
    "response-expires"
    "restore"
    "tagging"
    "uploads"
    "versionid"
    "versioning"
    "versions"
    "website"})

(def actions
  #{:acl
    :cors
    :delete
    :lifecycle
    :location
    :logging
    :notification
    :policy
    :requestpayment
    :restore
    :tagging
    :uploads
    :versioning
    :versions
    :website})

(defn keywordized
  [params]
  (dissoc
   (->> (map (juxt (comp keyword known lower-case key) val) params)
        (reduce merge {}))
   nil))

(defn insert-id
  [req]
  (assoc req :reqid (uuid/random)))

(defn assoc-params
  [{:keys [query-string] :as req}]
  (or
   (when-let [params (and (seq query-string)
                          (codec/form-decode query-string))]
     (as-> req req
           (assoc req :params (keywordized
                               (cond (map? params)    params
                                     (string? params) {params nil}
                                     :else            {})))
           (assoc req :action-params (set (filter actions (-> req :params keys))))))
   (assoc req :params {} :action-params #{})))

(defn rewrite-host
  [{:keys [uri] :as request}]
  (if-let [[_ trail] (re-find #"^https?://[^/]+/?(.*)" uri)]
    (assoc request :uri (str "/" trail))
    request))

(defn yield-rewrite-bucket
  "Move from a vhost based access method to a full resource access path"
  [service-uri]
  (let [pattern-str (str "^(.*)." service-uri "$")
        pattern     (re-pattern pattern-str)
        transformer (fn [bucket uri] (str "/" bucket (if (seq uri) uri "/")))]
    (fn [{:keys [uri server-name] :as request}]
      (if-let [[_ bucket] (re-find pattern server-name)]
        (assoc request :uri (transformer bucket uri))
        request))))

(defn authenticate
  [req keystore]
  (let [authorization (validate keystore req)]
    (assoc req :authorization authorization)))

(defn debug
  [req]
  (clojure.pprint/pprint req)
  req)

(defn prepare
  [req keystore filestore {:keys [service-uri]}]
  (let [rewrite-bucket  (yield-rewrite-bucket service-uri)
        assoc-target    (yield-assoc-target)
        assoc-operation (yield-assoc-operation actions)]
    
    (-> req
        (insert-id)
        (assoc-params)
        (rewrite-host)
        (rewrite-bucket)
        (authenticate keystore)
        (assoc-target)
        (assoc-operation)
        ;; (authorize filestore)
;;        (debug)
        )))
