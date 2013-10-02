(ns io.exo.pithos.request
  (:require [clojure.string               :refer [lower-case join]]
            [clojure.tools.logging        :refer [debug info warn]]
            [clout.core                   :refer [route-matches route-compile]]
            [io.exo.pithos.sig            :refer [validate]]
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

(defn action-routes
  []
  (let [sroute (route-compile "/")
        broute1 (route-compile "/:bucket")
        broute2 (route-compile "/:bucket/")
        oroute (route-compile "/:bucket/*")]
    [[:service (partial route-matches sroute)]
     [:bucket  (partial route-matches broute1)]
     [:bucket  (partial route-matches broute2)]
     [:object  (partial route-matches oroute)]]))

(defn match-action-route
  [request [target matcher]]
  (when-let [{bucket :bucket object :*} (matcher request)]
    {:target target :bucket bucket :object object}))

(defn yield-assoc-target
  []
  (let [routes (action-routes)]
    (fn [request]
      (merge request
        (or (some (partial match-action-route request) routes)
            {:target :unknown})))))

(defn yield-assoc-operation
  [suffixes]
  (fn [{:keys [request-method action-params target] :as request}]
    (assoc request
      :operation
      (->> (if-let [suffix (some suffixes action-params)]
             [request-method target suffix]
             [request-method target])
           (map name)
           (join "-")
           keyword))))

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
  (let [auth   (validate keystore req)
        master (:master auth)
        tenant (get-in req [:headers "x-amz-masquerade-tenant"])]
    (debug "got auth details: " auth master tenant (:headers req))
    (assoc req :authorization 
           (if (and master tenant) (assoc auth :tenant tenant) auth))))

(defn prepare
  [req keystore metastore regions {:keys [service-uri]}]
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
        (assoc-operation))))
