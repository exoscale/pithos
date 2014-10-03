(ns io.pithos.request
  "This namespace provides all necessary wrapper functions to validate and
   augment the incoming request map."
  (:require [clojure.string        :refer [lower-case join]]
            [clojure.tools.logging :refer [debug info warn error]]
            [clojure.pprint        :refer [pprint]]
            [clout.core            :refer [route-matches route-compile]]
            [io.pithos.sig         :refer [validate]]
            [io.pithos.operations  :refer [ex-handler]]
            [io.pithos.system      :refer [service-uri keystore]]
            [ring.util.codec       :as codec]
            [qbits.alia.uuid       :as uuid]))

(def known
  "known query args"
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
    "partnumber"
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
    "uploadid"
    "uploads"
    "versionid"
    "versioning"
    "versions"
    "website"})

(def actions
  "known actions"
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
    :uploadid
    :versioning
    :versions
    :website})

(def subresources
  "known subresources, used when signing"
  {:acl "acl"
   :cors "cors"
   :lifecycle "lifecycle"
   :location "location"
   :logging "logging"
   :notification "notification"
   :partnumber "partNumber"
   :policy "policy"
   :requestpayment "requestPayment"
   :torrent "torrent"
   :uploadid "uploadId"
   :uploads "uploads"
   :versionid "versionId"
   :versioning "versioning"
   :versions "versions"
   :website "website"})

(defn action-routes
  "Really simple router, extracts target (service, bucket or object)"
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
  "Matches incoming route and yields target bucket and object"
  [request [target matcher]]
  (when-let [{bucket :bucket object :*} (matcher request)]
    {:target target :bucket bucket :object object}))

(defn yield-assoc-target
  "closure which for each incoming request will assoc target, bucket
   abnd object"
  []
  (let [routes (action-routes)]
    (fn [request]
      (merge request
        (or (some (partial match-action-route request) routes)
            {:target :unknown})))))

(defn yield-assoc-operation
  "Closure which will build an operation keyword based on the incoming
   request. This is the bulk of the routing in pithos. This becomes necessary
   because S3's behavior varies based on the route, but also based on query
   arguments.

   `action-params` holds query args which are relevant and need to be taken
    into account, when found, it will be part of the operation name."
  [suffixes]
  (fn [{:keys [uri request-method action-params target params] :as request}]
    (let [suffix (some suffixes action-params)
          getpair (fn [[k v]] (if v (str k "=" v) k))
          append (some->> (filter (comp subresources key) params)
                          (map (juxt (comp subresources first) second))
                          (sort-by first)
                          (map getpair)
                          (seq)
                          (join "&")
                          ((partial str "?")))]
      (assoc request
        :sign-uri  (str uri append)
        :action    (when suffix (name suffix))
        :operation (->> (map name (if suffix
                                    [request-method target suffix]
                                    [request-method target]))
                        (join "-")
                        (keyword))))))

(defn keywordized
  "Yield a map where string keys are keywordized"
  [params]
  (dissoc
   (->> (map (juxt (comp keyword known lower-case key) val) params)
        (reduce merge {}))
   nil))

(defn insert-id
  "Assoc a random UUID to a request"
  [req]
  (assoc req :reqid (uuid/random)))

(defn assoc-params
  "Parse, keywordize and store query arguments"
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
  "Discard host from URI"
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
    (fn [{:keys [uri] {:strs [host] :or {host ""}} :headers :as request}]
      (if-let [[_ bucket] (re-find pattern host)]
        (assoc request :uri (transformer bucket uri))
        request))))

(defn authenticate
  "Authenticate tenant, allow masquerading only for _master_ keys"
  [req system]
  (let [auth   (validate (keystore system) req)
        master (:master auth)
        tenant (get-in req [:headers "x-amz-masquerade-tenant"])]
    (assoc req :authorization
           (if (and master tenant) (assoc auth :tenant tenant) auth))))

(defn prepare
  "Generate closures and walks each requests through wrappers."
  [req system]
  (let [service-uri     (service-uri system)
        rewrite-bucket  (yield-rewrite-bucket service-uri)
        assoc-target    (yield-assoc-target)
        assoc-operation (yield-assoc-operation actions)]

    (-> req
        (insert-id)
        (assoc-params)
        (rewrite-host)
        (rewrite-bucket)

        (assoc-target)
        (assoc-operation)
        (authenticate system))))

(defn safe-prepare
  "Wrap prepare in a try-catch block"
  [req system]
  (try (prepare req system)
       (catch Exception e
         {:operation :error :exception e})))
