(ns io.pithos.request
  "This namespace provides all necessary wrapper functions to validate and
   augment the incoming request map."
  (:require [clojure.string                   :refer [lower-case join starts-with?]]
            [clojure.tools.logging            :refer [debug info warn error]]
            [clojure.pprint                   :refer [pprint]]
            [clojure.java.io                  :as io]
            [io.pithos.sig                    :refer [validate check-sig anonymous]]
            [io.pithos.sig4                   :refer [validate4 sha256-input-stream]]
            [io.pithos.system                 :refer [service-uri keystore]]
            [io.pithos.util                   :refer [string->pattern uri-decode]]
            [clout.core                       :as c]
            [ring.middleware.multipart-params :as mp]
            [ring.util.request                :as req]
            [ring.util.codec                  :as codec]
            [clojure.data.codec.base64        :as base64]
            [cheshire.core                    :as json]
            [qbits.alia.uuid                  :as uuid])
  (:import  [java.io ByteArrayInputStream]
            [java.io ByteArrayOutputStream]))

(def known
  "known query args"
  #{"acl"
    "awsaccesskeyid"
    "cors"
    "delete"
    "delimiter"
    "expires"
    "file"
    "key"
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
    "response-content-type"
    "response-content-disposition"
    "response-content-encoding"
    "response-content-language"
    "response-expires"
    "restore"
    "signature"
    "success_action_redirect"
    "success_action_status"
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
  {:acl                          "acl"
   :cors                         "cors"
   :delete                       "delete"
   :lifecycle                    "lifecycle"
   :location                     "location"
   :logging                      "logging"
   :notification                 "notification"
   :partnumber                   "partNumber"
   :policy                       "policy"
   :response-content-disposition "response-content-disposition"
   :response-content-type        "response-content-type"
   :response-content-encoding    "response-content-encoding"
   :response-content-language    "response-content-language"
   :response-cache-control       "response-cache-control"
   :response-expires             "response-expires"
   :requestpayment               "requestPayment"
   :tagging                      "tagging"
   :torrent                      "torrent"
   :uploadid                     "uploadId"
   :uploads                      "uploads"
   :versionid                    "versionId"
   :versioning                   "versioning"
   :versions                     "versions"
   :website                      "website"})

(defn action-routes
  "Really simple router, extracts target (service, bucket or object)"
  []
  (let [sroute  (c/route-compile "/")
        broute1 (c/route-compile "/:bucket")
        broute2 (c/route-compile "/:bucket/")
        oroute  (c/route-compile "/:bucket/*")]
    [[:service (partial c/route-matches sroute)]
     [:bucket  (partial c/route-matches broute1)]
     [:bucket  (partial c/route-matches broute2)]
     [:object  (partial c/route-matches oroute)]]))

(defn match-action-route
  "Matches incoming route and yields target bucket and object"
  [request [target matcher]]
  (when-let [{bucket :bucket object :*} (matcher request)]
    {:target target :bucket (uri-decode bucket) :object (uri-decode object)}))

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
    (let [suffix  (some suffixes action-params)
          getpair (fn [[k v]] (if (and v (seq v)) (str k "=" v) k))
          append  (some->> (filter (comp subresources key) params)
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

(defn assoc-orig-uri
  "Assoc a random UUID to a request"
  [req]
  (assoc req :orig-uri (get req :uri)))

(defn protect-body-stream [request]
  (let [headers (get request :headers)]
    (if (and (contains? headers "x-amz-content-sha256") (not= (get headers "x-amz-content-sha256") "UNSIGNED-PAYLOAD"))
      (assoc request :body (sha256-input-stream (get request :body) (get headers "x-amz-content-sha256"))))
      request))

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
           (assoc req :action-params
                  (set (filter actions (-> req :params keys))))))
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
  (let [pattern-str (str "^(.*)\\." (string->pattern service-uri) "$")
        pattern     (re-pattern pattern-str)
        transformer (fn [bucket uri] (str "/" bucket (if (seq uri) uri "/")))]
    (fn [{:keys [uri] {:strs [host] :or {host ""}} :headers :as request}]
      (if-let [[_ bucket] (re-find pattern host)]
        (assoc request :uri (transformer bucket uri))
        request))))

(defn authenticate
  "Authenticate tenant, allow masquerading only for _master_ keys"
  [{:keys [multipart-params request-method sign-uri] :as req} system]

  (cond

    (= request-method :options)
    (assoc req :authorization anonymous)

    (and (= request-method :post) (seq multipart-params))
    (let [{:keys [signature awsaccesskeyid policy]} multipart-params
          [_ bucket] (re-find #"^/[^/]*(/.*)?$" sign-uri)
          auth (check-sig req (keystore system) awsaccesskeyid policy signature)]
      (assoc req
        :post-upload? true
        :authorization auth
        :policy (json/parse-string (String. (-> policy
                                                .getBytes
                                                base64/decode))
                                   true)))
    (and (contains? (get req :headers) "authorization") (starts-with? (get (get req :headers) "authorization") "AWS4-"))
    (assoc req :authorization (validate4 (keystore system) req))
    :else
    (let [auth   (validate (keystore system) req)
          master (:master auth)
          tenant (get-in req [:headers "x-amz-masquerade-tenant"])]
      (assoc req :authorization
             (if (and master tenant) (assoc auth :tenant tenant) auth)))))

(defn decode-uri
  [req]
  (update-in req [:uri] uri-decode))

(defn multipart-params
  [req]
  (if (= (req/content-type req) "multipart/form-data")
    (let [make-input-stream #(when % (java.io.FileInputStream. %))]
      (-> (mp/multipart-params-request req)
          (update-in [:params] #(reduce merge {} (filter (comp keyword? key) %)))
          (update-in [:multipart-params] keywordized)
          (update-in [:multipart-params :file :tempfile] make-input-stream)))
    req))

(defn prepare
  "Generate closures and walks each requests through wrappers."
  [req system]
  (let [service-uri     (service-uri system)
        rewrite-bucket  (yield-rewrite-bucket service-uri)
        assoc-target    (yield-assoc-target)
        assoc-operation (yield-assoc-operation actions)]

    (-> req
        (insert-id)
        (assoc-orig-uri)
        (assoc-params)
        (protect-body-stream)
        (rewrite-host)
        (rewrite-bucket)

        (assoc-target)
        (assoc-operation)

        (multipart-params)
        (authenticate system)
        (decode-uri))))

(defn safe-prepare
  "Wrap prepare in a try-catch block"
  [req system]
  (try (prepare req system)
       (catch Exception e
         (debug e "unhandled exception during request preparation")
         (insert-id
          {:operation :error :exception e}))))
