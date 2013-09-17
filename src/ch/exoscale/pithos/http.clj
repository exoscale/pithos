(ns ch.exoscale.pithos.http
  (:require [clojure.data.xml          :refer [indent-str]]
            [ring.adapter.jetty        :refer [run-jetty]]
            [ring.util.response        :refer [response header response?
                                               content-type status]]
            [ring.middleware.resource  :refer [wrap-resource]]
            [compojure.core            :refer [GET POST PUT DELETE routes]]
            [compojure.handler         :refer [api]]
            [clojure.repl              :refer [pst]]
            [ch.exoscale.pithos        :refer [put!]]
            [ch.exoscale.pithos.sig    :refer [validate]]
            [ch.exoscale.pithos.bucket :refer [buckets!]]
            [ch.exoscale.pithos.path   :refer [paths! path!]]
            [ch.exoscale.pithos.file   :refer [get-stream! file-sum!]]
            [ch.exoscale.pithos.xml    :as xml]))

(defn handler
  [store]
  (routes
   (GET "/"
        {{:keys [organization]} :authorization}
        (-> (xml/list-all-my-buckets
             (buckets! store organization))
            (response)
            (content-type "application/xml")))

   (GET "/:bucket/"
        {{:keys [delimiter marker max-keys prefix bucket] :as opts} :params
         {:keys [organization]} :authorization}
        (let [opts (select-keys opts [:delimiter :prefix :max-keys])
              [contents prefixes] (paths! store organization bucket opts)]
          (-> (xml/list-bucket organization prefix delimiter
                               1000 bucket contents prefixes)
              (response)
              (content-type "application/xml"))))

   (PUT "/:bucket/*"
        {{bucket :bucket path :*} :route-params
         {:keys [organization]}   :authorization
         body                     :body
         :as req}
        (let [{:keys [version id] :as p}
              (path! store organization bucket path)
              hash (put! p body)]
          (-> (response "")
              (header "ETag" hash)
              (header "Content-Length" "0")
              (header "Connection" "close"))))

   (GET "/:bucket/*"
        {{bucket :bucket path :*} :route-params
         {:keys [organization]}   :authorization
         :as req}
        (let [{:keys [version id]} (path! store organization bucket path)]
          (-> (response (get-stream! store id version))
              (header "ETag" (file-sum! store id version))
              (content-type "application/download"))))))

(defn update-host
  [{:keys [uri server-name] :as request}]
  (if-let [[_ bucket] (re-find #"^(.*).s3.amazonaws.com$" server-name)]
    (assoc request :uri 
           (str "/" bucket (if (and uri (not (empty? uri))) uri "/")))
    request))

(defn wrap-aws-api
  "Behave like AWS, namely:
    - authenticate requests
    - insert an AWS request ID header
    - coerce output to XML
    - Indicate who we are in the Server header"
  [handler keystore]
  (fn [request]
    (let [id      (java.util.UUID/randomUUID)
          request (update-host request)
          body    (try (let [authorization
                             (validate keystore request)]
                         (handler (assoc request
                                    :reqid id
                                    :authorization authorization)))
                       (catch Exception e
                         (pst e)
                         (xml/exception
                          (merge {:exception e :code 500 :type :generic}
                                 (ex-data e)))))
          code    (if (instance? Exception body) (get body :code 500) 200)
          resp    (if (response? body) body (response body))]
      (-> resp
          (status code)
          (header "x-amz-request-id" (str id))
          (header "Server" "OmegaS3")))))

(defn run-api
  [keystore filestore opts]
  (run-jetty
   (-> (handler filestore)
       (wrap-aws-api keystore)
       (api))
   (merge {:host "127.0.0.1" :port 8080} opts)))
