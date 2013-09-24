(ns io.exo.pithos.http
  (:require [clojure.data.xml     :refer [indent-str]]
            [ring.adapter.jetty   :refer [run-jetty]]
            [ring.util.response   :refer [response header content-type status]]
            [compojure.core       :refer [GET POST PUT DELETE routes]]
            [compojure.handler    :refer [api]]
            [clojure.repl         :refer [pst]]
            [io.exo.pithos        :refer [put!]]
            [io.exo.pithos.sig    :refer [validate]]
            [io.exo.pithos.bucket :refer [buckets!]]
            [io.exo.pithos.path   :refer [paths! path!]]
            [io.exo.pithos.file   :refer [get-stream! file-sum!]]
            [io.exo.pithos.xml    :as xml]))

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

(defn wrap-aws-api
  "Behave like AWS, namely:
    - authenticate requests
    - insert an AWS request ID header
    - coerce output to XML
    - Indicate who we are in the Server header"
  [handler keystore]
  (fn [{:keys [uri server-name] :as request}]
    (let [id      (java.util.UUID/randomUUID)
          uri     (if-let [[_ bucket]
                           (re-find #"^(.*).s3.amazonaws.com$" server-name)]
                    (str "/" bucket (if (not (empty? uri)) uri "/")) uri)
          request (assoc request :uri uri)
          body    (try 
                    (let [authorization
                          (validate keystore request)]
                      (handler (assoc request
                                 :reqid id
                                 :authorization authorization)))
                    (catch Exception e
                      (pst e)
                      (let [{:keys [code] :as data} 
                            (merge {:exception e 
                                    :code 500 
                                    :type :generic}
                                   (ex-data e))]
                        (-> (response (xml/exception data))
                            (status code)))))]
      (-> body
          (header "x-amz-request-id" (str id))
          (header "Server" "OmegaS3")))))

(defn run-api
  [keystore filestore opts]
  (run-jetty
   (-> (handler filestore)
       (wrap-aws-api keystore)
       (api))
   (merge {:host "127.0.0.1" :port 8080} opts)))
