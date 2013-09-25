(ns io.exo.pithos.http
  (:require [clojure.data.xml     :refer [indent-str]]
            [ring.adapter.jetty   :refer [run-jetty]]
            [ring.util.response   :refer [response header content-type status]]
            [compojure.core       :refer [GET POST PUT DELETE routes]]
            [compojure.handler    :refer [api]]
            [clojure.repl         :refer [pst]]
            [io.exo.pithos.sig    :refer [validate]]
            [qbits.alia           :refer [with-session]]
            [qbits.alia.uuid      :as uuid]
            [io.exo.pithos.inode  :as inode]
            [io.exo.pithos.bucket :as bucket]
            [io.exo.pithos.path   :as path]
            [io.exo.pithos.xml    :as xml]))

(defn handler
  [store]
  (routes
   (GET "/"
        {{:keys [tenant]} :authorization}
        (-> (xml/list-all-my-buckets
             (bucket/fetch tenant))
            (response)
            (content-type "application/xml")))

   (GET "/:bucket/"
        {{:keys [delimiter marker max-keys prefix bucket] :as opts} :params
         {:keys [tenant]} :authorization}
        (let [opts (select-keys opts [:delimiter :prefix :max-keys])
              [contents prefixes] (path/fetch store tenant bucket opts)]
          (-> (xml/list-bucket tenant prefix delimiter
                               1000 bucket contents prefixes)
              (response)
              (content-type "application/xml"))))

   (PUT "/:bucket/*"
        {{bucket :bucket path :*} :route-params
         {:keys [tenant]} :authorization
         body :body
         :as req}
        (let [{:keys [inode]} (path/fetch tenant bucket path)
              version         (inode/bump! inode)
              hash            (inode/append-stream inode version body)]
          (-> (response "")
              (header "ETag" hash)
              (header "Content-Length" "0")
              (header "Connection" "close"))))

   (GET "/:bucket/*"
        {{bucket :bucket path :*} :route-params
         {:keys [tenant]} :authorization
         :as req}
        (throw (ex-info "not supported yet" {})))))

(defn wrap-aws-api
  "Behave like AWS, namely:
    - authenticate requests
    - insert an AWS request ID header
    - coerce output to XML
    - Indicate who we are in the Server header"
  [handler keystore]
  (fn [{:keys [uri server-name] :as request}]
    (let [id      (uuid/random)
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

(defn wrap-filestore
  [handler filestore]
  (fn [request]
    (with-session filestore (handler request))))

(defn run-api
  [keystore filestore opts]
  (run-jetty
   (-> (handler filestore)
       (wrap-filestore filestore)
       (wrap-aws-api keystore)
       (api))
   (merge {:host "127.0.0.1" :port 8080} opts)))
