(ns io.pithos.response
  "Provides ring like facilities for working with
   HTTP responses"
  (:require [clojure.tools.logging :refer [debug error]]))

(defn response
  "Create a basic response, with optional body"
  ([]
     {:status 200 :headers {}})
  ([body]
     {:status 200 :headers {} :body body}))

(defn header
  "Add a header to a response, coerce value to string"
  [resp header val]
  (let [strval (if (keyword? val) (name val) (str val))]
    (assoc-in resp [:headers header] strval)))

(defn content-type
  "Add Content-Type header"
  [resp type]
  (header resp "Content-Type" type))

(defn status
  "Set response status code"
  [resp status]
  (assoc resp :status status))

(defn xml-response
  "Yields a HTTP response, assuming body is XML data"
  [body]
  (-> body
      response
      (header "Content-Type" "application/xml")))

(defn html-response
  "Yields a HTTP response, assuming body is HTML data"
  [body]
  (-> (response body)
      (header "Content-Type" "text/html")))

(defn request-id
  "Provision S3 specific headers"
  [resp {:keys [reqid]}]
  (-> resp
      (header "Server" "Pithos")
      (header "x-amz-id-2" (str reqid))
      (header "x-amz-request-id" (str reqid))))

(defn exception-status
  "When handler raised an exception, try to look up a status code
   in its data"
  [resp details]
  (let [{:keys [status-code] :or {status-code 500}} details]
    (-> resp
        (status status-code))))
