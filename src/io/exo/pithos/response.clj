(ns io.exo.pithos.response)

(defn response
  [body]
  {:status 200
   :headers {}
   :body body})

(defn header
  [req header val]
  (assoc-in req [:headers header] val))

(defn content-type
  [req type]
  (header req "Content-Type" type))

(defn status
  [req status]
  (assoc req :status status))

(defn xml-response
  [body]
  (-> body
      response
      (header "Content-Type" "application/xml")))

(defn html-response
  [body]
  (-> (response body)
      (header "Content-Type" "text/html")))

