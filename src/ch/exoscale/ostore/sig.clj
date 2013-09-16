(ns ch.exoscale.ostore.sig
  "Compute request signatures as described in
   http://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html"
  (:require [clojure.string              :as s]
            [clojure.data.codec.base64   :as base64]
            [ch.exoscale.ostore.keystore :as keystore])
  (:import  javax.crypto.Mac javax.crypto.spec.SecretKeySpec))

(defn canonicalized
  [headers uri]
  (s/join "\n"
          (concat (->> headers
                       (map (juxt (comp name key) (comp s/trim val)))
                       (filter (comp #(.startsWith % "x-amz") first))
                       (sort-by first)
                       (map (partial s/join ":")))
                  [uri])))

(defn string-to-sign
  [{:keys [headers request-method uri] :as request}]
  (let [content-md5  (get headers "content-md5")
        content-type (get headers "content-type")
        date         (get headers "date")]
    (s/join
     "\n"
     [(-> request-method name s/upper-case)
      (or content-md5 "")
      (or content-type "")
      (or date "")
      (canonicalized headers uri)])))

(defn sign-request
  [request access-key secret-key]
  (let [to-sign (string-to-sign request)
        key     (SecretKeySpec. (.getBytes secret-key) "HmacSHA1")]
    (String. (-> (doto (Mac/getInstance "HmacSHA1") (.init key))
                 (.doFinal (.getBytes to-sign))
                 (base64/encode)))))

(defn validate
  [keystore request]
  (if-let [auth-str (get-in request [:headers "authorization"])]
    (let [[_ access-key sig] (re-matches #"^AWS (.*):(.*)$" auth-str)
          {:keys [secret] :as authorization} (keystore/get! access-key)]

      (when-not (= sig (sign-request request access-key secret))
        (throw (ex-info "invalid request signature"
                        {:type :invalid-signature
                         :auth-string auth-str
                         :expected (sign-request request access-key secret)
                         :to-sign (string-to-sign request)})))
      authorization)
    (throw (ex-info "invalid authorization string"
                    {:type :invalid-auth-string
                     :request request}))))
