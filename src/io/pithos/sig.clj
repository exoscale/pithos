(ns io.pithos.sig
  "Compute request signatures as described in
   http://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html"
  (:require [clojure.string            :as s]
            [clojure.tools.logging     :refer [info]]
            [clojure.data.codec.base64 :as base64]
            [io.pithos.keystore        :as ks])
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
          {:keys [secret] :as authorization} (ks/fetch keystore access-key)]

      (info "got authorization: " authorization)

      (when-not (= sig (sign-request request access-key secret))
        (throw (ex-info "invalid request signature"
                        {:type :signature-does-not-match
                         :auth-string auth-str
                         :request request
                         :expected (sign-request request access-key secret)
                         :to-sign (string-to-sign request)})))
      (update-in authorization [:memberof] concat ["authenticated-users"]))
    {:tenant :anonymous :memberof ["anonymous"]}))
