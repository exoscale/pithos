(ns io.pithos.sig
  "Compute request signatures as described in
   http://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html"
  (:require [clojure.string            :as s]
            [clojure.tools.logging     :refer [info debug]]
            [clojure.data.codec.base64 :as base64]
            [io.pithos.keystore        :as ks])
  (:import  javax.crypto.Mac javax.crypto.spec.SecretKeySpec))

(defn canonicalized
  "Group headers starting with x-amz, each on a separate line and add uri"
  [headers uri]
  (s/join "\n"
          (concat (->> headers
                       (map (juxt (comp name key) (comp s/trim val)))
                       (filter (comp #(.startsWith % "x-amz") first))
                       (sort-by first)
                       (map (partial s/join ":")))
                  [uri])))

(defn string-to-sign
  "Yield the string to sign for an incoming request"
  [{:keys [headers request-method sign-uri] :as request}]
  (let [content-md5  (get headers "content-md5")
        content-type (get headers "content-type")
        date         (get headers "date")]
    (s/join
     "\n"
     [(-> request-method name s/upper-case)
      (or content-md5 "")
      (or content-type "")
      (or date "")
      (canonicalized headers sign-uri)])))

(defn sign-request
  "Sign the request, signatures are basic HmacSHA1s, encoded in base64"
  [request access-key secret-key]
  (let [to-sign (string-to-sign request)
        key     (SecretKeySpec. (.getBytes secret-key) "HmacSHA1")]
    (String. (-> (doto (Mac/getInstance "HmacSHA1") (.init key))
                 (.doFinal (.getBytes to-sign))
                 (base64/encode)))))

(defn validate
  "Validate an incoming request (e.g: make sure the signature is correct),
   when applicable (requests may be unauthenticated)"
  [keystore request]
  (if-let [auth-str (get-in request [:headers "authorization"])]
    (let [[_ access-key sig] (re-matches #"^[Aa][Ww][Ss] (.*):(.*)$" auth-str)
          {:keys [secret] :as authorization} (ks/fetch keystore access-key)
          signed (try (sign-request request access-key secret)
                      (catch Exception e
                        {:failed true :exception e}))]
      (when-not (= sig signed)
        (info "will throw because of failed signature!")
        (debug "string-to-sign: " (string-to-sign request))
        (throw (ex-info "invalid request signature"
                        {:type :signature-does-not-match
                         :status-code 403
                         :auth-string auth-str
                         :request request
                         :expected signed
                         :to-sign (string-to-sign request)})))
      (update-in authorization [:memberof] concat ["authenticated-users"]))
    {:tenant :anonymous :memberof ["anonymous"]}))
