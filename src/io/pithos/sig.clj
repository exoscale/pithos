(ns io.pithos.sig
  "Compute request signatures as described in
   http://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html"
  (:require [clojure.string            :as s]
            [clojure.tools.logging     :refer [info debug]]
            [clojure.data.codec.base64 :as base64]
            [clj-time.core             :refer [after? now]]
            [clj-time.coerce           :refer [to-date-time]]
            [constance.comp            :refer [===]])
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
  [{:keys [headers request-method sign-uri params] :as request}]
  (let [content-md5  (get headers "content-md5")
        content-type (get headers "content-type")
        date         (or (get params :expires)
                         (if-not (get headers "x-amz-date")
                           (get headers "date")))]
    (s/join
     "\n"
     [(-> request-method name s/upper-case)
      (or content-md5 "")
      (or content-type "")
      (or date "")
      (canonicalized headers sign-uri)])))

(defn sign-string
  [src secret-key]
  (let [key (SecretKeySpec. (.getBytes secret-key) "HmacSHA1")]
    (String. (-> (doto (Mac/getInstance "HmacSHA1") (.init key))
                 (.doFinal (.getBytes src))
                 (base64/encode)))))

(defn sign-request
  "Sign the request, signatures are basic HmacSHA1s, encoded in base64"
  [request secret-key]
  (sign-string (string-to-sign request) secret-key))

(defn auth
  "Extract access key and signature from the request, using query string
  parameters or Authorization header"
  [request]
  (if-let [auth-str (get-in request [:headers "authorization"])]
    (let [[_ access-key sig] (re-matches #"^[Aa][Ww][Ss] (.*):(.*)$" auth-str)]
      {:sig sig :access-key access-key})
    (let [access-key (get-in request [:params :awsaccesskeyid])
          sig        (get-in request [:params :signature])]
      (if (and access-key sig)
        {:sig sig :access-key access-key}
        nil))))

(defn check-sig
  [request keystore key str sig]
  (let [{:keys [secret] :as authorization} (get keystore key)
        signed (try (sign-string str secret)
                    (catch Exception e
                      {:failed true :exception e}))]
    (when-not (and (not (nil? sig))
                   (string? signed)
                   (=== sig signed))
      (info "will throw because of failed signature!")
      (when (:exception signed)
        (debug (:exception signed) "got exception during signing"))
      (throw (ex-info "invalid policy signature"
                      {:type :signature-does-not-match
                       :status-code 403
                       :request request
                       :expected signed
                       :to-sign str})))
    (update-in authorization [:memberof] concat ["authenticated-users"])))

(defn validate
  "Validate an incoming request (e.g: make sure the signature is correct),
   when applicable (requests may be unauthenticated)"
  [keystore request]
  (if-let [data (auth request)]
    (let [{:keys [sig access-key]}           data
          {:keys [secret] :as authorization} (get keystore access-key)
          signed (try (sign-request request secret)
                      (catch Exception e
                        {:failed true :exception e}))]
      (when-not (and (not (nil? sig))
                     (string? signed)
                     (constant-string= sig signed))
        (info "will throw because of failed signature!")
        (when (:exception signed)
          (debug (:exception signed) "got exception during signing"))
        (debug "string-to-sign: " (string-to-sign request))
        (throw (ex-info "invalid request signature"
                        {:type :signature-does-not-match
                         :status-code 403
                         :request request
                         :expected signed
                         :to-sign (string-to-sign request)})))
      (when-let [expires (get-in request [:params :expires])]
        (let [expires (to-date-time (* 1000 (Integer/parseInt expires)))]
          (when (after? (now) expires)
            (throw (ex-info "expired request"
                            {:type :expired-request
                             :status-code 403
                             :request request
                             :expires expires})))))
      (update-in authorization [:memberof] concat ["authenticated-users"]))
    {:tenant :anonymous :memberof ["anonymous"]}))
