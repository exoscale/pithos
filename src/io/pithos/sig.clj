(ns io.pithos.sig
  "Compute request signatures as described in
   http://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html"
  (:require [clojure.string            :as s]
            [clojure.data.codec.base64 :as base64]
            [ring.util.codec           :as codec]
            [clojure.tools.logging     :refer [info debug]]
            [clj-time.core             :refer [after? now]]
            [clj-time.coerce           :refer [to-date-time]]
            [org.spootnik.constance    :refer [constant-string=]]
            [io.pithos.util            :refer [cond-let]])
  (:import  javax.crypto.Mac javax.crypto.spec.SecretKeySpec))

(defn method-dispatcher
  "Dispatcher for multi-methods relying on the signing method"
  [params & _]
  (:method params))

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

(defmulti string-to-sign method-dispatcher)

(defmethod string-to-sign :v2
  [_ {:keys [headers request-method sign-uri params] :as request}]
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


(defn v4-header?
  [[k v]]
  (or (= "host" k)
      (= "content-type" k)
      (.startsWith k "x-amz"))
  )
(defn v4-canonical
  [request]
  (let [headers (->> (:headers request)
                     (map (juxt (comp s/lower-case key) val))
                     (filter v4-header?)
                     (sort-by first))]
    (s/join
     "\n"
     [(-> request :request-method name s/upper-case)
      (:sign-uri request)
      (->> (codec/form-decode (:query-string request))
           (map (juxt (comp s/lower-case key) val))
           (sort-by first)
           (map (partial apply format "%s=%s"))
           (s/join "&"))
      (->> headers
           (map (partial apply format "%s:%s"))
           (s/join "&"))
      (s/join ";" (map first headers))
      (-> request :body slurp hex-sha256)

      ]
     ))
  )



(defmethod string-to-sign :v4
  [sign-params {:keys [headers request-method sign-uri params] :as request}]
  (let [canonical    (v4-canonical request)
        content-md5  (get headers "content-md5")
        content-type (get headers "content-type")
        date         (or (get params :expires)
                         (if-not (get headers "x-amz-date")
                           (get headers "date")))]
    (s/join
     "\n"
     [(-> request-method name s/upper-case)
      (s/join "&" (map ))
      (or content-md5 "")
      (or content-type "")
      (or date "")
      (canonicalized headers sign-uri)])))

(defn hmac-string
  [algo src secret-key]
  (let [key (SecretKeySpec. (.getBytes secret-key) algo)]
    (String. (-> (doto (Mac/getInstance algo) (.init key))
                 (.doFinal (.getBytes src))
                 (base64/encode)))))

(defmulti sign-request method-dispatcher)

(defmethod sign-request :v2
  [{:keys [secret-key] :as params} request]
  (hmac-string "HmacSHA1" (string-to-sign params request) secret-key))

(defmethod sign-request :v4
  [{:keys [secret-key] :as params} request]
  (hmac-string "HmacSHA256" (string-to-sign params request) secret-key))

(defn v4-query-params
  [params])

(defn v4-header-params
  [auth]
  (->> (for [var (s/split auth #",")
             :let [[k v] (s/split var #"=" 2)]
             :when (and k v)
             :let [type (some-> k s/lower-case keyword)]]
         [type
          (cond
            (= type :credential)    (s/split v #"/")
            (= type :signedheaders) (set (map keyword (s/split v #";")))
            :else                   v)])
       (into {:method :v4})))

(defn auth
  [request]
  (let [auth-str (get-in request [:headers "authorization"] "")
        v2params ((juxt :awsaccesskeyid :signature) (:params request))
        v4params (v4-query-params (:params request))]

    (cond-let
     [[_ auth] (re-matches #"^AWS4-HMAC-SHA256 (.*)$" auth-str)]
     (v4-header-params auth)

     [[_ access-key sig] (re-matches #"^[Aa][Ww][Ss] (.*):(.*)$" auth-str)]
     {:access-key access-key :sig sig :method :v2}

     [[access-key sig] v2params]
     (when (and access-key sig)
       {:access-key access-key :sig sig :method :v2}))))

(defn check-sig
  "Validate a signature for a POST upload, which is the only case where
   signature parameters will live in multipart params"
  [request keystore key str sig]
  (let [{:keys [secret] :as authorization} (get keystore key)
        signed (try (sign-string-sha1 str secret)
                    (catch Exception e
                      {:failed true :exception e}))]
    (when-not (and (not (nil? sig))
                   (string? signed)
                   (constant-string= sig signed))
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

(defmulti assoc-secret method-dispatcher)

(defmethod assoc-secret :v2
  [params keystore]
  (merge params (get keystore (:access-key params))))

(defmethod assoc-secret :v4
  [params keystore]
  (merge params (get keystore (-> params :credential first))))

(defn extract-authorization
  [params]
  (select-keys params [:tenant :memberof]))

(defn validate
  "Validate an incoming request (e.g: make sure the signature is correct),
   when applicable (requests may be unauthenticated)"
  [keystore request]
  (if-let [raw-params (auth request)]
    (let [sign-params (assoc-secret raw-params keystore)
          signed (try (sign-request sign-params request)
                      (catch Exception e
                        {:failed true :exception e}))]
      (when-not (and (not (nil? (:signature sign-params)))
                     (string? signed)
                     (constant-string= (:signature sign-params) signed))
        (info "will throw because of failed signature!")
        (when (:exception signed)
          (debug (:exception signed) "got exception during signing"))
        (debug "string-to-sign: " (string-to-sign sign-params request))
        (throw (ex-info "invalid request signature"
                        {:type :signature-does-not-match
                         :status-code 403
                         :request request
                         :expected signed
                         :to-sign (string-to-sign sign-params request)})))
      (when-let [expires (get-in request [:params :expires])]
        (let [expires (to-date-time (* 1000 (Integer/parseInt expires)))]
          (when (after? (now) expires)
            (throw (ex-info "expired request"
                            {:type :expired-request
                             :status-code 403
                             :request request
                             :expires expires})))))
      (update-in (extract-authorization sign-params)
                 [:memberof] concat ["authenticated-users"]))
    {:tenant :anonymous :memberof ["anonymous"]}))
