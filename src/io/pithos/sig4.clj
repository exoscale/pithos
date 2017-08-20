(ns io.pithos.sig4
  (:require [clojure.string :as str]
            [clojure.tools.logging     :refer [info debug]]
            [clj-time.core :as time]
            [clj-time.format :as format]
            [ring.util.codec                  :as codec])
  (:import  [javax.crypto Mac]
            [javax.crypto.spec SecretKeySpec]
            [java.security MessageDigest]))


(defn parse-authorization [request]
  """
  Parse an AWS SIG4 authorization header.. e.g.

  AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20170805/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=fadf01d63c3c6e4c8238625fc971eddf7a1b2d0470750a21ae8f33c03b4bbdb7

  TYPE<SPACE>KEY=VALUE/VALUE/VALUE,KEY=VALUE;VALUE;VALUE,KEY=VALUE
  """
  (let [
    authorization-header (get (get request :headers) "authorization")
    authorization (zipmap [:access-key :date :region :service :signed-headers :signature]
      (rest (re-find #"AWS4-HMAC-SHA256 Credential=(\w+)\/(\d{8})\/([\w\d-]+)\/([\w\d]+)\/aws4_request,[ ]*SignedHeaders=([\w-;]+),[ ]*Signature=(\w+)" authorization-header))
    )]
    (assoc authorization :signed-headers (str/split (get authorization :signed-headers) #";"))
  ))

(defn sha256 [input]
  (let [hash (MessageDigest/getInstance "SHA-256")]
    (. hash digest input)))

(defn secretKeyInst [key mac]
    (SecretKeySpec. key (.getAlgorithm mac)))

(defn hmac-sha256 [key string]
  "Returns the signature of a string with a given key, using a SHA-256 HMAC."
  (let [mac (Mac/getInstance "HMACSHA256")
        secretKey (secretKeyInst key mac)]
    (-> (doto mac
          (.init secretKey)
          (.update (.getBytes string)))
        .doFinal)))

(defn hex [input]
  """ Format bytes as a hex string """
  (apply str (map #(format "%02x" %) input)))

(defn bytes [input]
  """ Format a string as bytes """
  (. input getBytes))

(defn request-time [request]
  """ Parse the date or x-amzdate headers and return a time object """
  (let [headers (get request :headers)]
    (cond
      (contains? headers "x-amz-date")
      (format/parse (format/formatters :basic-date-time-no-ms) (get headers "x-amz-date"))
      (contains? headers "date")
      (format/parse (format/formatters :basic-date-time-no-ms) (get headers "date"))
      )))

(defn signing-key [secret-key request-time authorization]
  """ Generate a signing key for a v4 signature """
  (debug secret-key)
  (-> (str "AWS4" secret-key)
      (bytes)
      (hmac-sha256 (format/unparse (format/formatters :basic-date) request-time))
      (hmac-sha256 (get authorization :region))
      (hmac-sha256 (get authorization :service))
      (hmac-sha256 "aws4_request")
  ))

(defn canonical-verb [request]
  ( -> (get request :request-method) name str/upper-case))

(defn- double-escape [^String x]
  (.replace (.replace x "\\" "\\\\") "$" "\\$"))

(defn- percent-encode [^String unencoded]
  (->> (.getBytes unencoded "UTF-8")
       (map (partial format "%%%02X"))
       (str/join)))

(defn uri-escape [unencoded]
  (str/replace
    unencoded
    #"[^A-Za-z0-9_~.\-/]+"
    #(double-escape (percent-encode %))))

(defn query-escape [unencoded]
  (str/replace
    unencoded
    #"[^A-Za-z0-9_~.\-]+"
    #(double-escape (percent-encode %))))

(defn canonical-uri [request]
  (uri-escape (get request :orig-uri)))

(defn canonical-query-string [request]
  (let [
    query-string (get request :query-string)
    decoded (and (seq query-string) (codec/form-decode query-string))
    params  (cond (map? decoded)    decoded
                  (string? decoded) {decoded nil}
                  :else            {})]

    (str/join "&"
      (->> params
           (map (juxt (comp query-escape name key) (comp query-escape str/trim (fn [input] (if (nil? input) "" input)) val)))
           (sort-by first)
           (map (partial str/join "="))
      ))))

(defn canonical-headers [request, include-headers]
    (str/join "\n"
            (concat (->> (select-keys (get request :headers) include-headers)
                         (map (juxt (comp name key) (comp str/trim val)))
                         (sort-by first)
                         (map (partial str/join ":")))
                    )))

(defn signed-headers [include-headers]
  (str/join ";" (sort-by first include-headers)))

(defn hash-payload [request]
  """ Hash the entire body. Thankfully this is done for us - its in the
  x-amz-content-sha256 and we have wrapped :body in something that will error
  if the stream is closed before reading out content matching the sha """
  (get (get request :headers) "x-amz-content-sha256"))

(defn canonical-request [request include-headers]
  (str/join "\n" [
    (canonical-verb request)
    (canonical-uri request)
    (canonical-query-string request)
    (canonical-headers request include-headers)
    ""
    (signed-headers include-headers)
    (hash-payload request)
  ]))

(defn string-to-sign [request request-time authorization]
  """ Format a request into a canonicalized representation for signing """
  (let [canonical-request (canonical-request request (get authorization :signed-headers))]
    (debug "canonical-request" canonical-request)
    (str/join "\n" [
      "AWS4-HMAC-SHA256"
      (format/unparse (format/formatters :basic-date-time-no-ms) request-time)
      (str/join "/" [
        (format/unparse (format/formatters :basic-date) request-time)
        (get authorization :region)
        (get authorization :service)
        "aws4_request"
      ])
      (hex (sha256 (bytes canonical-request)))
    ])))

(defn signature [signing-key, string-to-sign]
  """ Sign a canonicalized representation of the request with a signing key """
  (hex (hmac-sha256 signing-key string-to-sign)))

(defn is-signed-by? [request authorization secret-key]
  (let[
    request-time (request-time request)
    signing-key (signing-key secret-key request-time authorization)
    string-to-sign (string-to-sign request request-time authorization)
    signature (signature signing-key, string-to-sign)
    ]
    (debug request-time)
    (debug (hex signing-key))
    (debug string-to-sign)
    (debug signature (get authorization :signature) (= signature (get authorization :signature)))
    (= signature (get authorization :signature))
  )
)

(defn sha256-input-stream [body, goal-hash]
  """ Wrap a body stream with a hashing adapter that will throw if the data is invalid """
  (let [hash (MessageDigest/getInstance "SHA-256")]
    (proxy [java.io.InputStream] []
      (close []
        (try
          ;; Calculate final digest and if doesn't match expected value - throw
          (if (not= goal-hash (hex (.digest hash)))
            ;; FIXME: Is there a more appropriate error here?
            (throw (ex-info "body signature is incorrect"
              {:type :signature-does-not-match
                :status-code 403
                :expected goal-hash
                :to-sign ""
                })))
          (finally (.close body)))
         )

      (read [^bytes ba]
        (let [bytes_read (.read body ba)]
          (if (not= bytes_read -1) (.update hash ba 0 bytes_read))
          bytes_read))
    )))

(defn validate4
  [keystore request]
  (let [
    authorization (parse-authorization request)
    secret-key (get (get keystore (get authorization :access-key)) :secret)
    is-valid-signature (is-signed-by? request authorization secret-key)]
    (debug "request" request)
    (debug "authorization" (get keystore (get authorization :access-key)))
    (debug "secret" secret-key)
    (debug "is-valid-sig" is-valid-signature)
    (cond
      is-valid-signature
      {:tenant (get authorization :tenant) :memberof ["authenticated-users", "anonymous"]}
      :else
      {:tenant :anonymous :memberof ["anonymous"]}
      )))
