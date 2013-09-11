(ns ch.exoscale.ostore.query
  (:require [clojure.data.codec.base64 :as base64]
            [clj-http.client           :as client])
  (:import javax.crypto.Mac
           javax.crypto.spec.SecretKeySpec
           java.security.MessageDigest))

(def api-key "AKIAJIKJHHDND7DQBLDQ")
(def api-secret "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")
(def df (java.text.SimpleDateFormat. "EEE, dd MMM yyyy HH:mm:ss zzz"))

(defn sign-request
  [secret str]
  (let [key (SecretKeySpec. (.getBytes secret) "HmacSHA1")
        mac (doto (Mac/getInstance "HmacSHA1") (.init key))]
    (String.
     (-> (.doFinal mac (.getBytes str))
         (base64/encode)))))

(defn auth-header
  [key secret str]
  (format "AWS %s:%s" key (sign-request secret str)))

(defn buckets!
  []
  (let [date (.format df (java.util.Date.))]
    (client/get "https://s3.amazonaws.com"
                {:headers {"Date" 
                           date
                           "Authorization" 
                           (auth-header api-key
                                        api-secret
                                        (format "GET\n\n\n%s\n/" date))}})))
