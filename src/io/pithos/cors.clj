(ns io.pithos.cors
  (:refer-clojure :exclude [replace])
  (:require [clojure.data.xml      :refer [parse-str emit-str indent-str]]
            [clojure.string        :refer [upper-case lower-case replace join]]
            [clojure.tools.logging :refer [debug]]
            [clojure.zip           :refer [xml-zip node root]]
            [clojure.data.zip      :refer [children]]
            [clojure.data.zip.xml  :refer [xml-> xml1-> text]]
            [io.pithos.util        :refer [string->pattern]]
            [io.pithos.xml         :refer [seq->xml]]))

(defn node->rule
  "Provide a "
  [node]
  {:origins (vec (xml-> node :AllowedOrigin text))
   :methods (vec (map (comp keyword lower-case)
                      (xml-> node :AllowedMethod text)))
   :headers (vec (xml-> node :AllowedHeader text))
   :exposed (vec (xml-> node :ExposedHeader text))
   :max-age (xml1-> node :MaxAgeSeconds text)})

(defn safe-xml-zip
  "Ingest an XML representation, safely, throwing explicit
   and details errors."
  [src]
  (try
    (let [tree          (xml-zip (parse-str src))
          {:keys [tag]} (root tree)]
      (when-not (= :CORSConfiguration tag)
        (throw (ex-info "XML Root Node should be CORSConfiguration"
                        {:type :invalid-xml-root-node
                         :expected :CORSConfiguration
                         :got      tag})))
      tree)
    (catch clojure.lang.ExceptionInfo e
      (throw e))
    (catch Exception e
      (throw (ex-info "Invalid XML in ACL Body"
                      {:type :invalid-acl-xml
                       :status-code 400})))))

(defn xml->cors
  [src]
  (let [xml-tree (safe-xml-zip src)
        rules    (xml-> xml-tree
                        :CORSRule
                        node->rule)]
    (vec rules)))

(defn as-xml
  ([rules indent?]
     (let [format (if indent? indent-str emit-str)
           xml-ns "http://s3.amazonaws.com/doc/2006-03-01/"]
       (format
        (seq->xml
         (apply vector
                :CORSConfiguration {:xmlns xml-ns}
                (for [{:keys [origins methods headers exposed max-age]} rules]
                  (apply vector :CORSRule
                         (concat
                          (mapv (partial vector :AllowedOrigin) origins)
                          (mapv (partial vector :AllowedMethod)
                                (map (comp upper-case name) methods))
                          (mapv (partial vector :AllowedHeader) headers)
                          (mapv (partial vector :ExposedHeader) exposed)
                          (if max-age
                            [[:MaxAgeSeconds max-age]]
                            [])))))))))
  ([rules]
     (as-xml rules false)))

(defn origin-matches?
  [src dst]
  (let [dst (string->pattern dst)
        pat (str "^" (replace dst "\\*" "(.*)") "$")]
    (re-find (re-pattern pat) src)))

(defn origins-match?
  [origin method req-headers {:keys [origins methods headers]}]
  (and (some #(origin-matches? origin %) origins)
       ((set methods) method)))

(defn merge-rules
  [left right]
  (if (sequential? left)
    (set (concat left right))
    (if (neg? (compare left right)) left right)))

(defn rule->headers
  [origin method req-headers {:keys [methods exposed headers max-age]}]
  (-> {"Access-Control-Allow-Origin"   origin
       "Access-Control-Allow-Methods"  (-> method name upper-case)
       "Access-Control-Allow-Headers"  (join ", " headers)
       "Access-Control-Expose-Headers" (join ", " exposed)}
      (cond-> max-age (assoc "Access-Control-Max-Age" (str max-age)))))

(defn matches?
  [cors headers method]
  (let [origin     (get headers "origin")
        method     (if (= method :options)
                     (some-> (get headers "access-control-request-method")
                             lower-case
                             keyword)
                     method)
        req-headers (get headers "access-control-request-headers")]
    (when-not method
      (throw (ex-info "Invalid Argument" {:type :invalid-argument
                                          :status-code 400
                                          :arg "Access-Control-Request-Method"
                                          :val ""})))
    (if-let [matching-rules (seq (filter (partial origins-match?
                                                  origin method req-headers)
                                         cors))]
      (rule->headers
       origin method req-headers
       (reduce (partial merge-with merge-rules) {} matching-rules))
      {})))
