(ns io.pithos.cors
  (:require [clojure.data.xml     :refer [parse-str emit-str indent-str]]
            [clojure.tools.logging :refer [debug]]
            [clojure.zip          :refer [xml-zip node root]]
            [clojure.data.zip     :refer [children]]
            [clojure.data.zip.xml :refer [xml-> xml1-> text]]
            [io.pithos.xml        :refer [seq->xml]]))

(defn node->rule
  "Provide a "
  [node]
  {:origin  (vec (xml-> node :AllowedOrigin text))
   :methods (vec (xml-> node :AllowedMethod text))
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
                (for [{:keys [origin methods headers exposed max-age]} rules]
                  (apply vector :CORSRule
                         (concat
                          (mapv (partial vector :AllowedOrigin) origin)
                          (mapv (partial vector :AllowedMethod) methods)
                          (mapv (partial vector :AllowedHeader) headers)
                          (mapv (partial vector :ExposedHeader) exposed)
                          (if max-age
                            [[:MaxAgeSeconds max-age]]
                            [])))))))))
  ([rules]
     (as-xml rules false)))
