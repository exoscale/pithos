(ns io.pithos.acl
  "The purpose of this namespace is to coerce to and from
   the internal representation of ACLs.

   The current representation is:

     {:FULL_CONTROL
      [{:ID \"AUTH_KEY\" :DisplayName \"Some Name\"}
       {:URI \"http://groups/group-uri\"}]
      ...}
"
  (:require [clojure.data.xml     :refer [parse-str emit-str indent-str]]
            [clojure.zip          :refer [xml-zip node root]]
            [clojure.data.zip     :refer [children]]
            [clojure.data.zip.xml :refer [xml-> xml1-> text]]
            [io.pithos.xml        :refer [seq->xml]]))

;; ### XML ACL parsing functions
;;
;; We're doing a very sloppy type of schema validation
;; this should likely move to a stricter XSD validation
;; phase.

(def ^{:doc "List of known permissions. Valid in ACLs"}
  valid-permission?
  #{:FULL_CONTROL :READ :WRITE :READ_ACP :WRITE_ACP})

(def ^{:doc "List of known tags in grantees"}
  valid-grantee-tag?
  #{:ID :DisplayName :URI})

(def ^{:doc "List of known URIs"}
  known-uris
  {"http://acs.amazonaws.com/groups/global/AllUsers" "anonymous"})

(def ^{:doc "List of known Groups"}
  known-groups
  (reduce merge {} (map (juxt val key) known-uris)))

(defn node->grantee-spec
  "Produce a grantee specifier (ID, DisplayName or URI)"
  [n]
  (let [{:keys [tag content]} (node n)
        text                  (first content)]
    (when (and (valid-grantee-tag? tag) (string? text))
      (if (= :URI tag)
        (hash-map tag (or (known-uris text) text))
        (hash-map tag text)))))

(defn node->grantee
  "Produce a valid grantee."
  [n]
  (reduce merge {} (xml-> n children node->grantee-spec)))

(defn node->grant
  "Each grant in an input body shoudl contain at least an ID and DisplayName or
  a URI"
  [node]
  (hash-map
   (xml1-> node :Permission text (fnil keyword "invalid"))
   (vec (xml-> node :Grantee node->grantee))))


(defn safe-xml-zip
  "Ingest an XML representation, safely, throwing explicit
   and details errors."
  [src]
  (try
    (let [tree          (xml-zip (parse-str src))
          {:keys [tag]} (root tree)]
      (when-not (= :AccessControlPolicy tag)
        (throw (ex-info "XML Root Node should be AccessControlPolicy"
                        {:type :invalid-xml-root-node
                         :expected :AccessControlPolicy
                         :got      tag})))
      tree)
    (catch clojure.lang.ExceptionInfo e
      (throw e))
    (catch Exception e
      (throw (ex-info "Invalid XML in ACL Body"
                      {:type :invalid-acl-xml
                       :status-code 400})))))

(defn xml->acl
  "Given an XML source, try to parse it and return valid"
  [src]
  (let [xml-tree (safe-xml-zip src)
        policies (xml-> xml-tree
                        :AccessControlList
                        :Grant
                        node->grant)
        policy   (apply merge-with (comp vec concat) policies)]
    (when-not (every? valid-permission? (keys policy))
      (throw (ex-info "Invalid XML Acl Body" {:type :invalid-acl-xml
                                              :status-code 400})))
    policy))

(defn grant->permission
  "Generate grant XML tags from a hash map of permissions to grantees"
  [[permission grantees]]
  (let [xmlns-xsi "http://www.w3.org/2001/XMLSchema-instance"]
    (for [{:keys [ID DisplayName URI]} grantees]
      [:Grant
       (if URI
         [:Grantee {:xmlns:xsi xmlns-xsi :xsi:type "Group"}
          [:URI (or (known-groups URI) URI)]]
         [:Grantee {:xmlns:xsi xmlns-xsi :xsi:type "CanonicalUser"}
          [:ID ID]
          [:DisplayName DisplayName]])
       [:Permission (name permission)]])))

(defn as-xml
  "Given an internal representation of an ACL, output a valid
   XML representation.
   Optionaly supply a boolean to indicate whether to indent the output"
  ([grants indent?]
     (let [xmlns     "http://s3.amazonaws.com/doc/2006-03-01/"
           format    (if indent? indent-str emit-str)]
       (format
        (seq->xml
         [:AccessControlPolicy {:xmlns xmlns}
          [:Owner
           [:ID "foo"]
           [:DisplayName "bar"]]
          (apply vector :AccessControlList
                 (mapcat grant->permission grants))]))))
  ([grants]
     (as-xml grants false)))
