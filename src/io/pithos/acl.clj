(ns io.pithos.acl
  "The purpose of this namespace is to coerce to and from
   the internal representation of ACLs.

   The current representation is:

     {:FULL_CONTROL
      [{:ID \"AUTH_KEY\" :DisplayName \"Some Name\"}
       {:URI \"http://groups/group-uri\"}]
      ...}
"
  (:require [clojure.data.xml     :refer [parse-str indent-str]]
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

(defn node->grantee-spec
  "Produce a grantee specifier (ID, DisplayName or URI)"
  [n]
  (let [{:keys [tag content]} (node n)
        text                  (first content)]
    (when (and (valid-grantee-tag? tag) (string? text))
      (hash-map tag text))))

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
                      {:type :invalid-acl-xml})))))

(defn xml->acl
  "Given an XML source, try to parse it and return valid"
  [src]

  (let [xml-tree (safe-xml-zip src)
        policies (xml-> xml-tree
                        :AccessControlList
                        :Grant
                        node->grant)
        policy   (apply merge-with concat policies)]
    (when-not (every? valid-permission? (keys policy))
      (throw (ex-info "Invalid XML Acl Body" {:type :invalid-acl-xml})))
    policy))

(defn as-xml
  "Given an internal representation of an ACL, output a valid
   XML representation."
  [grants]
  (let [xmlns     "http://s3.amazonaws.com/doc/2006-03-01/"
        xmlns-xsi "http://www.w3.org/2001/XMLSchema-instance"]
    (indent-str
     (seq->xml
      [:AccessControlPolicy {:xmlns xmlns}
       [:Owner
        [:ID "foo"]
        [:DisplayName "bar"]]
       (conj (for [[permission grantees] grants]
               (conj (for [{:keys [ID DisplayName URI]} grantees]
                       (if URI
                         [:Grantee {:xmlns:xsi xmlns-xsi :xsi:type "Group"}
                          [:URI URI]]
                         [:Grantee {:xmlns:xsi xmlns-xsi :xsi:type "CanonicalUser"}
                          [:ID ID]
                          [:DisplayName DisplayName]]))
                     [:Permission (name permission)]
                     :Grant))
             :AccessControlList)]))))



;; ACL handling functions

(def
  ^{:doc "Map of expected permission per operation."}
  by-operation
  {})

(defn matches?
  "Authorize operation `what` for origin `who` against rules `acl`"
  [{:keys [grants] :as acl} who what]
  
  true)
