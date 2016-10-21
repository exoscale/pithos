(ns io.pithos.acl-test
  (:require [clojure.test    :refer :all]
            [clojure.pprint  :refer [pprint]]
            [io.pithos.acl   :refer [as-xml xml->acl]]
            [clojure.java.io :as io]))

(deftest xml-to-acl-test
  (let [repr {:acl1 {:FULL_CONTROL [{:ID "foo" :DisplayName "bar"}]}
              :acl4 {:FULL_CONTROL [{:ID "foo" :DisplayName "bar"}
                                    {:URI "bar" :DisplayName "bar"}]
                     :READ_ACP [{:ID "foo" :DisplayName "baz"}
                                {:URI "baz" :DisplayName "baz"}]}
              :acl5 {:READ [{:URI "anonymous"
                             :DisplayName "anonymous"}]}}]

    (doseq [[src int-repr] repr
            :let [path (format "%s.xml" (name src))
                  ext-repr (slurp (io/resource path))]]
      (testing (str "valid xml input for " (name src))
        (is (= (xml->acl ext-repr) int-repr))))

    (doseq [[src int-repr] repr
            :let [path (format "%s.xml" (name src))
                  ext-repr (slurp (io/resource path))]]
      (testing (str "valid xml output for " (name src))
               (is (= (as-xml int-repr true) ext-repr))))

    (testing "invalid xml"
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Invalid XML in ACL Body"
           (xml->acl (slurp (io/resource "acl2.xml")))))

      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"XML Root Node should be AccessControlPolicy"
           (xml->acl (slurp (io/resource "acl3.xml"))))))))
