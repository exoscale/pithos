(ns io.pithos.cors-test
  (:require [io.pithos.cors  :refer :all]
            [clojure.test    :refer :all]
            [clojure.java.io :as io]))

(deftest xml-slurp-test
  (let [repr {:cors1 [{:origins ["*"]
                       :methods [:get]
                       :headers ["*"]
                       :exposed []
                       :max-age nil}]}]

    (doseq [[src int-repr] repr
            :let [path (format "%s.xml" (name src))
                  ext-repr (slurp (io/resource path))]]
      (testing (str "valid xml input for " (name src))
        (is (= (xml->cors ext-repr) int-repr))))

    (doseq [[src int-repr] repr
            :let [path (format "%s.xml" (name src))
                  ext-repr (slurp (io/resource path))]]
      (testing (str "valid xml output for " (name src))
        (is (= (as-xml int-repr true) ext-repr))))))
