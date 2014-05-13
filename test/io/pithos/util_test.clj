(ns io.pithos.util-test
  (:require [clojure.test   :refer :all]
            [io.pithos.util :refer [inc-prefix]]))

(deftest inc-prefix-test
  (testing "nil prefix"
    (is (= nil (inc-prefix nil))))
  (testing "empty prefix"
    (is (= nil (inc-prefix ""))))

  (testing "simple prefix"
    (is (= "fop" (inc-prefix "foo")))))
