(ns io.pithos.util-test
  (:require [clojure.test   :refer :all]
            [io.pithos.util :refer [inc-prefix to-bytes]]))

(deftest inc-prefix-test
  (testing "nil prefix"
    (is (= nil (inc-prefix nil))))
  (testing "empty prefix"
    (is (= nil (inc-prefix ""))))

  (testing "simple prefix"
    (is (= "fop" (inc-prefix "foo")))))

(deftest byte-factor-test
  (testing "from bytes"
    (is (= 512 (to-bytes "512"))))

  (testing "from kilobytes"
    (is (= 2048 (to-bytes "2k"))))

  (testing "from megabytes"
    (is (= 2097152 (to-bytes "2m"))))

  (testing "from gigabytes"
    (is (= 2147483648 (to-bytes "2G"))))

  (testing "from terabytes"
    (is (= 2199023255552 (to-bytes "2tb"))))

  (testing "from petabytes"
    (is (= 2251799813685248 (to-bytes "2Pb")))))
