(ns io.pithos.util-test
  (:require [clojure.test   :refer :all]
            [io.pithos.util :refer [inc-prefix to-bytes parse-uuid
                                    string->pattern]]))

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

(deftest uuid-test
  (testing "from uuid"
    (is (= #uuid "05ac767e-170f-a639-1ce7-39078945ee4480"
           (parse-uuid "05ac767e-170f-a639-1ce7-39078945ee4480")))))

(deftest string-to-pattern-test
  (testing "no special characters"
    (is (= "17hj018" (string->pattern "17hj018"))))
  (testing "single character"
    (is (= "/" (string->pattern "/"))))
  (testing "with dots and stars"
    (is (= "\\.\\*89\\+2\\?" (string->pattern ".*89+2?"))))
  (testing "with grouping"
    (is (= "\\(89\\)" (string->pattern "(89)"))))
  (testing "with anchors"
    (is (= "\\^test\\$" (string->pattern "^test$"))))
  (testing "with classes"
    (is (= "\\\\d\\\\s\\\\S" (string->pattern "\\d\\s\\S"))))
  (testing "with sets and repetitions"
    (is (= "\\[a\\-z\\]\\{1,2\\}" (string->pattern "[a-z]{1,2}"))))
  (testing "with alternatives"
    (is (= "abc\\|cde" (string->pattern "abc|cde"))))
  (testing "with escapes"
    (is (= "abc\\\\" (string->pattern "abc\\"))))
  (testing "with back references"
    (is (= "\\(42\\)\\\\1\\\\k\\<here\\>"
           (string->pattern "(42)\\1\\k<here>")))))
