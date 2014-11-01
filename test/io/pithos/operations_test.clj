(ns io.pithos.operations-test
  (:require [clojure.test         :refer :all]
            [io.pithos.desc       :as desc]
            [io.pithos.operations :refer [get-range]]))

(deftest range-test

  (let [desc   (reify desc/BlobDescriptor (size [this] 1024))
        in-out ["no headers"    {}                                  [0  1024]
                "range"         {"range" "bytes=10-90"}             [10 90]
                "content-range" {"content-range" "bytes 10-90/200"} [10 90]
                "begin-range1"  {"range" "bytes=10-"}               [10 1024]
                "begin-range2"  {"content-range" "bytes 10-/200"}   [10 1024]
                "priority"      {"range"         "bytes=1-"
                                 "content-range" ""}                [1 1024]]]
    (doseq [[nickname headers output] (partition 3 in-out)]
      (testing (str "valid output for " nickname)
        (is (= output (get-range desc headers)))))))
