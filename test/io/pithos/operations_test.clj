(ns io.pithos.operations-test
  (:require [clojure.test         :refer :all]
            [io.pithos.desc       :as desc]
            [io.pithos.operations :refer [get-range]]))

(deftest range-test

  (let [desc   (reify desc/BlobDescriptor (size [this] :nope))
        in-out ["no headers"    {}                                  [0  :nope]
                "range"         {"range" "bytes=10-90"}             [10 90]
                "content-range" {"content-range" "bytes 10-90/200"} [10 90]]]
    (doseq [[nickname headers output] (partition 3 in-out)]
      (testing (str "valid output for " nickname)
        (is (= output (get-range desc headers)))))))
