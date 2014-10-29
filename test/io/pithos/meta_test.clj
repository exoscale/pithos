(ns io.pithos.meta-test
  (:require [clojure.test    :refer :all]
            [io.pithos.meta  :refer [filter-content filter-prefixes]]))

(deftest prefixes-and-contents-test

  (let [in-and-outs ["simple list"
                     [{:object "foo/bar.txt"}
                      {:object "foo/baz.txt"}]
                     ""
                     "/"
                     #{"foo/"}
                     []
                     10
                     nil
                     false

                     "no delimiter"
                     [{:object "foo/bar.txt"}
                      {:object "foo/baz.txt"}]
                     ""
                     nil
                     #{}
                     [{:object "foo/bar.txt"}
                      {:object "foo/baz.txt"}]
                     ""
                     10
                     nil
                     false
                     ]]
    (doseq [[nickname objects prefix delimiter
             prefixes keys max-keys marker truncated?]
            (partition 9 in-and-outs)]
      (testing (str "valid output for " nickname)

        (let [found-prefixes (filter-prefixes objects prefix delimiter)]
          (is (= prefixes found-prefixes))
          (is (= keys (remove found-prefixes
                              (filter-content objects prefix delimiter)))))))))
