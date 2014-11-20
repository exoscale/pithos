(ns io.pithos.meta-test
  (:require [clojure.test    :refer :all]
            [io.pithos.util  :refer [inc-prefix]]
            [io.pithos.meta  :refer [filter-keys filter-prefixes get-prefixes]]))


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
                     10
                     nil
                     false

                     "simple list with prefix"
                     [{:object "foo/bar.txt"}
                      {:object "foo/baz.txt"}
                      {:object "batman/foo.txt"}]
                     "foo/"
                     "/"
                     #{}
                     [{:object "foo/bar.txt"}
                      {:object "foo/baz.txt"}]
                     10
                     nil
                     false

                     "with prefix but no delimiter"
                     [{:object "foo-bar.txt"}
                      {:object "foo-baz.txt"}
                      {:object "batman-foo.txt"}]
                     "foo-"
                     nil
                     #{}
                     [{:object "foo-bar.txt"}
                      {:object "foo-baz.txt"}]
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
                              (filter-keys objects prefix delimiter)))))))))


(defn make-fetcher
  "Provide a simulation of cassandra's wide row storage for testing
   Alternate store implementations will need to provide the same properties"
  [input]
  (fn [prefix marker limit]
    (let [>pred   #(or (= (:object %) (or marker prefix))
                       (not (.startsWith (or (:object %) "")
                                         (or marker prefix ""))))
          <pred   #(or (empty? prefix)
                       (neg? (compare (:object %) (inc-prefix prefix))))]
      (->> input
           (sort-by :object)
           (drop-while >pred)
           (take-while <pred)
           (take limit)))))


(deftest get-prefixes-test

  (let [in-and-outs ["simple list"
                     {:max-keys 10
                      :delimiter "/"}
                     [{:object "foo/bar.txt"}
                      {:object "foo/baz.txt"}]
                     {:keys []
                      :prefixes #{"foo/"}
                      :truncated? false}

                     "truncated list"
                     {:max-keys 1}
                     [{:object "foo/bar.txt"}
                      {:object "foo/baz.txt"}]
                     {:keys [{:object "foo/bar.txt"}]
                      :prefixes #{}
                      :truncated? true}]]
    (doseq [[nickname params objects output] (partition 4 in-and-outs)]
      (testing (str "valid get-prefixes output for " nickname)
        (let [fetcher (make-fetcher objects)]
          (is (= (get-prefixes fetcher params) output )))))))
