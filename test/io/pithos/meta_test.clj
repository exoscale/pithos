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
                              (filter-keys objects prefix delimiter)))))))))

(defn make-fetcher
  [init]
  (let [payload (atom (sort-by :object init))]
    (fn [prefix marker limit]
      (locking payload
        (let [pred    #(not (.startsWith (or (:object %) "")
                                         (or marker prefix "")))
              input   @payload
              skipped (count (take-while pred input))
              res     (->> input (drop-while pred) (take limit))]
          (reset! payload (drop (+ skipped (count res)) input))
          res)))))

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
