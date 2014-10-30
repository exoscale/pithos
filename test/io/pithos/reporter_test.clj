(ns io.pithos.reporter-test
  (:require [clojure.test       :refer :all]
            [io.pithos.reporter :refer [report! report-all! Reporter]]))

(defn atom-reporter
  []
  (let [contents (atom nil)]
    [contents
     (reify Reporter
       (report! [_ e]
         (swap! contents conj e)))]))

(deftest reporter-test

  (let [[contents r] (atom-reporter)]
    (report! r :foo)
    (report! r :bar)
    (report! r :baz)

    (testing "simple inserts"
      (is (= [:baz :bar :foo] @contents)))))
