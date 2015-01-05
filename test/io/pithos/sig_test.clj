(ns io.pithos.sig-test
  (:require [clojure.test   :refer :all]
            [clojure.string :refer [join]]
            [io.pithos.sig  :refer [string-to-sign]]))

(deftest string-to-sign-test
  (testing "signature with only Date header"
    (is (= (string-to-sign {:headers {"date" "Thu, 17 Nov 2005 18:49:58 GMT"}
                            :request-method "GET"
                            :sign-uri "/bucket/batman"
                            :params {}})
           (join "\n" ["GET"
                       ""
                       ""
                       "Thu, 17 Nov 2005 18:49:58 GMT"
                       "/bucket/batman"]))))

  (testing "signature with custom x-amz-* headers"
    (is (= (string-to-sign {:headers {"date" "Thu, 17 Nov 2005 18:49:58 GMT"
                                      "x-amz-meta-magic" "magic string"
                                      "x-amz-magic" "batman"}
                            :request-method "GET"
                            :sign-uri "/bucket/batman"
                            :params {}})
           (join "\n" ["GET"
                       ""
                       ""
                       "Thu, 17 Nov 2005 18:49:58 GMT"
                       "x-amz-magic:batman"
                       "x-amz-meta-magic:magic string"
                       "/bucket/batman"]))))

    (testing "signature with non x-amz-headers"
    (is (= (string-to-sign {:headers {"date" "Thu, 17 Nov 2005 18:49:58 GMT"
                                      "x-noamz-meta-magic" "magic string"
                                      "x-noamz-magic" "batman"}
                            :request-method "GET"
                            :sign-uri "/bucket/batman"
                            :params {}})
           (join "\n" ["GET"
                       ""
                       ""
                       "Thu, 17 Nov 2005 18:49:58 GMT"
                       "/bucket/batman"]))))

  (testing "signature with both Content-{Md5,Type} headers"
    (is (= (string-to-sign {:headers {"date" "Thu, 17 Nov 2005 18:49:58 GMT"
                                      "content-md5" "c8fdb181845a4ca6b8fec737b3581d76"
                                      "content-type" "text/html"}
                            :request-method "GET"
                            :sign-uri "/bucket/batman"
                            :params {}})
           (join "\n" ["GET"
                       "c8fdb181845a4ca6b8fec737b3581d76"
                       "text/html"
                       "Thu, 17 Nov 2005 18:49:58 GMT"
                       "/bucket/batman"]))))

  (testing "signature for GET and x-amz-date header"
    (is (= (string-to-sign {:headers {"date" "Thu, 17 Nov 2005 18:49:58 GMT"
                                      "x-amz-date" "Thu, 17 Nov 2005 18:49:20 GMT"}
                            :request-method "GET"
                            :sign-uri "/bucket/batman"
                            :params {}})
           (join "\n" ["GET"
                       ""
                       ""
                       ""
                       "x-amz-date:Thu, 17 Nov 2005 18:49:20 GMT"
                       "/bucket/batman"]))))

  (testing "signature with query string"
    (is (= (string-to-sign {:headers {"date" "Thu, 17 Nov 2005 18:49:58 GMT"}
                            :request-method "GET"
                            :sign-uri "/bucket/batman"
                            :params {:expires "1141889120"}})
           (join "\n" ["GET"
                       ""
                       ""
                       "1141889120"
                       "/bucket/batman"])))))
