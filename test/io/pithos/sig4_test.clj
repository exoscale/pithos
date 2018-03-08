(ns io.pithos.sig4-test
  (:require [clojure.test   :refer :all]
            [clojure.string :refer [join]]
            [io.pithos.sig4  :refer [canonical-request is-signed-by? parse-authorization request-time string-to-sign signature signing-key sha256-input-stream]])
  (:import  [java.io ByteArrayInputStream]))

(deftest sig4-canonical-request
  (testing "get-utf8"
    (is (= (canonical-request {:headers {"x-amz-date" "20150830T123600Z"
                                         "x-amz-content-sha256" "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                                         "host" "example.amazonaws.com"
                                         "authorization" "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=8318018e0b0f223aa2bbf98705b62bb787dc9c0e678f255a891fd03141be5d85"}
                               :request-method "GET"
                               :orig-uri "/ሴ"
                               :query-string ""} ["x-amz-date", "host"])

            (join "\n" ["GET"
                        "/%E1%88%B4"
                        ""
                        "host:example.amazonaws.com"
                        "x-amz-date:20150830T123600Z"
                        ""
                        "host;x-amz-date"
                        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"])))
  )
  (testing "get-vanilla"
    (is (= (canonical-request {:headers {"x-amz-date" "20150830T123600Z"
                                         "x-amz-content-sha256" "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                                         "host" "example.amazonaws.com"
                                         "authorization" "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"}
                               :request-method "GET"
                               :orig-uri "/"
                               :query-string ""} ["x-amz-date", "host"])

            (join "\n" ["GET"
                        "/"
                        ""
                        "host:example.amazonaws.com"
                        "x-amz-date:20150830T123600Z"
                        ""
                        "host;x-amz-date"
                        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"])))
  )
  (testing "get-vanilla-empty-query-key"
    (is (= (canonical-request {:headers {"x-amz-date" "20150830T123600Z"
                                         "x-amz-content-sha256" "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                                         "host" "example.amazonaws.com"
                                         "authorization" "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=a67d582fa61cc504c4bae71f336f98b97f1ea3c7a6bfe1b6e45aec72011b9aeb"}
                               :request-method "GET"
                               :orig-uri "/"
                               :query-string "Param1=value1"} ["x-amz-date", "host"])

            (join "\n" ["GET"
                        "/"
                        "Param1=value1"
                        "host:example.amazonaws.com"
                        "x-amz-date:20150830T123600Z"
                        ""
                        "host;x-amz-date"
                        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"])))
  )
  (testing "get-vanilla-query-order-key-case"
    (is (= (canonical-request {:headers {"x-amz-date" "20150830T123600Z"
                                         "x-amz-content-sha256" "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                                         "host" "example.amazonaws.com"
                                         "authorization" "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=b97d918cfa904a5beff61c982a1b6f458b799221646efd99d3219ec94cdf2500"}
                               :request-method "GET"
                               :orig-uri "/"
                               :query-string "Param2=value2&Param1=value1"} ["x-amz-date", "host"])

            (join "\n" ["GET"
                        "/"
                        "Param1=value1&Param2=value2"
                        "host:example.amazonaws.com"
                        "x-amz-date:20150830T123600Z"
                        ""
                        "host;x-amz-date"
                        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"])))
  )
  (testing "get-vanilla-query-unreserved"
    (is (= (canonical-request {:headers {"x-amz-date" "20150830T123600Z"
                                         "x-amz-content-sha256" "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                                         "host" "example.amazonaws.com"
                                         "authorization" "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=9c3e54bfcdf0b19771a7f523ee5669cdf59bc7cc0884027167c21bb143a40197"}
                               :request-method "GET"
                               :orig-uri "/"
                               :query-string "-._~0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz=-._~0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"} ["x-amz-date", "host"])

            (join "\n" ["GET"
                        "/"
                        "-._~0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz=-._~0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
                        "host:example.amazonaws.com"
                        "x-amz-date:20150830T123600Z"
                        ""
                        "host;x-amz-date"
                        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"])))
  )
  (testing "get-vanilla-utf8-query"
    (is (= (canonical-request {:headers {"x-amz-date" "20150830T123600Z"
                                         "x-amz-content-sha256" "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                                         "host" "example.amazonaws.com"
                                         "authorization" "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=2cdec8eed098649ff3a119c94853b13c643bcf08f8b0a1d91e12c9027818dd04"}
                               :request-method "GET"
                               :orig-uri "/"
                               :query-string "ሴ=bar"} ["x-amz-date", "host"])

            (join "\n" ["GET"
                        "/"
                        "%E1%88%B4=bar"
                        "host:example.amazonaws.com"
                        "x-amz-date:20150830T123600Z"
                        ""
                        "host;x-amz-date"
                        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"])))
  )
  (testing "post-vanilla"
    (is (= (canonical-request {:headers {"x-amz-date" "20150830T123600Z"
                                         "x-amz-content-sha256" "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                                         "host" "example.amazonaws.com"
                                         "authorization" "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=5da7c1a2acd57cee7505fc6676e4e544621c30862966e37dddb68e92efbe5d6b"}
                               :request-method "POST"
                               :orig-uri "/"
                               :query-string ""} ["x-amz-date", "host"])

            (join "\n" ["POST"
                        "/"
                        ""
                        "host:example.amazonaws.com"
                        "x-amz-date:20150830T123600Z"
                        ""
                        "host;x-amz-date"
                        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"])))
  )
  (testing "regression-dash"
    (is (= (canonical-request {:headers {"x-amz-date" "20150830T123600Z"
                                         "x-amz-content-sha256" "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                                         "host" "example.amazonaws.com"
                                         "authorization" "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=abab"}
                               :request-method "POST"
                               :orig-uri "/cd-1.iso"
                               :query-string ""} ["x-amz-date", "host"])

            (join "\n" ["POST"
                        "/cd-1.iso"
                        ""
                        "host:example.amazonaws.com"
                        "x-amz-date:20150830T123600Z"
                        ""
                        "host;x-amz-date"
                        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"])))
  )
  (testing "regression-unsigned-payload"
    (is (= (canonical-request {:headers {"x-amz-date" "20150830T123600Z"
                                         "x-amz-content-sha256" "UNSIGNED-PAYLOAD"
                                         "host" "example.amazonaws.com"
                                         "authorization" "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=abab"}
                               :request-method "POST"
                               :orig-uri "/"
                               :query-string ""} ["x-amz-content-sha256", "x-amz-date", "host"])

            (join "\n" ["POST"
                        "/"
                        ""
                        "host:example.amazonaws.com"
                        "x-amz-content-sha256:UNSIGNED-PAYLOAD"
                        "x-amz-date:20150830T123600Z"
                        ""
                        "host;x-amz-content-sha256;x-amz-date"
                        "UNSIGNED-PAYLOAD"])))
  )
)

(defn test-string-to-sign [request]
  (let [
    request-time (request-time request)
    authorization (parse-authorization request)]
    (string-to-sign request request-time authorization)))

(deftest sig4-string-to-sign
  (testing "get-vanilla"
    (is (= (test-string-to-sign {:headers {"x-amz-date" "20150830T123600Z"
                                           "x-amz-content-sha256" "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                                           "host" "example.amazonaws.com"
                                           "authorization" "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"}
                                 :request-method "GET"
                                 :orig-uri "/"
                                 :query-string ""})

            (join "\n" ["AWS4-HMAC-SHA256"
                        "20150830T123600Z"
                        "20150830/us-east-1/service/aws4_request"
                        "bb579772317eb040ac9ed261061d46c1f17a8133879d6129b6e1c25292927e63"])))
  )
)

(defn test-signature [request]
  (let [
    request-time (request-time request)
    authorization (parse-authorization request)]
    (signature (signing-key "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY" request-time authorization) (string-to-sign request request-time authorization))))

(deftest sig4-signature
  (testing "get-vanilla"
    (is (= (test-signature {:headers {"x-amz-date" "20150830T123600Z"
                                      "x-amz-content-sha256" "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                                      "host" "example.amazonaws.com"
                                      "authorization" "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"}
                                 :request-method "GET"
                                 :orig-uri "/"
                                 :query-string ""})

            "5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"))
  )
)

(defn test-is-signed-by [request]
  (is-signed-by? request (parse-authorization request) "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"))

(deftest sig4-is-signed-by
  (testing "get-vanilla"
    (is (test-is-signed-by {:headers {"x-amz-date" "20150830T123600Z"
                                      "x-amz-content-sha256" "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                                      "host" "example.amazonaws.com"
                                      "authorization" "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"}
                                 :request-method "GET"
                                 :orig-uri "/"
                                 :query-string ""}))
  )

  (testing "real-captured-request-1"
    ;; This was captured when running 's3cmd mb s3://test' (so the signature was generated by s3cmd/boto)
    (is (test-is-signed-by {:headers {"x-amz-date" "20170806T173430Z"
                                      "x-amz-content-sha256" "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                                      "host" "s3.example.com"
                                      "accept-encoding" "identity"
                                      "content-length" "0"
                                      "authorization" "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20170806/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=fdb4f1256d5c5a204d05d8126249a29d766b45d2c78c0908389714b87d949d20"}
                                 :request-method "GET"
                                 :orig-uri "/test/"
                                 :query-string "location="}))
  )
)

(deftest body-validating-input-stream
  (testing "signature-failure-no-body"
    (print "signature-failure-no-body ")
    (let [stream (sha256-input-stream (ByteArrayInputStream. (.getBytes "")) "5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03") ba (byte-array 1024)]
      (.read stream ba)
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"body signature is incorrect"
                            (.close stream))))
  )

  (testing "empty-body"
    (print "empty-body ")
    (let [stream (sha256-input-stream (ByteArrayInputStream. (.getBytes "")) "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855") ba (byte-array 1024)]
       (is (.read stream ba) -1)
       (.close stream)
    )
  )

  (testing "hello"
    (print "hello ")
    (let [stream (sha256-input-stream (ByteArrayInputStream. (.getBytes "hello")) "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824") ba (byte-array 1024)]
       (is (.read stream ba) 5)
       (.close stream)
       ;; (is (= (take 5 (seq ba)) (seq (.getBytes "hello"))))
    )
  )
)
