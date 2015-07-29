(defproject io.pithos/pithos "0.7.5-SNAPSHOT"
  :description "cassandra-backed object storage"
  :maintainer {:email "Pierre-Yves Ritschard <pyr@spootnik.org>"}
  :url "http://pithos.io"
  :license {:name "Apache License, Version 2.0"
            :url " http://www.apache.org/licenses/LICENSE-2.0"}
  :aot :all
  :main io.pithos
  :jvm-opts ["-Xmx2g"]
  :profiles {:dev {:resource-paths ["test/data"]}}
  :plugins [[lein-sub      "0.3.0"]
            [lein-difftest "2.0.0"]
            [lein-ancient  "0.6.7"]]
  :sub ["pithos-quickstart"]
  :dependencies [[org.clojure/clojure           "1.7.0-beta3"]
                 [org.clojure/data.codec        "0.1.0"]
                 [org.clojure/data.xml          "0.0.8"]
                 [org.clojure/data.zip          "0.1.1"]
                 [org.clojure/tools.cli         "0.3.1"]
                 [org.clojure/tools.logging     "0.3.1"]
                 [org.clojure/core.async        "0.1.346.0-17112a-alpha"]
                 [spootnik/unilog               "0.7.4"]
                 [spootnik/constance            "0.5.3"]
                 [clj-yaml                      "0.4.0"]
                 [clout                         "2.1.2"]
                 [cheshire                      "5.4.0"]
                 [clj-time                      "0.9.0"]
                 [ring/ring-core                "1.3.2"]
                 [ring/ring-codec               "1.0.0"]
                 [cc.qbits/alia                 "2.7.2"]
                 [cc.qbits/jet                  "0.6.2"]
                 [net.jpountz.lz4/lz4           "1.3"]
                 [org.xerial.snappy/snappy-java "1.1.1.7"]])
