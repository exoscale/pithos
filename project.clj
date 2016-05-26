(defproject io.pithos/pithos "0.7.6-SNAPSHOT"
  :description "cassandra-backed object storage"
  :maintainer {:email "Pierre-Yves Ritschard <pyr@spootnik.org>"}
  :url "http://pithos.io"
  :license {:name "Apache License, Version 2.0"
            :url  "http://www.apache.org/licenses/LICENSE-2.0"}
  :aot :all
  :main io.pithos
  :jvm-opts ["-Xmx2g"]
  :profiles {:dev {:resource-paths ["test/data"]}}
  :plugins [[lein-difftest "2.0.0"]
            [lein-rpm "0.0.5"]
            [lein-ancient  "0.6.7"]]
  :dependencies [[org.clojure/clojure           "1.8.0"]
                 [org.clojure/data.codec        "0.1.0"]
                 [org.clojure/data.xml          "0.0.8"]
                 [org.clojure/data.zip          "0.1.1"]
                 [org.clojure/tools.cli         "0.3.5"]
                 [org.clojure/tools.logging     "0.3.1"]
                 [org.clojure/core.async        "0.2.374"]
                 [spootnik/unilog               "0.7.13"]
                 [spootnik/constance            "0.5.3"]
                 [spootnik/raven                "0.1.1"]
                 [spootnik/uncaught             "0.5.3"]
                 [clj-yaml                      "0.4.0"]
                 [clout                         "2.1.2"]
                 [cheshire                      "5.5.0"]
                 [clj-time                      "0.9.0"]
                 [ring/ring-core                "1.3.2"]
                 [ring/ring-codec               "1.0.0"]
                 [cc.qbits/alia                 "3.1.8"]
                 [cc.qbits/hayt                 "3.0.1"]
                 [cc.qbits/jet                  "0.6.2"]
                 [net.jpountz.lz4/lz4           "1.3.0"]
                 [org.xerial.snappy/snappy-java "1.1.2.4"]])
