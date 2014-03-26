(defproject io.pithos/pithos "0.1.0"
  :description "cassandra-backed object storage"
  :url "https://github.com/exoscale/pithos"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main io.pithos
  :jvm-opts ["-Xmx2g"]
  :profiles {:dev {:resource-paths ["test/data"]}}
  :dependencies [[org.clojure/clojure       "1.6.0"]
                 [org.clojure/data.codec    "0.1.0"]
                 [org.clojure/data.xml      "0.0.7"]
                 [org.clojure/data.zip      "0.1.1"]
                 [org.clojure/tools.cli     "0.2.4"]
                 [commons-logging/commons-logging "1.1.3"]
                 [org.spootnik/aleph        "0.3.3-fix-content-length-r1"]
                 [clj-yaml                  "0.4.0"]
                 [clout                     "1.1.0"]
                 [ring/ring-codec           "1.0.0"]
                 [cc.qbits/alia             "2.0.0-beta10"]
                 [org.slf4j/slf4j-log4j12   "1.6.4"]
                 [log4j/apache-log4j-extras "1.0"]
                 [log4j/log4j               "1.2.16"
                  :exclusions [javax.mail/mail
                               javax.jms/jms
                               com.sun.jdmk/jmxtools
                               com.sun.jmx/jmxri]]])
