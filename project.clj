(defproject io.pithos/pithos "0.1.0"
  :description "cassandra-backed object storage"
  :url "https://github.com/exoscale/ostore"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main io.pithos.main
  :jvm-opts ["-Xmx2g"]
  :dependencies [[org.clojure/clojure       "1.5.1"]
                 [org.clojure/data.codec    "0.1.0"]
                 [org.clojure/data.xml      "0.0.7"]
                 [org.clojure/tools.cli     "0.2.4"]
                 [aleph                     "0.3.0"]
                 [clj-yaml                  "0.4.0"]
                 [clout                     "1.1.0"]
                 [ring/ring-codec           "1.0.0"]
                 [cc.qbits/alia             "1.8.2"]
                 [org.slf4j/slf4j-log4j12   "1.6.4"]
                 [log4j/apache-log4j-extras "1.0"]
                 [log4j/log4j               "1.2.16"
                  :exclusions [javax.mail/mail
                               javax.jms/jms
                               com.sun.jdmk/jmxtools
                               com.sun.jmx/jmxri]]])


