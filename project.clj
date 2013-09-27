(defproject io.exo/pithos "0.1.0"
  :description "cassandra-backed object storage"
  :url "https://github.com/exoscale/ostore"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main io.exo.pithos.main
  :dependencies [[org.clojure/clojure     "1.5.1"]
                 [org.clojure/data.codec  "0.1.0"]
                 [org.clojure/data.xml    "0.0.7"]
                 [http-kit                "2.1.10"]
                 [clj-yaml                "0.4.0"]
                 [clout                   "1.1.0"]
                 [ring/ring-codec         "1.0.0"]
                 [cc.qbits/alia           "1.8.2"]])


