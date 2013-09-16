(defproject ch.exoscale/ostore "0.1.0"
  :description "cassandra-backed object storage"
  :url "https://github.com/exoscale/ostore"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :aot [ch.exoscale.ostore]
  :main ch.exoscale.ostore.main
  :dependencies [[org.clojure/clojure     "1.5.1"]
                 [org.clojure/data.codec  "0.1.0"]
                 [org.clojure/data.xml    "0.0.7"]
                 [compojure               "1.1.5"]
                 [ring/ring-core          "1.2.0"]
                 [ring/ring-jetty-adapter "1.2.0"]
                 [ring/ring-json          "0.2.0"]
                 [cc.qbits/alia           "1.8.2"]])


