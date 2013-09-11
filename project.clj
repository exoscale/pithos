(defproject ch.exoscale/ostore "0.1.0"
  :description "cassandra-backed object storage"
  :url "https://github.com/exoscale/ostore"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main ch.exoscale.ostore.query
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/data.codec "0.1.0"]
                 [org.clojure/data.zip "0.1.1"]
                 [clj-http            "0.7.6"]
                 [cc.qbits/alia       "1.8.0-beta3"]])


