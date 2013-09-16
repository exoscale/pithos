(ns ch.exoscale.ostore.main
  (:require [ch.exoscale.ostore.http     :as http]
            [ch.exoscale.ostore.keystore :as keystore]
            [qbits.alia                  :as alia])
  (:gen-class))

(defn -main [& args]
  (let [cluster (alia/cluster "localhost")
        store   (alia/connect cluster "storage")]
    (http/run-api keystore/keystore store {})))

