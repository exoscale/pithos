(ns ch.exoscale.pithos.main
  (:require [ch.exoscale.pithos.http     :as http]
            [ch.exoscale.pithos.keystore :as keystore]
            [qbits.alia                  :as alia])
  (:gen-class))

(defn -main [& args]
  (let [cluster (alia/cluster "localhost")
        store   (alia/connect cluster "storage")]
    (http/run-api keystore/keystore store {})))

