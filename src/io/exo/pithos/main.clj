(ns io.exo.pithos.main
  (:require [io.exo.pithos.http     :as http]
            [io.exo.pithos.keystore :as keystore]
            [qbits.alia             :as alia])
  (:gen-class))

(defn -main [& args]
  (let [cluster (alia/cluster "localhost")
        store   (alia/connect cluster "storage")]
    (http/run-api keystore/keystore store {})))

