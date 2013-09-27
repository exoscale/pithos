(ns io.exo.pithos.main
  (:gen-class)
  (:require [io.exo.pithos.api    :as api]
            [io.exo.pithos.config :as config]))

(defn -main 
  [& [path]]
  (-> path config/init api/run)
  nil)

