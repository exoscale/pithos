(ns ch.exoscale.pithos)

(defprotocol Common
  (list! [this] [this path])
  (get! [this] [this where])
  (put! [this] [this where])
  (del! [this])
  (bump! [this])
  (sync! [this]))



