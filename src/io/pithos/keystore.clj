(ns io.pithos.keystore)

(defprotocol Keystore
  (fetch [this id]))

(defrecord MapKeystore [keys]
  Keystore
  (fetch [this id]
    (get keys (keyword id))))
