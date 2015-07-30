(ns io.pithos.system)

(defprotocol SystemDescriptor
  (regions [this])
  (bucketstore [this])
  (keystore [this])
  (reporters [this])
  (service [this])
  (service-uri [this]))

(defn system-descriptor
  [config]
  (reify
    SystemDescriptor
    (regions [this] (:regions config))
    (bucketstore [this] (:bucketstore config))
    (keystore [this] (:keystore config))
    (reporters [this] (:reporters config))
    (service [this] (:service config))
    (service-uri [this] (get-in config [:options :service-uri]))
    clojure.lang.ILookup
    (valAt [this k] (get config k))
    (valAt [this k default] (or (get config k) default))))
