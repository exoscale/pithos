(ns io.pithos.system)

(defprotocol SystemDescriptor
  (regions [this])
  (bucketstore [this])
  (keystore [this])
  (service [this])
  (service-uri [this]))

(defn system-descriptor
  [config]
  (reify SystemDescriptor
    (regions [this] (:regions config))
    (bucketstore [this] (:bucketstore config))
    (keystore [this] (:keystore config))
    (service [this] (:service config))
    (service-uri [this] (get-in config [:options :service-uri]))))
