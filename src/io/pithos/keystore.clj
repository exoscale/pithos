(ns io.pithos.keystore
  "A keystore is a simple protocol which yields a map
   of tenant details for a key id.

   The basic implementation wants keys from the configuration
   file, you'll likely want to use a custom implementation that
   interacts with your user-base here.
  ")

(defn map-keystore [keys]
  "Wrap a map, translating looked-up keys to keywords."
  (reify
    clojure.lang.ILookup
    (valAt [this id]
      (get keys (keyword id)))))
