(ns io.pithos.keystore
  "A keystore is a simple protocol which yields a map
   of tenant details for a key id.

   The basic implementation wants keys from the configuration
   file, you'll likely want to use a custom implementation that
   interacts with your user-base here.
")

(defprotocol Keystore
  "Single function protocol, fetch retrieves information for an ID"
  (fetch [this id]))

;;
;; A simple record holding onto a map of keys to details.
;; the fetch fn just looks up in its map.
(defrecord MapKeystore [keys]
  Keystore
  (fetch [this id]
    (get keys (keyword id))))
