(ns ch.exoscale.ostore.keystore)

(def keystore
  {"AKIAIOSFODNN7EXAMPLE"
   {:organization "pyr@spootnik.org"
    :secret       "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}})

(defn get!
  [access-key]
  (get keystore access-key))
