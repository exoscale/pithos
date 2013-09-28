(ns io.exo.pithos.util
  (:require [clojure.string :refer [lower-case]]))

(def byte-factors
  {"k" 1 "m" 2 "g" 3 "t" 4 "p" 5})

(def byte-pattern #"([0-9]+)(([kKmMgGtTpP])[bB]?)?")

(defn to-bytes
  "Parse an input string into a byte amount, the input
   string can be suffixed by a unit specifier"
  [input]
  (if-let [[_ amount _ factor] (re-find byte-pattern (str input))]
    (do
      (long
       (* (Long/parseLong amount)
          (if factor 
            (java.lang.Math/pow 1024 (get byte-factors (lower-case factor)))
            1))))
    (throw (ex-info (str "invalid byte amount: " input) {}))))
