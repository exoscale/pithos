(ns io.pithos.util
  (:import [java.io PipedInputStream PipedOutputStream])
  (:require [clojure.string :refer [lower-case]]))

(defn md5-init
  []
  (doto (java.security.MessageDigest/getInstance "MD5") (.reset)))

(defn md5-safe
  []
  (agent (md5-init)))

(defn md5-update
  [hash ba from to]
  (doto hash
    (.update ba from to)))

(defn md5-sum
  [hash]
  (.toString (java.math.BigInteger. 1 (.digest hash)) 16))

(defn inc-prefix
  "Given an object path, yield the next semantic one."
  [p]
  (when p
    (let [[c & s] (reverse p)
          reversed (conj s (-> c int inc char))]
      (apply str (reverse reversed)))))

(def byte-factors
  {"k" 1 "m" 2 "g" 3 "t" 4 "p" 5})

(def byte-pattern #"([0-9]+)(([kKmMgGtTpP])[bB]?)?")

(defn to-bytes
  "Parse an input string into a byte amount, the input
   string can be suffixed by a unit specifier"
  [input & [param]]
  (if-let [[_ amount _ factor] (re-find byte-pattern (str input))]
    (do
      (long
       (* (Long/parseLong amount)
          (if factor 
            (java.lang.Math/pow 1024 (get byte-factors (lower-case factor)))
            1))))
    (throw (ex-info (format "invalid byte amount [%s]: %s" 
                            (or param "") input) {}))))


(defn piped-input-stream
  []
  (let [os  (PipedOutputStream.)
        is  (PipedInputStream. os)]
    [is os]))
