(ns io.pithos.util
  "A few utility functions, used in several places"
  (:import [java.io          PipedInputStream PipedOutputStream]
           [java.lang        Math]
           [org.joda.time    DateTimeZone])
  (:require [clojure.string  :as s]
            [clojure.string  :refer [lower-case]]
            [clj-time.core   :refer [now]]
            [clj-time.format :refer [formatters parse unparse formatter]]))

(defn uri-decode
  [s]
  (when s
    (java.net.URLDecoder/decode s "UTF-8")))

(defn md5-init
  "Yield an MD5 MessageDigest instance"
  []
  (doto (java.security.MessageDigest/getInstance "MD5") (.reset)))

(defn md5-update
  "Add data from byte-array in a MessageDigest instance"
  [hash ba from to]
  (locking hash
    (doto hash
      (.update ba from to))))

(defn md5-sum
  "Yield a padded hex string of an MD5 digest"
  [hash]
  (let [digest (.toString (java.math.BigInteger. 1 (.digest hash)) 16)
        pad    (apply str (repeat (- 32 (count digest)) "0"))]
    (str pad digest)))

(defn inc-prefix
  "Given an object path, yield the next semantic one."
  [p]
  (when (seq p)
    (let [[c & s] (reverse p)
          reversed (conj s (-> c int inc char))]
      (apply str (reverse reversed)))))

(def byte-factors
  "1024 factor of corresponding storage unit"
  {"k" 1 "m" 2 "g" 3 "t" 4 "p" 5})

(def byte-pattern
  "Regular expression pattern for data size"
  #"([0-9]+)(([kKmMgGtTpP])[bB]?)?")

(defn to-bytes
  "Parse an input string into a byte amount, the input
   string can be suffixed by a unit specifier"
  [input & [param]]
  (when input
    (if-let [[_ amount _ factor] (re-find byte-pattern (str input))]
      (long
       (* (Long/parseLong amount)
          (if factor
            (Math/pow 1024 (get byte-factors (lower-case factor)))
            1)))
      (throw (ex-info (format "invalid byte amount [%s]: %s"
                              (or param "") input) {})))))


(defn piped-input-stream
  "yield two interconnected PipedInputStream and PipedOutputStream"
  []
  (let [os  (PipedOutputStream.)
        is  (PipedInputStream. os)]
    [is os]))

(defn parse-uuid
  "Parse the string representation of a uuid"
  [s]
  (java.util.UUID/fromString s))

(def gmt
  "The GMT timezone, only fetched once"
  (DateTimeZone/forID "GMT"))

(def rfc822-format
  (formatter "EEE, dd MMM yyyy HH:mm:ss" gmt))

(defn date->rfc822
  [d]
  (str (unparse rfc822-format d) " GMT"))

(defn iso8601->date
  [isodate]
  (parse (:date-time-parser formatters) isodate))

(defn iso8601->rfc822
  "RFC822 representation based on an iso8601 timestamp"
  [isodate]
  (->> (parse (:date-time-parser formatters) isodate)
       (date->rfc822)))

(defn iso8601
  "iso8601 timestamp representation"
  [date]
  (unparse (:date-time formatters) date))

(defn iso8601-timestamp
  "String representation of the current timestamp in UTC"
  []
  (iso8601 (now)))

(def ^:private regex-char-esc-smap
  "Characters to be escaped in a regular pattern (including inside a set)"
  ;; The documentation is available here:a
  ;;  https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html
  (let [esc-chars "[]{}()<>*+^$?|\\.&-!#"]
    (zipmap esc-chars
            (map (partial str "\\") esc-chars))))
(defn string->pattern
  "Escape a string to be used as a regular pattern"
  [string]
  (->> string
       (replace regex-char-esc-smap)
       (reduce str "")))

(defn interpol
  [s args]
  (let [trimk (fn [s] (keyword (.substring s 2 (dec (.length s)))))]
    (s/replace s #"\$\{[^}]*\}" (fn [k] (get args (trimk k) "")))))


(defmacro cond-with
  "Takes a symbol and a set of test/expr pairs. It evaluates
   each test one at a time. If a test returns logical true,
   cond-with evaluates the corresponding expr, binding the
   symbol to the test's return. The return value of the expr
   is returned and no more tests are evaluated. If no test/expr
   pairs are present, nil is returned. An odd number of clauses
   will throw an exception."
  [sym & clauses]
  (when clauses
    (list 'if-let [`~sym (first clauses)]
          (if (next clauses)
            (second clauses)
            (throw (IllegalArgumentException.
                    "cond-with requires an even number of forms.")))
          (cons `cond-with (conj  (next (next clauses)) `~sym)))))

(defmacro cond-let
  "Takes a symbol and a set of test/expr pairs. Tests may be
   expressions or binding vectors. If a test returns logical true,
   cond-let evaluates the corresponding, if a binding vector was
   provided, the expr will be evaluated within that context. The
   return value of the expr is returned and no more tests are
   evaluated. If no test/expr paris are present, nil is returned.
   An odd number of clauses will throw an exception."
  [& clauses]
  (when clauses
    (list (if (vector? (first clauses)) 'if-let 'if)
          (first clauses)
          (if (next clauses)
            (second clauses)
            (throw (IllegalArgumentException.
                    "cond-let requires an even number of forms.")))
          (cons `cond-let (next (next clauses))))))
