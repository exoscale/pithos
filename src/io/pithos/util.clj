(ns io.pithos.util
  "A few utility functions, used in several places"
  (:import [java.io                PipedInputStream PipedOutputStream]
           [java.util              TimeZone Calendar]
           [java.lang              Math])
  (:require [clojure.string  :refer [lower-case]]
            [clj-time.core   :as time-core]
            [clj-time.format :refer [formatters parse unparse formatter with-zone]]))

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

(def utc
  "The UTC timezone, only fetched once"
  (delay (TimeZone/getTimeZone "UTC")))

(defn iso8601-timestamp
  "String representation of the current timestamp in UTC"
  []
  (javax.xml.bind.DatatypeConverter/printDateTime (Calendar/getInstance @utc)))


(defn ^{:private true} formatter-with-zone
  [s z]
  (with-zone (formatter s) z))

(def ^{:private true} rfc822-interop-format
  "Common used date format, when rfc822 is supposed to be supported"
  (formatter-with-zone "EEE, dd MMM yyyy HH:mm:ss 'GMT'" time-core/utc))

(def ^{:private true} rfc822-obs-military-zones
  "Map of military time zone identifiers to timezone"
  (let [identifiers-upper "YXWVUTSRQPONZABCDEFGHIKLM"
        identifiers-lower (.toLowerCase identifiers-upper java.util.Locale/US)
        identifiers       (concat identifiers-upper identifiers-lower)
        offsets           (range -12 13)
        offset-zones      (map time-core/time-zone-for-offset offsets)]
    (zipmap identifiers (cycle offset-zones))))

(def ^{:private true} rfc822-obs-named-zones
  "Map of RFC822 named time zone identifiers to timezone"
  ;; this handles daylight saving times by trusting the correctness of the given string
  ;; even if the date is given in the wrong tz (e.g. EDT in December)
  {"UT"   time-core/utc
   "GMT"  time-core/utc
   "EST"  (time-core/time-zone-for-offset -5)
   "EDT"  (time-core/time-zone-for-offset -4)
   "CST"  (time-core/time-zone-for-offset -6)
   "CDT"  (time-core/time-zone-for-offset -5)
   "MST"  (time-core/time-zone-for-offset -7)
   "MDT"  (time-core/time-zone-for-offset -6)
   "PST"  (time-core/time-zone-for-offset -8)
   "PDT"  (time-core/time-zone-for-offset -7)})


(def ^{:private true} rfc822-formatters
  "List of date formatters as specified in RFC822 (RFC2822) in order of preference"
  (let [zone-mapping->formatter (fn [[zone offset]] (-> "EEE, dd MMM yyyy HH:mm:ss '%s'"
                                                        (format zone)
                                                        (formatter-with-zone offset)))]
    (concat
      ;; recommended date format of rfc822 successor rfc2822
      [(formatter "EEE, dd MMM yyyy HH:mm:ss Z")]
      [rfc822-interop-format]
      ;; use joda time tz parsing;
      ;; it handles summer time better if EST is given during EDT dates
      ;; this is more lenient than RFC822 as it allows more time zone strings (e.g. UTC)
      [(formatter "EEE, dd MMM yyyy HH:mm:ss z")]
      (->> rfc822-obs-named-zones (map zone-mapping->formatter))
      (->> rfc822-obs-military-zones (map zone-mapping->formatter)))))

(defn ^{:private true} parse-with-formatter-safe
  "Parses string with formatter and returns parse-error or nil"
  ([s formatter] (parse-with-formatter-safe s formatter nil))
  ([s formatter parse-error]
  (try
    (parse formatter s)
    (catch IllegalArgumentException iae
      parse-error))))


(def rfc822-format
  rfc822-interop-format)

(defn rfc822->date
  "Attempts to parse with all rfc822-formatters and returns the first non-nil result or throws an IllegalArgumentException."
  [s]
  (or (some identity (map #(parse-with-formatter-safe s % nil) rfc822-formatters))
      (throw (new IllegalArgumentException (format "Could not parse [ %s ] as RFC822 date" s)))))

(defn iso8601->date
  [isodate]
  (parse (:date-time-parser formatters) isodate))

(defn iso8601->rfc822
  "RFC822 representation based on an iso8601 timestamp"
  [isodate]
  (->> isodate
       (parse (:date-time-parser formatters))
       (unparse rfc822-format)))

(defn iso8601
  "iso8601 timestamp representation"
  [date]
  (unparse (formatter "yyyy-MM-dd'T'HH:mm:ssZ") date))

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
