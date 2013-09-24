(ns io.exo.pithos.util)

(defn uuid
  []
  (java.util.UUID/randomUUID))

(defn uuid?
  [u]
  (instance? java.util.UUID u))

(defn non-empty-string?
  [s]
  (and (seq s) (string? s)))

(defn non-empty-strings?
  [& slist]
  (every? non-empty-string? slist))
