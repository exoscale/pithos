(ns ch.exoscale.ostore.shell
  (:require [qbits.alia         :as a]
            [qbits.hayt         :as h]))

(def wd (atom (list)))
(def tree 
  (atom
   {"toto" {:size 488}
    "titi" {:entries {"titi" {:size 512}
                      "tete" {:entries {"foo" {:size 712}}}}}}))

(defn cassandra-uri
  [str & {:keys [ks port] :or {ks "system" port 9160}}]
  (let [u (java.net.URI. str)]
    {:host (.getHost u)
     :ks   (let [s (.replace (.getPath u) "/" "")] (if (empty? s) ks s))
     :port (let [p (.getPort u)] (if (= -1 p) 9160 p))}))

(defn cd
  [path]
  (if (= path "..")
    (swap! wd (partial drop 2))
    (swap! wd conj path :entries)))

(defn ls
  []
  (doseq [[file {:keys [entries size]}] (get-in @tree (reverse @wd))]
    (printf "%s %5d %s\n" 
            (if entries "drwxr-xr-x" "-rw-------")
            (or size 0)
            file)))

(defn pwd
  []
  (println (if (empty? @wd)
             "/"
             (apply str (interleave (repeat "/") (filter string? @wd))))))
