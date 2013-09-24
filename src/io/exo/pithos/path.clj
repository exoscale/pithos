(ns io.exo.pithos.path
  (:import java.util.UUID)
  (:require [qbits.alia :refer [execute]]
            [qbits.hayt :refer [select where set-columns delete update limit]]))

(defn inc-path
  "Given a path, yield the next semantic one."
  [p]
  (let [[c & s] (reverse p)
        reversed (conj s (-> c int inc char))]
    (apply str (reverse reversed))))

(defn fetch-path-q
  [^String tenant ^String bucket ^String prefix]
  (let [path-def    [[:tenant tenant] [:bucket bucket]]
        next-prefix (inc-path prefix)
        prefix-def  [[:path [:>= prefix]] [:path [:< next-prefix]]]]
    (select :path (where (cond-> path-def
                                 (seq prefix) (concat prefix-def))))))

(defn get-path-q
  [^String tenant ^String bucket ^String path]
  (select :path 
          (where {:tenant tenant :bucket bucket :path path})
          (limit 1)))

(defn update-path-q
  [^String tenant ^String bucket ^String path ^UUID inode ^Long version]
  (update :path
          (set-columns {:inode inode :version version})
          (where {:tenant tenant :bucket bucket :path path})))

(defn delete-path-q
  [^String tenant ^String bucket ^String path]
  (delete :path (where {:tenant tenant :bucket bucket :path path})))

(defn filter-content
  [paths prefix delimiter]
  (let [pat (re-pattern (str "^" prefix "[^\\" delimiter "]+$"))]
    (filter (comp (partial re-find pat) :path) paths)))

(defn filter-prefixes
  [paths prefix delimiter]
  (let [pat (re-pattern
             (str "^(" prefix "[^\\" delimiter "]+\\" delimiter ").*$"))]
    (->> (map (comp second (partial re-find pat) :path) paths)
         (remove nil?)
         (set))))

(defn fetch
  ([^String tenant ^String bucket {}]
     (fetch tenant bucket ""))
  ([^String tenant ^String bucket & {:keys [path prefix delimiter max-keys]}]
     (if path
       (first (execute (get-path-q tenant bucket path)))
       (let [paths    (execute (fetch-paths-q tenant bucket prefix))
             prefixes (if delimiter (filter-prefixes paths prefix delimiter) #{})
             contents (if delimiter (filter-content paths prefix delimiter) paths)]
         [(remove prefixes contents) prefixes]))))

(defn update!
  [^String tenant ^String bucket ^String path ^UUID inode ^Long version]
  (execute (update-path-q tenant bucket path inode)))

(defn delete!
  [^String tenant ^String bucket ^String path]
  (execute (delete-path-q tenant bucket path)))
