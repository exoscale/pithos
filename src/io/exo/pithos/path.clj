(ns io.exo.pithos.path
  (:require [qbits.alia          :refer [execute]]
            [qbits.hayt          :refer [select where set-columns columns
                                         delete update limit order-by]]
            [io.exo.pithos.inode :as inode]))

(defn inc-path
  "Given a path, yield the next semantic one."
  [p]
  (let [[c & s] (reverse p)
        reversed (conj s (-> c int inc char))]
    (apply str (reverse reversed))))

(defn fetch-path-q
  [bucket prefix]
  (let [path-def    [[:bucket bucket]]
        next-prefix (inc-path prefix)
        prefix-def  [[:path [:>= prefix]] [:path [:< next-prefix]]]]
    (select :path (where (cond-> path-def
                                 (seq prefix) (concat prefix-def))))))

(defn get-path-q
  [bucket path]
  (select :path 
          (where {:bucket bucket :path path})
          (limit 1)))

(defn update-path-q
  [bucket path columns]
  (update :path
          (set-columns columns)
          (where {:bucket bucket :path path})))

(defn delete-path-q
  [bucket path]
  (delete :path (where {:bucket bucket :path path})))

(defn published-versions-q
  [inode]
  (select :inode
          (columns :version)
          (where {:inode inode
                  :published true})
          (order-by [:version :desc])
          (limit 1)))

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

(defn published-path?
  [inode]
  (not (nil? (first (execute (published-versions-q inode))))))

(defn fetch
  ([bucket]
     (fetch bucket {}))
  ([bucket {:keys [path prefix delimiter max-keys hidden]}]
     (if path
       (first (execute (get-path-q bucket path)))
       (let [raw-paths (execute (fetch-path-q bucket prefix))
             paths     (if hidden raw-paths (filter published-path? raw-paths))
             prefixes  (if delimiter 
                         (filter-prefixes paths prefix delimiter)
                         #{})
             contents  (if delimiter
                         (filter-content paths prefix delimiter) 
                         paths)]
         [(remove prefixes contents) prefixes]))))

(defn update!
  [bucket path columns]
  (execute (update-path-q bucket path columns)))

(defn delete!
  [bucket path]
  (execute (delete-path-q bucket path)))
