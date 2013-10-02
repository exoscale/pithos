(ns io.pithos.object
  (:require [qbits.alia      :refer [execute]]
            [qbits.hayt      :refer [select where set-columns columns
                                     delete update limit order-by]]
            [io.pithos.inode :as inode]))

(defn inc-prefix
  "Given an object path, yield the next semantic one."
  [p]
  (let [[c & s] (reverse p)
        reversed (conj s (-> c int inc char))]
    (apply str (reverse reversed))))

(defn fetch-object-q
  [bucket prefix]
  (let [object-def    [[:bucket bucket]]
        next-prefix (inc-prefix prefix)
        prefix-def  [[:object [:>= prefix]] [:object [:< next-prefix]]]]
    (select :object (where (cond-> object-def
                                 (seq prefix) (concat prefix-def))))))

(defn get-object-q
  [bucket object]
  (select :object 
          (where {:bucket bucket :object object})
          (limit 1)))

(defn update-object-q
  [bucket object columns]
  (update :object
          (set-columns columns)
          (where {:bucket bucket :object object})))

(defn delete-object-q
  [bucket object]
  (delete :object (where {:bucket bucket :object object})))

(defn published-versions-q
  [inode]
  (select :inode
          (columns :version)
          (where {:inode inode
                  :published true})
          (order-by [:version :desc])
          (limit 1)))

(defn filter-content
  [objects prefix delimiter]
  (let [pat (re-pattern (str "^" prefix "[^\\" delimiter "]+$"))]
    (filter (comp (partial re-find pat) :object) objects)))

(defn filter-prefixes
  [objects prefix delimiter]
  (let [pat (re-pattern
             (str "^(" prefix "[^\\" delimiter "]+\\" delimiter ").*$"))]
    (->> (map (comp second (partial re-find pat) :object) objects)
         (remove nil?)
         (set))))

(defn published-object?
  [inode]
  (not (nil? (first (execute (published-versions-q inode))))))

(defn fetch
  ([bucket]
     (fetch bucket {}))
  ([bucket {:keys [object prefix delimiter max-keys hidden]}]
     (if object
       (first (execute (get-object-q bucket object)))
       (let [raw-prefixes (execute (fetch-object-q bucket prefix))
             objects      (if hidden
                            raw-prefixes
                            (filter published-object? raw-prefixes))
             prefixes     (if delimiter
                            (filter-prefixes objects prefix delimiter)
                            #{})
             contents     (if delimiter
                            (filter-content objects prefix delimiter)
                            objects)]
         [(remove prefixes contents) prefixes]))))

(defn update!
  [bucket object columns]
  (execute (update-object-q bucket object columns)))

(defn delete!
  [bucket object]
  (execute (delete-object-q bucket object)))
