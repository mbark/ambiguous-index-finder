(ns ambig-index-finder.analyzer)

(defn distinct-by
    "Returns a lazy sequence of the elements of coll, removing any elements that return duplicate values when passed to a function f."
  [f coll]
  (let [step (fn step [xs seen]
               (lazy-seq
                ((fn [[x :as xs] seen]
                   (when-let [s (seq xs)]
                     (let [fx (f x)]
                       (if (contains? seen fx)
                         (recur (rest s) seen)
                         (cons x (step (rest s) (conj seen fx)))))))
                 xs seen)))]
    (step coll #{})))

(defn db->index-access-identifier [db]
  (cond
    (= db :postgresql) "Index Name"
    (= db :mariadb) "key"))

(defn analyze-plan [db plan]
  (zipmap
   (keys plan)
   (map
    (fn [accesses]
      (distinct-by
       #(get % (db->index-access-identifier db))
       accesses))
    (vals plan))))
