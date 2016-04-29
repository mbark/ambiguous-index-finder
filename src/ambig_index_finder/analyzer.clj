(ns ambig-index-finder.analyzer)

(defn- idx-key [db]
  (case db
    :postgresql (keyword "Index Name")
    :mariadb :key))

(defn conj-distinct [f x y]
  (reduce
   (fn [coll v]
     (if (some #(= (f %) (f v)) coll)
       coll
       (conj coll v)))
   x y))

(defn analyze-plans [db next-plan plans-to-read]
  (loop [m {} plans-left plans-to-read]
    (if (zero? plans-left)
      m
      (recur
       (merge-with
        #(conj-distinct (fn [access] (get access (idx-key db)))
                        %1 %2)
        m (next-plan))
       (dec plans-left)))))
