(ns ambig-index-finder.parser
  (:require [clojure.walk :refer [postwalk]]))

(def index-access-identifier "Alias")
(def relation-accesses (atom []))

;; Not the best solution, but the easiest in this case
(defn- save-if-relation-access [o]
  (if (and (map? o) (contains? o index-access-identifier))
    (swap! relation-accesses conj o))
  o)

(defn- find-relation-accesses [plan]
  (reset! relation-accesses [])
  (postwalk save-if-relation-access plan)
  @relation-accesses)

(defn- group-by-relation [accesses]
  (apply merge-with concat
         (map
           (fn [l] (group-by #(get % index-access-identifier) l))
           accesses)))

(defn- diff-relation-access [access-by-relation]
  (zipmap (keys access-by-relation)
          (map
            (fn [l]
              (distinct (map
                          #(dissoc % "Plan Rows" "Plan Width" "Total Cost")
                          l)))
            (vals access-by-relation))))

(defn parse-plans [plans]
  (let [accesses (map find-relation-accesses plans)
        by-relation (group-by-relation accesses)
        diff (diff-relation-access by-relation)]
    diff))
