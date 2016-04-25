(ns ambig-index-finder.parser
  (:require [clojure.walk :refer [postwalk]]))

(def relation-accesses (atom []))

(defn db->index-access-identifier [db]
  (cond
    (= db :postgresql) "Alias"
    (= db :mariadb) "table"))

;; Not the best solution, but the easiest in this case
(defn- save-if-relation-access [db o]
  (if (and (map? o) (contains? o (db->index-access-identifier db)))
    (swap! relation-accesses conj o))
  o)

(defn- find-relation-accesses [db plan]
  (reset! relation-accesses [])
  (postwalk #(save-if-relation-access db %) plan)
  @relation-accesses)

(defn- group-by-relation [db accesses]
  (apply merge-with concat
         (map
          (fn [l] (group-by #(get % (db->index-access-identifier db)) l))
          accesses)))

(defn parse-plans [db plans]
   (let [accesses (map #(find-relation-accesses db %) plans)
         by-relation (group-by-relation db accesses)]
     by-relation))
