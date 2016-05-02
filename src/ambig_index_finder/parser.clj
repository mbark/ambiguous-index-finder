(ns ambig-index-finder.parser
  (:require [clojure.walk :refer [postwalk]]))

(def relation-accesses (atom []))

(defn access-key [db]
  (case db
    :postgresql :Alias
    :mariadb :table))

;; Not the best solution, but the easiest in this case
(defn- save-if-relation-access [db-id o]
  (if (and (map? o) (contains? o db-id))
    (swap! relation-accesses conj o))
  o)

(defn- find-relation-accesses [db-id plan]
  (reset! relation-accesses [])
  (postwalk #(save-if-relation-access db-id %) plan)
  @relation-accesses)

(defn- group-by-relation [db-id accesses]
  (group-by
   #(get % db-id)
   accesses))

(defn parse-plan [db plan]
  (let [db-id (access-key db)]
    (group-by-relation db-id (find-relation-accesses db-id plan))))
