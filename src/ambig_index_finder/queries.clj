(ns ambig-index-finder.queries
  (:require [clojure.java.jdbc :as j]
            [clojure.data.json :as json]))

(def query-dir "resources/job")

(defn- resample-with! [db-spec n]
  (letfn [(exec! [& s] (j/execute! db-spec [(apply str s)]))]
    (exec! "DELETE FROM pg_statistic;")
    (exec! "SET default_statistics_target TO " n ";")
    (exec! "ANALYZE;")))

(defn- read-query [query-id]
  (slurp (str query-dir "/" query-id ".sql")))

(defn- explain-query [db-spec id]
  (->> id
       (read-query)
       (str "EXPLAIN (ANALYZE TRUE, FORMAT JSON, VERBOSE TRUE) ")
       (vector)
       (j/query db-spec)
       (first)
       ((keyword "query plan"))
       (.getValue)
       (json/read-str)))

(defn- sample-and-query [db-spec id n]
  (do
    (resample-with! db-spec n)
    (println (json/write-str (explain-query db-spec id)))
    (println (read-query id))
    (println (j/query db-spec ["SELECT n_distinct, null_frac, correlation FROM pg_stats;"]))))

(defn compare-query [db-spec id n1 n2]
  (do
    (sample-and-query db-spec id n1)
    (sample-and-query db-spec id n2)))
