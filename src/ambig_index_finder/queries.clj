(ns ambig-index-finder.queries
  (:require [clojure.java.jdbc :as j]
            [clojure.data.json :as json]))

(def pg-db {:classname "org.postgresql.Driver"
            :subprotocol "postgresql"
            :subname "//localhost:5432/imdb"})

(def query-dir "resources/job")

(defn resample-with! [n]
  (letfn [(exec! [& s]
                 (println (apply str s))
                 (j/execute! pg-db [(apply str s)]))]
    (exec! "DELETE FROM pg_statistic;")
    (exec! "SET default_statistics_target TO " n ";")
    (exec! "ANALYZE;")))

(defn read-query [query-id]
  (slurp (str query-dir "/" query-id ".sql")))

(defn explain-query [id]
  (->> id
       (read-query)
       (str "EXPLAIN (ANALYZE TRUE, FORMAT JSON, VERBOSE TRUE) ")
       (vector)
       (j/query pg-db)
       (first)
       ((keyword "query plan"))
       (.getValue)
       (json/read-str)))

(defn sample-and-query [id n]
  (do
    (resample-with! n)
    (println (json/write-str (explain-query id)))
    (println (read-query id))
    (println (j/query pg-db ["SELECT n_distinct, null_frac, correlation FROM pg_stats;"]))))

(defn compare-query [id n1 n2]
  (do
    (sample-and-query id n1)
    (sample-and-query id n2)))
