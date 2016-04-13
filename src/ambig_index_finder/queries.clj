(ns ambig-index-finder.queries
  (:require [clojure.data.json :as json]
            [clojure.java.jdbc :as j]
            [clojure.string :refer [join trim]]
            [clojure.tools.logging :as log]))

(def query-dir "resources/queries")

(defn- resample-with! [db-spec n]
  (letfn [(exec! [& s]
                 (do
                   (log/debug "Executing non-select query" s)
                   (j/execute! db-spec [(join s)])))]
    (exec! "DELETE FROM pg_statistic;")
    (exec! "SET default_statistics_target TO " n ";")
    (exec! "ANALYZE;")))

(defn- read-query [query-id]
  (trim (slurp (str query-dir "/" query-id ".sql"))))

(defn- execute-query [db-spec query]
  (do
    (log/debug "Executing select-query" query)
    (j/query db-spec query)))

(defn- explain-query [db-spec id]
  (->> id
       (read-query)
       (str "EXPLAIN (ANALYZE FALSE, FORMAT JSON, VERBOSE TRUE) ")
       (vector)
       (execute-query db-spec)
       (first)
       ((keyword "query plan"))
       (.getValue)
       (json/read-str)))

(defn- sample-and-query [db-spec id n]
  (do
    (resample-with! db-spec n)
    (explain-query db-spec id)))

(defn- repeat-query [db-spec id sample-size repetitions]
  (repeatedly
    repetitions
    #(sample-and-query db-spec id sample-size)))

(defn compare-query [db-spec query-id [sample-size & r] repetitions]
  (if (nil? sample-size)
    []
    (cons
      (repeat-query db-spec query-id sample-size repetitions)
      (compare-query db-spec query-id r repetitions))))

(defn compare-queries [db-spec [query & r] sample-sizes repetitions]
  (if (nil? query)
    []
    (cons
      (compare-query db-spec query sample-sizes repetitions)
      (compare-queries db-spec r sample-sizes repetitions))))
