(ns ambig-index-finder.queries
  (:require [clojure.java.jdbc :as j]
            [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [clojure.string :refer [trim]]))

(def query-dir "resources/job")

(defn- resample-with! [db-spec n]
  (letfn [(exec! [& s]
                 (do
                   (log/debug "Executing non-select query" s)
                   (j/execute! db-spec [(apply str s)])))]
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
       (str "EXPLAIN (ANALYZE TRUE, FORMAT JSON, VERBOSE TRUE) ")
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

(defn- repeat-query [db-spec id repetitions sample-size]
  (repeatedly
    repetitions
    #(sample-and-query db-spec id sample-size)))

(defn compare-query [db-spec id repetitions sample-sizes]
  (let [sample-size (first sample-sizes)
        r (rest sample-sizes)
        results (repeat-query db-spec id repetitions sample-size)]
    (if (empty? r)
      [results]
      (cons
        results
        (compare-query db-spec id repetitions r)))))
