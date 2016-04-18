(ns ambig-index-finder.generator
  (:require [clojure.data.json :as json]
            [clojure.java.jdbc :as j]
            [environ.core :as environ]
            [clojure.string :refer [join trim]]
            [clojure.tools.logging :as log]))

(def db-specs (load-string (slurp (environ/env :db-config-file))))
(def query-dir "resources/queries")

(defn- read-analyze-query [query-id]
  (trim (slurp (str query-dir "/" query-id "-analyze.sql"))))

(defn- resample-with! [id db-spec n]
  (letfn [(exec! [& s]
            (do
              (log/debug "Executing non-select query" s)
              (j/execute! db-spec [(join s)])))]
    (exec! "DELETE FROM pg_statistic;")
    (exec! "SET default_statistics_target TO " n ";")
    (exec! (read-analyze-query id))))

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
    (resample-with! id db-spec n)
    (explain-query db-spec id)))

(defn- repeat-query [db-spec id sample-size repetitions]
  (repeatedly
   repetitions
   #(sample-and-query db-spec id sample-size)))

(defn- plans-for-query [db-spec query-id [sample-size & r] repetitions]
  (if (nil? sample-size)
    []
    (cons
     (repeat-query db-spec query-id sample-size repetitions)
     (plans-for-query db-spec query-id r repetitions))))

(defn generate-plans [opts]
  (plans-for-query ((:database opts) db-specs)
                   (:query opts)
                   (:samplesizes opts)
                   (:repetitions opts)))
