(ns ambig-index-finder.generator
  (:require [clojure.data.json :as json]
            [clojure.java.jdbc :as j]
            [environ.core :as environ]
            [clojure.string :refer [join trim]]
            [clojure.walk :refer [postwalk prewalk]]
            [clojure.tools.logging :as log]))

(def  db-specs (load-string (slurp (environ/env :db-config-file))))
(def query-dir "resources/queries")

(defn- read-analyze-query [query-id]
  (trim (slurp (str query-dir "/" query-id "-analyze.sql"))))

(defn- resample-mariadb-statements [id n]
  ;; Just to be sure, lets set it to off each time
  ["SET GLOBAL innodb_stats_persistent='OFF';"
   "SET GLOBAL innodb_stats_auto_recalc='OFF';"
   (str "SET GLOBAL innodb_stats_transient_sample_pages=" n ";")
   (read-analyze-query id)])

(defn- resample-postgres-statements [id n]
  ["DELETE FROM pg_statistic;"
   (str "SET default_statistics_target TO " n ";")
   (read-analyze-query id)])

(defn- resample-with! [id db n]
  (let [exec! (fn [s]
                (do
                  (log/debug "Executing non-select query" s)
                  (j/execute! (db db-specs) [s])))
        stmts (cond
                (= db :mariadb) (resample-mariadb-statements id n)
                (= db :postgresql) (resample-postgres-statements id n))]
        (dorun (map exec! stmts))))

(defn- read-query [query-id]
  (trim (slurp (str query-dir "/" query-id ".sql"))))

(defn- execute-query [db query]
  (log/debug "Executing select-query" query)
  (j/query (db db-specs) query))

(defn- format-query-result [db res]
  (cond
    (= db :mariadb) {:access-methods res}
    (= db :postgresql)
    (->> res
         (first)
         ((keyword "query plan"))
         (.getValue)
         (json/read-str))))

(defn- explain-query [db id]
  (->> id
       (read-query)
       (str (cond
              (= db :mariadb) "EXPLAIN "
              (= db :postgresql) "EXPLAIN (ANALYZE FALSE, FORMAT JSON) "))
       (vector)
       (execute-query db)
       (format-query-result db)))

(defn- sample-and-query [db id n]
  (delay
    (resample-with! id db n)
    (explain-query db id)))

(defn- repeat-query [db id sample-size repetitions]
  (repeatedly
   repetitions
   #(sample-and-query db id sample-size)))

(defn- plans-for-query [db query-id sample-sizes repetitions]
  (map #(repeat-query db query-id % repetitions) sample-sizes))

(defn generate-plans [opts]
  (plans-for-query (:database opts)
                   (:query opts)
                   (:samplesizes opts)
                   (:repetitions opts)))
