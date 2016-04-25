(ns ambig-index-finder.generator
  (:require [clojure.data.json :as json]
            [clojure.java.jdbc :as j]
            [environ.core :as environ]
            [clojure.string :refer [join trim]]
            [clojure.walk :refer [postwalk prewalk]]
            [clojure.tools.logging :as log]))

(def db-specs (load-string (slurp (environ/env :db-config-file))))
(def db-connection (atom {:connection nil}))
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

(defn- resample-with! [db-con db id n]
  (let [exec! (fn [s]
                (log/debug "Executing non-select query" s)
                (j/execute! db-con [s]))
        stmts (cond
                (= db :mariadb) (resample-mariadb-statements id n)
                (= db :postgresql) (resample-postgres-statements id n))]
        (dorun (map exec! stmts))))

(defn- read-query [query-id]
  (trim (slurp (str query-dir "/" query-id ".sql"))))

(defn- execute-query [db-con params query]
  (log/debug "Executing select-query" query)
  (try (j/query db-con [query params])
       (catch Exception e (str "Caught exception: " (.getMessage e)))))

(defn- format-query-result [db res]
  (cond
    (= db :mariadb) {:access-methods res}
    (= db :postgresql)
    (->> res
         (first)
         ((keyword "query plan"))
         (.getValue)
         (json/read-str))))

(defn- explain-query [db-con db id params]
  (->> id
       (read-query)
       (str (cond
              (= db :mariadb) "EXPLAIN "
              (= db :postgresql) "EXPLAIN (ANALYZE FALSE, FORMAT JSON) "))
       (execute-query db-con params)
       (format-query-result db)))

(defn- sample-and-query [db-con db id n]
  (delay
    (resample-with! db-con db id n)
    (map
     #(explain-query db-con db id %)
     (range 1 1000))))

(defn- repeat-query [db-con db id sample-size repetitions]
  (repeatedly
   repetitions
   #(sample-and-query db-con db id sample-size)))

(defn- plans-for-query [db-con db query-id sample-sizes repetitions]
  (map #(repeat-query db-con db query-id % repetitions) sample-sizes))

(defn generate-plans [opts]
  (swap! db-connection
         (assoc :connection
                (j/get-connection ((:database opts) db-specs))))
  (plans-for-query {:connection @db-connection}
                   (:database opts)
                   (:query opts)
                   (:samplesizes opts)
                   (:repetitions opts)))
