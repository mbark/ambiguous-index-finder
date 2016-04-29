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
;; the highest value is 347130, maybe switch to that value?
(def param-range (range 1 347130))

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

(defn- resample-with! [options]
  (let [exec! (fn [s]
                (log/debug "Executing non-select query" s)
                (j/execute! (:connection options) [s]))
        stmts (case (:database options)
                :mariadb (resample-mariadb-statements
                          (:query options)
                          (:sample-size options))
                :postgresql (resample-postgres-statements
                             (:query options)
                             (:sample-size options)))]
    (doseq [stmt stmts]
      (exec! stmt))))

(defn- read-query [query-id]
  (trim (slurp (str query-dir "/" query-id ".sql"))))

(defn- execute-query [db-con params query]
  (log/debug "Executing select-query" query)
  (try (j/query db-con [query params])
       (catch Exception e (str "Caught exception: " (.getMessage e)))))

(defn- format-query-result [db res]
  (case db
    :mariadb res
    :postgresql (->> res
                     (first)
                     ((keyword "query plan"))
                     (.getValue)
                     (json/read-str))))

(defn- explain-query [options params]
  (->> (:query options)
       (read-query)
       (str (case (:database options)
              :mariadb "EXPLAIN "
              :postgresql "EXPLAIN (ANALYZE FALSE, FORMAT JSON) "))
       (execute-query (:connection options) params)
       (format-query-result (:database options))))

(defn- sample-and-query [save-plan options]
  (resample-with! options)
  (doseq [param param-range]
    (save-plan (explain-query options param))))

(defn- opts->db-info [opts]
  ((:database opts)
   db-specs))

(defn generate-plans [opts save-plan]
  (j/with-db-connection [db-con (opts->db-info opts)]
    (doseq [sample-size (:samplesizes opts)]
      (dotimes [i (:repetitions opts)]
       (sample-and-query save-plan
                         (assoc
                          opts
                          :sample-size sample-size
                          :connection db-con))))))
