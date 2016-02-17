(ns ambig-index-finder.queries
  (:require [clojure.java.jdbc :as j]
            [clojure.data.json :as json]))

(def pg-db {:classname "org.postgresql.Driver"
            :subprotocol "postgresql"
            :subname "//localhost:5432/imdb"})

(def query-dir "resources/job")

(defn resample-with [n]
  (letfn [(exec! [& s] (j/execute! pg-db [(apply str s)]))]
    (exec! "SET default_statistics_target TO " n ";")
    (exec! "ANALYZE;")))

(defn read-query [query-id]
  (slurp (str query-dir "/" query-id".sql")))

(defn explain-query [id]
  (->> id
       (read-query)
       (str "EXPLAIN (FORMAT JSON) ")
       (vector)
       (j/query pg-db)
       (first)
       ((keyword "query plan"))
       (.getValue)
       (json/read-str)))

(resample-with 10)
(explain-query "4c")
