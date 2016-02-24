(ns ambig-index-finder.core
  (:require [ambig-index-finder.queries :as queries]
            [clojure.tools.logging :as log]
            [environ.core :as environ]
            [clojure.data.json :as json]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.string :refer [split]]))

(def db-specs (load-string
                (slurp
                  (environ/env :db-config-file))))

(defn- repeat-query [db-spec id repetitions sample-size]
  (repeatedly
    repetitions
    #(queries/sample-and-query db-spec id sample-size)))

(defn- compare-query [db-spec id repetitions sample-sizes]
  (let [sample-size (first sample-sizes)
        r (rest sample-sizes)
        results (repeat-query db-spec id repetitions sample-size)
        key-val [(keyword (str "sample-" sample-size)) results]]
    (do
      (log/debug results)
      (if (empty? r)
        (seq [key-val])
        (cons
          key-val
          (compare-query db-spec id repetitions r))))))

(defn evaluate-query [db-name query-id repetitions sample-sizes]
  (do
    (log/info "Evaluating query" query-id "for db" db-name "repeating" repetitions "times, with sample sizes" sample-sizes)
    (let [result (json/write-str
                   (into {} (compare-query (db-name db-specs)
                                           query-id
                                           repetitions
                                           sample-sizes)))]
      (log/info result))))

(def cli-options
  [["-qid" "--queryid QID" "Query id"]
   ["-r" "--repetitions REPETITIONS" "Number of repetitions"
    :parse-fn read-string]
   ["-s" "--samplesizes SAMPLE_SIZES" "Sample sizes"
    :parse-fn #(map read-string (split % #" "))]
   ["-db" "--database DATABASE" "Database"
    :parse-fn keyword]])

(defn -main [& args]
  (let [opts (:options (parse-opts args cli-options))]
    (evaluate-query
      (:database opts)
      (:queryid opts)
      (:repetitions opts)
      (:samplesizes opts))))

; postmaster -D /usr/local/var/postgres
; lein run --queryid=1a --repetitions=2 --samplesizes='1 2' --database=postgresql
