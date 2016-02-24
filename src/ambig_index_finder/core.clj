(ns ambig-index-finder.core
  (:require [ambig-index-finder.queries :as queries]
            [clojure.tools.logging :as log]
            [environ.core :as environ]))

(def db-specs (load-string
                (slurp
                  (environ/env :db-config-file))))

(defn- repeat-query [db-spec id repetitions sample-size]
  (do
    (log/debug "Query" id "will be repeated" repetitions "times")
    (repeatedly
      repetitions
      #(queries/sample-and-query db-spec id sample-size))))

(defn- compare-query [db-spec id repetitions sample-sizes]
  (let [sample-size (first sample-sizes)
        r (rest sample-sizes)
        results (repeat-query db-spec id repetitions sample-size)
        key-val [(keyword (str "sample-" sample-size)) results]]
    (do
      (log/debug "Sampling with" sample-size "for query with id" id)
      (log/debug results)
      (if (empty? r)
        (seq [key-val])
        (cons
          key-val
          (compare-query db-spec id repetitions r))))))

(defn evaluate-query [db-name query-id repetitions sample-sizes]
  (do
    (log/info "Evaluating query" query-id "for db" db-name "repeating" repetitions "times, with sample sizes" sample-sizes)
    (into {} (compare-query (db-name db-specs)
                            query-id
                            repetitions
                            sample-sizes))))

(defn -main []
  (let [result (evaluate-query :postgresql "1a" 2 [1 2])]
    (log/debug result)))

; postmaster -D /usr/local/var/postgres
