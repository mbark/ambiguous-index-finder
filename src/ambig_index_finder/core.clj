(ns ambig-index-finder.core
  (:require [ambig-index-finder.queries :as queries]
            [clojure.tools.logging :as log]
            [environ.core :as environ]
            [clojure.data.json :as json]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.string :refer [split]]
            [clojure.java.io :as io]))

(def output-file (str "output/execution-" (quot (System/currentTimeMillis) 1000)))
(def output-writer (io/writer output-file))

(def db-specs (load-string (slurp (environ/env :db-config-file))))

(defn save-to-file [s]
  (.write output-writer (str s "\n")))

(defn- repeat-query [db-spec id repetitions sample-size]
  (repeatedly
    repetitions
    #(queries/sample-and-query db-spec id sample-size)))

(defn- compare-query [db-spec id repetitions sample-sizes]
  (let [sample-size (first sample-sizes)
        r (rest sample-sizes)
        results (repeat-query db-spec id repetitions sample-size)]
    (doall (map #(save-to-file (json/write-str %)) results))
    (if (empty? r)
      nil
      (compare-query db-spec id repetitions r))))

(def cli-options
  [["-qid" "--queryid QID" "Query id"]
   ["-r" "--repetitions REPETITIONS" "Number of repetitions"
    :parse-fn read-string]
   ["-s" "--samplesizes SAMPLE_SIZES" "Sample sizes"
    :parse-fn #(map read-string (split % #" "))]
   ["-db" "--database DATABASE" "Database"
    :parse-fn keyword]])

(defn -main [& args]
  (try
    (let [opts (:options (parse-opts args cli-options))]
      (log/info "Executing with parameters:" (json/write-str opts))
      (log/info "The results of this execution are saved in" output-file)
      (save-to-file (json/write-str opts))
      (compare-query
        ((:database opts) db-specs)
        (:queryid opts)
        (:repetitions opts)
        (:samplesizes opts))
      (log/info "Execution finished"))
    (finally (.close output-writer))))

; postmaster -D /usr/local/var/postgres
; lein run --queryid=1a --repetitions=2 --samplesizes='1 2' --database=postgresql
