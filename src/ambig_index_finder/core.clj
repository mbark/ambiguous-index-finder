(ns ambig-index-finder.core
  (:require [ambig-index-finder.queries :as queries]
            [clojure.tools.logging :as log]
            [environ.core :as environ]
            [clojure.data.json :as json]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.string :refer [split]]
            [clojure.java.io :as io]
            [clj-progress.core :as progress]))

(def output-file (str "output/execution-" (quot (System/currentTimeMillis) 1000)))
(io/make-parents output-file)
(def output-writer (io/writer output-file))

(def db-specs (load-string (slurp (environ/env :db-config-file))))

(def cli-options
  [["-q" "--queries QUERIES" "Queries"
    :parse-fn #(split % #" ")]
   ["-r" "--repetitions REPETITIONS" "Number of repetitions"
    :parse-fn read-string]
   ["-s" "--samplesizes SAMPLE_SIZES" "Sample sizes"
    :parse-fn #(map read-string (split % #" "))]
   ["-db" "--database DATABASE" "Database"
    :parse-fn keyword]])

(defn save-json-to-file [s]
  (.write output-writer (str (json/write-str s) "\n")))

(defn save-results-to-file [results]
  (let [save-results-per-repetition
        (fn [res] (dorun (map #(do
                                 (save-json-to-file %)
                                 (progress/tick))
                              res)))
        save-results-per-sample
        (fn [res] (dorun (map save-results-per-repetition res)))
        save-results-per-query
        (fn [res] (dorun (map save-results-per-sample res)))]
    (save-results-per-query results)))

(defn execute-evaluation [opts]
  (save-results-to-file
    (queries/compare-queries ((:database opts) db-specs)
                             (:queries opts)
                             (:samplesizes opts)
                             (:repetitions opts))))

(defn- select-count [opts]
  (*
    (count (:queries opts))
    (count (:samplesizes opts))
    (:repetitions opts)))

(defn -main [& args]
  (try
    (let [opts (:options (parse-opts args cli-options))]
      (log/info "Executing with parameters:" (json/write-str opts))
      (log/info "The results of this execution are saved in" output-file)
      (progress/set-progress-bar! ":header [:bar] :percent :done/:total (Elapsed: :elapsed seconds, ETA: :eta seconds)")
      (progress/init "SELECTs executed" (select-count opts))
      (save-json-to-file opts)
      (execute-evaluation opts)
      (log/info "Execution finished")
      (progress/done))
    (finally (.close output-writer))))

; postmaster -D /usr/local/var/postgres
; lein run --queryid='1a 1b' --repetitions=2 --samplesizes='1 2' --database=postgresql
