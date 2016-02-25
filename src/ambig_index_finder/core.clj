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
(def output-writer (io/writer output-file))

(def db-specs (load-string (slurp (environ/env :db-config-file))))

(def cli-options
  [["-qid" "--queryid QID" "Query id"]
   ["-r" "--repetitions REPETITIONS" "Number of repetitions"
    :parse-fn read-string]
   ["-s" "--samplesizes SAMPLE_SIZES" "Sample sizes"
    :parse-fn #(map read-string (split % #" "))]
   ["-db" "--database DATABASE" "Database"
    :parse-fn keyword]])

(defn save-json-to-file [s]
  (.write output-writer (str (json/write-str s) "\n")))

(defn execute-evaluation [opts]
  (let [results
        (queries/compare-query ((:database opts) db-specs)
                               (:queryid opts)
                               (:repetitions opts)
                               (:samplesizes opts))
        save-results
        (fn [r] (doall (map #(do (save-json-to-file %) (progress/tick)) r)))]
    (doall (map save-results results))))

(defn -main [& args]
  (try
    (let [opts (:options (parse-opts args cli-options))]
      (log/info "Executing with parameters:" (json/write-str opts))
      (log/info "The results of this execution are saved in" output-file)
      (progress/init "Queries executed" (* (:repetitions opts) (count (:samplesizes opts))))
      (save-json-to-file opts)
      (execute-evaluation opts)
      (log/info "Execution finished")
      (progress/done))
    (finally (.close output-writer))))

; postmaster -D /usr/local/var/postgres
; lein run --queryid=1a --repetitions=2 --samplesizes='1 2' --database=postgresql
