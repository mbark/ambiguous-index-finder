(ns ambig-index-finder.core
  (:require [ambig-index-finder.queries :as queries]
            [clj-progress.core :as progress]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :refer [split]]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.tools.logging :as log]
            [environ.core :as environ]))

(def writer (atom nil))
(def db-specs (load-string (slurp (environ/env :db-config-file))))

(def cli-options
  [["-q" "--queries QUERIES" "The queries to run"
    :parse-fn #(split % #" ")
    :missing "queries not specified"]
   ["-s" "--samplesizes SAMPLE_SIZES" "The sample sizes per query"
    :parse-fn #(map read-string (split % #" "))
    :missing "sample sizes not specified"]
   ["-r" "--repetitions REPETITIONS" "The number of repetitions per sample size"
    :parse-fn read-string
    :missing "repetitions not specified"]
   ["-db" "--database DATABASE" "The database to run the tests on"
    :parse-fn keyword
    :missing "database not specified"]])

(defn- init-writer []
  (let [output-file
        (str
          "output/execution-"
          (quot (System/currentTimeMillis) 1000))]
    (reset! writer (io/writer output-file))
    output-file))

(defn- save-json-to-file [s]
  (.write @writer (str (json/write-str s) "\n")))

(defn- save-results-to-file [results]
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

(defn print-cli-error [cli-opts]
  (print "Missing options: ")
  (dorun (map #(print (str % "; ")) (:errors cli-opts)))
  (println "\nUsage:\n" (:summary cli-opts)))

(defn -main [& args]
  (let [cli-opts (parse-opts args cli-options)]
    (if (:errors cli-opts)
      (print-cli-error cli-opts)
      (try
        (let [opts (:options (parse-opts args cli-options))
              output-file (init-writer)]
          (log/info "Executing with parameters:" (json/write-str opts))
          (log/info "The results of this execution are saved in" output-file)
          (progress/set-progress-bar! ":header [:bar] :percent :done/:total (Elapsed: :elapsed seconds, ETA: :eta seconds)")
          (progress/init "SELECTs executed" (select-count opts))
          (io/make-parents output-file)
          (save-json-to-file opts)
          (execute-evaluation opts)
          (log/info "Execution finished")
          (progress/done)
          (println (str "Execution finished, results are saved in " output-file)))
        (finally (.close @writer))))))
