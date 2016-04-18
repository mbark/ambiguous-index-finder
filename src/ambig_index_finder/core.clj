(ns ambig-index-finder.core
  (:require [ambig-index-finder.generator :as generator]
            [ambig-index-finder.parser :as parser]
            [ambig-index-finder.analyzer :as analyzer]
            [clj-progress.core :as progress]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :refer [split]]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.tools.logging :as log]
            [environ.core :as environ]))

(def plans-dir "plans")
(def parse-dir "parses")
(def analyze-dir "analyzes")

(def cli-options
  [["-steps" "--steps STEPS" "The different steps of execution (generate, parse, analyze)"
    :parse-fn #(map keyword (split % #" "))
    :missing "steps to run not specified"]
   ["-q" "--query QUERY" "The query to run"
    :parse-fn str]
  ["-s" "--samplesizes SAMPLE_SIZES" "The sample sizes per query"
    :parse-fn #(map read-string (split % #" "))]
   ["-r" "--repetitions REPETITIONS" "The number of repetitions per sample size"
    :parse-fn read-string]
   ["-db" "--database DATABASE" "The database to run the tests on"
    :parse-fn keyword ]])

(defn plans-per-query [opts]
  (*
   (count (:samplesizes opts))
   (:repetitions opts)))

(defn- get-output-file [dir]
  (let
      [chars (map char (range 97 122))
       start (reduce str  (take 3 (repeatedly #(rand-nth chars))))
       end (quot (System/currentTimeMillis) 1000)
       file-name (str start "-" end)
       file (str dir "/" file-name)]
    (io/make-parents file)
    file))

(defn- write-opts [file opts]
  (spit file (str (json/write-str opts) "\n")))

(defn- add-plan [file plan]
  (spit file (str (json/write-str plan) "\n") :append true))

(defn- save-plans [file plans]
  (let [save-query #(do (add-plan file %)
                        (progress/tick))
        save-results-per-repetition #(dorun (map save-query %))
        save-results-per-sample #(dorun (map save-results-per-repetition %))]
    (dorun (map save-results-per-sample plans))))

(defn- save-parsed-plans [file plans]
  (dorun (map
          #(do (add-plan file %))
          plans)))

(defn- print-cli-error [cli-opts]
  (print "Missing options: ")
  (dorun (map #(print (str % "; ")) (:errors cli-opts)))
  (println "\nUsage:\n" (:summary cli-opts)))

(defn- parse-cli-opts [args]
  (let [cli-opts (parse-opts args cli-options)]
    (if (:errors cli-opts)
      (print-cli-error cli-opts)
      (:options cli-opts))))

(defn generate-plans [args]
  (let [opts (parse-cli-opts args)
        output-file (get-output-file plans-dir)]
    (log/info "Generating plans with options:" (json/write-str opts))
    (log/info "The results are saved in" output-file)
    (progress/set-progress-bar! ":header [:bar] :percent :done/:total (Elapsed: :elapsed seconds, ETA: :eta seconds)")
    (progress/init "SELECTs executed" (plans-per-query opts))
    (write-opts output-file opts)
    (save-plans output-file (generator/generate-plans opts))
    (log/info "Execution finished")
    (progress/done)
    (println (str "Done generating plans, results are saved in " output-file))
    output-file))

(defn- read-plans-from-file [reader]
  (let [lines (line-seq reader)
        opts (json/read-str (first lines) :key-fn keyword)
        plans (partition
                (:samplesizes opts)
                (map json/read-str (rest lines)))]
    [opts plans]))

(defn parse-plans [input-file]
  (with-open [reader (io/reader input-file)]
    (let [output-file (get-output-file parse-dir)
          [opts plans] (read-plans-from-file reader)]
      (log/info "Parsing plans in" input-file "which has options" opts)
      (write-opts output-file opts)
      (save-parsed-plans output-file (parser/parse-plans plans))
      (println (str "Done parsing plans, results are saved in " output-file))
      output-file)))

(defn analyze-plans [input-file]
  (with-open [reader (io/reader input-file)]
    (let [output-file (get-output-file analyze-dir)
          [opts plans] (read-plans-from-file reader)]
      (log/info "Analyzing plans in" input-file "which has options" opts)
      (write-opts output-file opts)
      (save-parsed-plans output-file (analyzer/analyze-plans plans))
      (println (str "Done analyzing plans, results are saved in " output-file))
      output-file)))

(defn -main [& args]
  (let [opts (parse-cli-opts args)
        steps (:steps opts)]
    (log/debug "Calling main with arguments:" args)
    (log/info "Executing steps" steps)
    (let [generate? (some #{:generate} steps)
          parse? (some #{:parse} steps)
          analyze? (some #{:analyze} steps)
          plans-file
          (if generate? (generate-plans args) (first args))]
      (log/info "generate?" generate? "parse?" parse? "analyze?" analyze?)
      (if parse?
        (let [parse-file (parse-plans plans-file)]
          (if analyze? (analyze-plans parse-file)))))))
