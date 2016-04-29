(ns ambig-index-finder.core
  (:gen-class)
  (:require [ambig-index-finder.generator :as generator]
            [ambig-index-finder.parser :as parser]
            [ambig-index-finder.analyzer :as analyzer]
            [clj-progress.core :as progress]
            [cheshire.core :as cheshire]
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
   (:repetitions opts)
   (count generator/param-range)))

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

(defn- add-plan [writer plan]
  (cheshire/generate-stream plan writer)
  (.write writer "\n")
  (.flush writer))

(defn- save-generated-plan [file plan]
  (with-open [writer (io/writer file :append true)]
    (add-plan writer plan)
    (progress/tick)))

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
    (println (str "Saving generated plans to file " output-file))
    (progress/init "SELECTs executed" (plans-per-query opts))
    (write-opts output-file opts)
    (generator/generate-plans opts #(save-generated-plan output-file %))
    (log/info "Execution finished")
    (progress/done)
    (println (str "Done generating plans, results are saved in " output-file))
    output-file))

(defn- read-json [reader]
  (if-let [line (.readLine reader)]
    (json/read-str line :key-fn keyword)))

(defn- save-parsed-plan [file plan]
  (with-open [writer (io/writer file :append true)]
    (add-plan writer plan)))

(defn- opts->db [opts]
  (keyword (:database opts)))

(defn parse-plans [input-file]
  (with-open [reader (io/reader input-file)]
    (let [output-file (get-output-file parse-dir)
          opts (read-json reader)]
      (log/info "Parsing plans in" input-file "which has options" opts)
      (println (str "Parsing plans, results are saved in " output-file))
      (progress/init "Plans parsed" (plans-per-query opts))
      (write-opts output-file opts)
      (dotimes [i (plans-per-query opts)]
        (save-parsed-plan output-file
                          (parser/parse-plan
                           (opts->db opts)
                           (read-json reader)))
        (progress/tick))
      (println (str "Done parsing plans, results are saved in " output-file))
      output-file)))

(defn- read-plans-from-file [reader]
  (let [lines (line-seq reader)
        opts (json/read-str (first lines) :key-fn keyword)
        plans (map json/read-str (rest lines))]
    [opts plans]))

(defn analyze-plans [input-file]
  (with-open [reader (io/reader input-file)]
    (let [output-file (get-output-file analyze-dir)
          opts (read-json reader)
          plan-count (plans-per-query opts)
          plans-per-analysis (/ plan-count (count (:samplesizes opts)))
          analyzes (/ plan-count plans-per-analysis)]
      (log/info "Analyzing plans in" input-file "which has options" opts)
      (println (str "Analyzing plans, results are saved in " output-file))
      (progress/init "Plans read for analysis" plan-count)
      (write-opts output-file opts)
      (dotimes [i analyzes]
        (let [analysis (analyzer/analyze-plans
                        (opts->db opts)
                        #(do (progress/tick)
                             (read-json reader))
                        plans-per-analysis)]
          (save-parsed-plan output-file analysis)
          (json/pprint analysis)))
      (println (str "Done analyzing plans, results are saved in " output-file))
      output-file)))

(defn -main [& args]
  (let [opts (parse-cli-opts args)
        steps (:steps opts)]
    (log/debug "Calling main with arguments:" args)
    (log/info "Executing steps" steps)
    (progress/set-progress-bar! ":header [:bar] :percent :done/:total (Elapsed: :elapsed seconds, ETA: :eta seconds)")
    (let [generate? (some #{:generate} steps)
          parse? (some #{:parse} steps)
          analyze? (some #{:analyze} steps)
          plans-file
          (if generate? (generate-plans args) (first args))]
      (log/info "generate?" generate? "parse?" parse? "analyze?" analyze?)
      (if parse?
        (let [parse-file (parse-plans plans-file)]
          (if analyze? (analyze-plans parse-file)))
        (if (and (not generate?) analyze?)
          (analyze-plans plans-file))))))
