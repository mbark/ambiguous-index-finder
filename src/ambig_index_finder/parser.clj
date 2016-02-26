(ns ambig-index-finder.parser
  (:require [clojure.tools.logging :as log]
            [environ.core :as environ]
            [clojure.data.json :as json]
            [clojure.java.io :as io]))

(defn- plans-per-query [opts]
  (* (count (:samplesizes opts))
     (:repetitions opts)))

(defn- plan-comparator [plan1 plan2]
  {:not :implemented})

(defn- compare-plans [plans]
  (reduce
    plan-comparator
    plans))

(defn analyze-plans [[plans-for-query & r] opts]
  (if (nil? plans-for-query)
    []
    (cons
      (compare-plans plans-for-query)
      (analyze-plans r opts))))

(defn -main [& args]
  (let [file-name (first args)]
    (if (nil? file-name)
      (println "File to parse not specified")
      (do
        (log/info "Parsing file" file-name)
        (with-open [reader (io/reader file-name)]
          (let [lines (line-seq reader)
                opts (json/read-str (first lines) :key-fn keyword)
                plans (partition
                        (plans-per-query opts)
                        (map json/read-str (rest lines)))]
            (log/info "Analyzing plans found in" (first args) "which has options" opts)
            (log/info (analyze-plans plans opts))))))))

