(ns ambig-index-finder.parser
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [clojure.data :refer [diff]]
            [clojure.walk :refer [postwalk]]))

(defn- plans-per-query [opts]
  (* (count (:samplesizes opts))
     (:repetitions opts)))

(defn transformer [o]
  (let [out (if (map? o)
              (if (contains? o "Index Name")
                #{(get o "Index Name")}
                (if (contains? o "Plans")
                  (get o "Plans")
                  (if (contains? o "Plan")
                    (get o "Plan")
                    #{})))
              (if (and (coll? o) (every? set? o))
                (reduce clojure.set/union o)
                o))]
    out))

(defn- print-index-selections [plan-diff]
  (println (postwalk transformer plan-diff)))

(defn- diff-plans [plan1 plan2]
  (let [[diff1 diff2 same] (diff
                             (first plan1)
                             (first plan2))]
    (print-index-selections same))
  plan2)

(defn compare-plans [plans-for-query]
  (reduce
    diff-plans
    plans-for-query))

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

