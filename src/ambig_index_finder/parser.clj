(ns ambig-index-finder.parser
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [clojure.walk :refer [postwalk]]))

(def output-dir "output/")
(def parse-results-dir "parse-results/")

(defn- plans-per-query [opts]
  (* (count (:samplesizes opts))
     (:repetitions opts)))

(def relation-accesses (atom []))

;; Not the best solution, but the easiest in this case
(defn- save-if-relation-access [o]
  (if (and (map? o) (contains? o "Relation Name"))
    (swap! relation-accesses conj o))
  o)

(defn- find-relation-accesses [plan]
  (reset! relation-accesses [])
  (postwalk save-if-relation-access plan)
  @relation-accesses)

(defn- group-by-relation [accesses]
  (apply merge-with concat
         (map
           (fn [l] (group-by #(get % "Relation Name") l))
           accesses)))

(defn- diff-relation-access [access-by-relation]
  (zipmap (keys access-by-relation)
          (map
            (fn [l]
              (distinct (map
                          #(dissoc % "Plan Rows" "Plan Width" "Total Cost")
                          l)))
            (vals access-by-relation))))

(defn analyze-plans-for-query [plans]
  (let [accesses (map find-relation-accesses plans)
        by-relation (group-by-relation accesses)
        diff (diff-relation-access by-relation)]
    diff))

(defn analyze-plans-for-all-queries [all-plans]
  (map analyze-plans-for-query all-plans))

(defn- save-parse-result [file-name parse-result]
  (let [file-path (str parse-results-dir file-name)]
    (io/make-parents file-path)
    (spit file-path (json/write-str parse-result))))

(defn -main [& args]
  (let [file-name (first args)]
    (if (nil? file-name)
      (println "File to parse not specified")
      (do
        (log/info "Parsing file" file-name)
        (with-open [reader (io/reader (str output-dir file-name))]
          (let [lines (line-seq reader)
                opts (json/read-str (first lines) :key-fn keyword)
                plans (partition
                        (plans-per-query opts)
                        (mapcat json/read-str (rest lines)))
                parse-result (analyze-plans-for-all-queries plans)]
            (log/info "Analyzing plans found in" (first args) "which has options" opts)
            (save-parse-result file-name parse-result)))))))

