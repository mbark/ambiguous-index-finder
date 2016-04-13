(ns ambig-index-finder.analyzer
  (:require [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [clojure.data.json :as json]))

(def parse-dir "parse-results/")
(def parse-results-dir "analyze-results/")

(defn analyze-query-access-methods [access-methods]
  (zipmap
   (keys access-methods)
   (map count (vals access-methods))))

(defn analyze [parse-content]
  (map
   #(map analyze-query-access-methods %)
   parse-content))

(defn save-results [file-name results]
  (let [file-path (str parse-results-dir file-name)]
    (log/info "Saving results to" file-path)
    (io/make-parents file-path)
    (spit file-path (json/write-str results))))

(defn -main [& args]
  (let [file-name (first args)]
    (if (nil? file-name)
      (println "No file to analyze specified")
      (do
        (log/info "Analyzing file" file-name)
        (let [parse-content (slurp (str parse-dir file-name))
              analyze-result (analyze (json/read-str parse-content))]
          (save-results file-name analyze-result))))))
