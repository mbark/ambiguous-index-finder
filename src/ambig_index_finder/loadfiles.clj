(ns ambig-index-finder.loadfiles
  "A hack to work around the fact that pgloader has a tendency to time out if given too many, too large tables."
  (:require [environ.core :as environ]
            [clojure.java.io :as io]))

(def transfer-info (load-string (slurp (environ/env :transfer-info))))
(def loadfile-dir "loadfiles/")
(def with-params "include drop, create tables, no truncate, reset sequences, create indexes, no foreign keys")
(def casts "type set to text using set-to-enum-array")

(defn ->command [tables]
  (str
    "LOAD DATABASE FROM " (:mysql-db transfer-info)
    " INTO " (:pg-db transfer-info)
    " WITH " with-params
    " CAST " casts
    " INCLUDING ONLY TABLE NAMES MATCHING " (reduce #(str %1 ", " %2) tables)
    ";"))

(defn all-commands [tables]
  (for [tables-per-cmd
        (partition 1 (map #(str "~'^" % "$'") tables))]
    (->command tables-per-cmd)))

(defn generate-loadfile [cmd]
  (let [file (str loadfile-dir (gensym "loadfile") ".tmp")]
    (io/make-parents file)
    (spit file cmd)))

(defn -main [& args]
  (dorun (map generate-loadfile
              (all-commands (distinct (:queried transfer-info))))))
