(ns ambig-index-finder.pgloader_hack
  "A hack to work around the fact that pgloader has a tendency to time out if given too many, too large tables."
  (:require [clojure.core.async :refer [alts! alts!! go <!! >!! <! >! thread chan timeout]]
            [environ.core :as environ]
            [clj-shell.shell :refer [sh]]))

(def transfer-info (load-string (slurp (environ/env :transfer-info))))
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

(defn run-cmd [cmd]
  (let [file (str (gensym "loadfile") ".tmp")]
    (spit file cmd)
    (println "Executing:" cmd)
    (sh "pgloader" "--verbose" "--debug" file :dir "/home/martinba/dev/pgloader/build/bin")))

(defn async-cmd [cmd c]
  (go (>! c (run-cmd cmd))))

(defn print-output [cmd c]
   (println (<!! c) "from command" cmd))

(defn transfer-db-async [tables-id]
  (let [cmds (all-commands (distinct (tables-id transfer-info)))
        res-chan (chan (count cmds))]
    (doseq [cmd cmds] (async-cmd cmd res-chan))
    (doseq [cmd cmds] (print-output cmd res-chan))))

(defn -main [& args]
  (transfer-db-async (keyword (first args))))
