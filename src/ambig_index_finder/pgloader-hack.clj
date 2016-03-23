(ns ambig-index-finder.pgloader-hack
  "A hack to work around the fact that pgloader has a tendency to time out if given too many, too large tables."
  (:gen-class)
  (:require [clojure.core.async :refer [alts! alts!! go <!! >!! <! >! thread chan timeout]]
            [me.raynes.conch :refer [let-programs]]))

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
    (let-programs [pgloader (:pgloader-path transfer-info)]
      (println "Executing:" cmd)
      (pgloader "--verbose" "--debug" file))))

(defn async-cmd [cmd c] 
  (go (>! c (thread (run-cmd cmd)))))

(defn print-output [cmd c]
   (println (<!! c) "from command" cmd))

(defn transfer-db-async []
  (let [cmds (all-commands queried-tables)
        res-chan (chan (count cmds))]
    (doseq [cmd cmds] (async-cmd cmd res-chan))
    (doseq [cmd cmds] (print-output cmd res-chan))))

(defn transfer-db []
  (doseq [cmd (all-commands (distinct (:queried tables-to-transfer)))]
    (run-cmd cmd)))

(defn -main [& args]
  (transfer-db-async))
