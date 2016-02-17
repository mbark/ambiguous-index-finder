(ns ambig-index-finder.core
	(:require [ambig-index-finder.queries :as queries]))

; postmaster -D /usr/local/var/postgres

(defn -main []
  (queries/compare-query "20a" 1 1000))
