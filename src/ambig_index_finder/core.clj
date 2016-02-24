(ns ambig-index-finder.core
  (:require [ambig-index-finder.queries :as queries]
            [clojure.tools.logging :as log]
            [environ.core :as environ]))

(def db-specs (load-string
                (slurp
                  (environ/env :db-config-file))))

(defn -main []
  (log/info db-specs)
  (queries/compare-query (:postgresql db-specs) "20a" 1 1000))

; postmaster -D /usr/local/var/postgres
