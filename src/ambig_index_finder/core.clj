(ns ambig-index-finder.core
  (:require [ambig-index-finder.queries :as queries]
            [clojure.tools.logging :as log]
            [environ.core :as environ]))

(def db-specs (load-string
                (slurp
                  (environ/env :db-config-file))))

(defn- compare-query [db-spec id sample-sizes]
  (let [sample-size (first sample-sizes)
        r (rest sample-sizes)
        result (queries/sample-and-query db-spec id sample-size)]
    (do
      (log/info "Sampling with" sample-size "for query with id" id)
      (log/info result)
      (if (empty? r)
        [result]
        (cons result
              (compare-query db-spec id r))))))

(defn -main []
  (compare-query (:postgresql db-specs) "1a" [1 10 100]))

; postmaster -D /usr/local/var/postgres
