(ns ambig-index-finder.analyzer)

(defn analyze-plan [plan]
  (zipmap
   (keys plan)
   (map count (vals plan))))
