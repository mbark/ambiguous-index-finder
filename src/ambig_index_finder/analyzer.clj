(ns ambig-index-finder.analyzer)

(defn- analyze-plan [plan]
  (zipmap
   (keys plan)
   (map count (vals plan))))

(defn analyze-plans [parse-content]
  (map
   #(map analyze-plan %)
   parse-content))
