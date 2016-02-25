(defproject ambig-index-finder "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main ambig-index-finder.core
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [postgresql "9.3-1102.jdbc41"]
                 [mysql/mysql-connector-java "5.1.38"]
                 [org.clojure/java.jdbc "0.4.2"]
                 [org.clojure/data.json "0.2.6"]
                 [environ "1.0.2"]
                 [org.clojure/tools.logging "0.3.1"]
                 [log4j/log4j "1.2.17" :exclusions [javax.mail/mail
                                                    javax.jms/jms
                                                    com.sun.jmdk/jmxtools
                                                    com.sun.jmx/jmxri]]
                 [org.clojure/tools.cli "0.3.3"]
                 [intervox/clj-progress "0.2.1"]]
  :plugins [[lein-environ "1.0.0"]]
  :profiles {:dev        {:jvm-opts ["-Dlogfile.path=development"]
                          :env {:clj-env "development"
                                :db-config-file "resources/config/dev.edn"}}
             :test       {:jvm-opts ["-Dlogfile.path=test"]
                          :env {:clj-env "test"
                                :db-config-file "resources/config/dev.edn"}}
             :production {:jvm-opts ["-Dlogfile.path=production"]
                          :env {:clj-env "production"
                                :db-config-file "resources/config/dev.edn"}}}
  :clean-targets [:target-path :compile-path "output/*"])
