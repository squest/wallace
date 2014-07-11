(defproject wallace "0.1.0"
  :description "Lightweight graph database on top of couchbase and couchbase-clj"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [couchbase-clj "0.1.3"]
								 [expectations "2.0.6"]]
	:plugins [[lein-expectations "0.0.7"]
						[codox "0.8.10"]])
