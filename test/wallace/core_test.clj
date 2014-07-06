(ns wallace.core-test
  (:require [expectations :refer :all]
						[couchbase-clj.client :as cc]
            [wallace.core :refer :all]))

(defdb mdb {:bucket "wallace"
					  :uris ["http://127.0.0.1:8091/pools"]})

(expect :123
				(cbkey 123))

(expect :whateva
				(cbkey "whateva"))

(expect :keypot
				(cbkey :keypot))

(expect true
				(do (set-ntype! mdb "user")
						(ntype? mdb "user")))

(expect [1 2 3 4]
				(set-conj [1 2 3] 4))

(expect [1 2 3 4]
				(set-conj [1 2 3 4] 4))

(expect ["jojon" "dodol"]
				(set-conj ["jojon" "dodol"]  "dodol"))



