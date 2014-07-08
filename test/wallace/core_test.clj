(ns wallace.core-test
  (:require [expectations :refer :all]
						[couchbase-clj.client :as cc]
            [wallace.core :refer :all]))

(defdb mdb {:bucket "wallace"
					  :uris ["http://127.0.0.1:8091/pools"]})

(expect [1 2 3 4]
				(set-conj [1 2 3] 4))

(expect [1 2 3 4]
				(set-conj [1 2 3 4] 2))

(expect :this
				(cbkey "this"))

(expect :123
				(cbkey 123))

(expect :that
				(cbkey :that))

(expect )