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

(def test-one (get-node mdb 4 "user"))

(expect {:name "popok"}
				(:data test-one))

(expect (:eid test-one)
				(:user (cc/get-json mdb :eid-generator)))

(def last-user-eid (:user (cc/get-json mdb :eid-generator)))

(def last-user (get-node mdb last-user-eid "user"))

(expect last-user
				(last (all-nodes mdb "user")))

(def test-two (get-node mdb 5 "user"))

(expect {:name "pasukan"}
				(:data (last (all-nodes mdb "user"))))

(expect 5
				(get-eid mdb "user"))

(def test-three (add-node! mdb "user" {:name "jojoba"}))

(expect 5
				(:eid test-three))

(expect (:uuid test-three)
				(get-ntype-uuid mdb (:eid test-three) "user"))

(expect {:name "jojoba"}
				(:data test-three))

(delete-node! mdb (get-ntype-uuid mdb (:eid test-three) "user"))






