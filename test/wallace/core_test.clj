(ns wallace.core-test
  (:require [expectations :refer :all]
						[couchbase-clj.client :as cc]
            [wallace.core :refer :all]))

(defdb mdb {:bucket "wallace"
					  :uris ["http://127.0.0.1:8091/pools"]})

(def all-nodes-pre (all-nodes mdb "user"))

(expect :123
				(cbkey 123))

(expect :whateva
				(cbkey "whateva"))

(expect :keypot
				(cbkey :keypot))

(expect true
				(ntype? mdb "user"))

(expect [1 2 3 4]
				(set-conj [1 2 3] 4))

(expect [1 2 3 4]
				(set-conj [1 2 3 4] 4))

(expect ["jojon" "dodol"]
				(set-conj ["jojon" "dodol"]  "dodol"))

(def test-one (get-node mdb 4 "user"))

(expect {:name "popok"}
				(:data test-one))

(def test-two (get-node mdb 5 "user"))

(expect {:name "pasukan"}
				(:data (last (all-nodes mdb "user"))))


(def test-three (get-node mdb 6 "user" ))

(expect 6
				(:eid test-three))

(expect (:uuid test-three)
				(get-node-uuid mdb (:eid test-three) "user"))

(expect {:name "jojoba"}
				(:data test-three))

(expect true
				(ntype? mdb "user"))

(expect true
				(ntype? mdb :user))

(expect false
				(ntype? mdb "momoka"))

(expect false
				(ntype? mdb :momogi))

(expect (:user (cc/get-json mdb :eid-generator))
				(get-eid mdb "user"))

(expect (:user (cc/get-json mdb :eid-generator))
				(get-eid mdb :user))

(expect (dissoc (cc/get-json mdb
														 (:user (cc/get-json mdb
																								 :ntype-lookup)))
								:gtype)
				(lookup-ntype mdb "user"))

(expect (all-nodes-uuid mdb :user)
				(all-nodes-uuid mdb "user"))

(expect (all-nodes mdb :user)
				(all-nodes mdb "user"))

(expect (get-node-uuid mdb 6 "user")
				(node-uuid mdb {:eid 6 :ntype "user"}))

(expect (:rels (get-node mdb 6 "user"))
				(:rels (get-node mdb (get-node-uuid mdb 6 "user"))))



(def test-node-01 (add-node! mdb "user" {:name "poposal" :resta "thisthat"}))
(def test-node-02 (add-node! mdb "user" {:name "jojojo" :resta "thisthat"}))
(def test-rel-01 (relate! mdb
													{:uuid (:uuid test-node-01)}
													"likes"
													{:uuid (:uuid test-node-02)}
													{:timestamp "paspasan"}))

(expect test-rel-01
				(cc/get-json mdb
										 (:uuid test-rel-01)))

(expect (get-node-uuid mdb
											 (:eid test-node-02)
											 "user")
				(delete-node! mdb
											(:eid test-node-02)
											"user"))

(expect (node-uuid mdb
									 {:eid (:eid test-node-01)
									  :ntype (:ntype test-node-01)})
				(delete-node! mdb
											(:eid test-node-01)
											(:ntype test-node-01)))

(def all-nodes-post (all-nodes mdb "user"))

(expect (count all-nodes-post)
				(count all-nodes-pre))










