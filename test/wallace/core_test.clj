(ns wallace.core-test
  (:require [expectations :refer :all]
						[couchbase-clj.client :as cc]
            [wallace.core :refer :all]))

(defdb cb {:bucket "wallace"
					 :uris ["http://127.0.0.1:8091/pools"]})

(expect [1 2 3 4]
				(set-conj [1 2 3] 4))

(expect true 
				(substring? "jon" "jojon"))

(expect false 
				(substring? "jo" "snow"))

(expect false 
				(substring? "jo" "JOJON"))

(expect true 
				(substring? (.toUpperCase "jo")
										"JOJON"))

(expect [1 2 3 4]
				(set-conj [1 2 3 4] 2))

(expect :this
				(cbkey "this"))

(expect :123
				(cbkey 123))

(expect :that
				(cbkey :that))

(expect true
				(string? (lookup-nodes cb "test-node")))

(expect true
				(map? (lookup-type cb :node)))

(expect "meta"
				(:$gtype (lookup-type cb :node)))

(expect true
				(nil? (lookup-rels cb :rel)))

(expect-let [test (lookup-nodes cb "test-node")]
						test
						(:test-node (cc/get-json cb :ntype-lookup)))

(expect (vals (dissoc-meta (cc/get-json cb (lookup-nodes cb "test-node"))))
				(all-nodes cb "test-node"))

(expect nil
				(all-rels cb "test-node"))

(expect-let [some-test-data (cc/set-json cb :test-01 {:name "fooking" :address "baar"})]
						{:name "fooking" :address "baar"}
						(cc/get-json cb :test-01))

(expect {:name "fooking" :address "baar" :dodol "lipet"}
				(assoc-doc! cb :test-01
										:dodol
										"lipet"))

(expect-let [next-data (cc/set-json cb :test-02 {:name "foo"})]
						{:name "foo" :letsome "data"}
						(merge-doc! cb :test-02
												{:letsome "data"}))

(expect true
				(ntype? cb "test-node"))

(expect false
				(ntype? cb "user"))

(expect false
				(rtype? cb "dodol"))

(expect true
				(seq? (all-types cb)))

(expect nil
				(lookup-nodes cb "jojon"))

(expect nil
				(lookup-rels cb "likes"))

(expect (cc/get-json cb :ntype-lookup)
				(lookup-type cb :node))

(expect (cc/get-json cb :rtype-lookup)
				(lookup-type cb :rel))

(def test-02 (add-ntype! cb "test-ntype-02"))

(expect {:status false}
				(dissoc (add-ntype! cb "test-ntype-02")
								:message))

(expect true
				(ntype? cb :test-ntype-02))

(def test-rel-01 (add-rtype! cb "test-rtype-01"))

(expect {:status false}
				(dissoc (add-rtype! cb "test-rtype-01")
								:message))

(expect true
				(rtype? cb :test-rtype-01))

(expect (into #{} [:test-node :test-ntype-02 :test-rtype-01 :test-rel-01])
				(into #{} (all-types cb)))

(expect (into #{} [:test-node :test-ntype-02])
				(into #{}
							(keys (dissoc (lookup-type cb :node)
														:$gtype))))

(def uuid-test "342hk234h5l4k5hh45jh45")

(expect-let [data (cc/set-json cb uuid-test
															 {:this "that" :that "this"})]
						{:this "that" :that "this" :mereka "momogi"}
						(assoc-doc! cb uuid-test :mereka "momogi"))

(def first-node (first (all-nodes cb :test-node)))
(def second-node (second (all-nodes cb :test-node)))

(expect (dissoc-meta (get-node cb first-node))
				(merge {:name "joni"}
							 (get-node-rel cb
														 (get-node cb first-node)
														 :test-rel-01
														 (get-node cb second-node))))

(expect [{:name "joni"}
				 {:name "joni"}]
				(map #(dissoc-meta %)
						 (map #(get-node cb %)
									(all-nodes cb :test-node))))

(expect nil
				(all-rels cb :test-rtype-01))

(def lookup-rels-1 (lookup-rels cb :test-rtype-01))

(expect true
				(string? lookup-rels-1))

(expect {:$gtype "lookup"}
				(cc/get-json cb lookup-rels-1))

(expect {:name "this" :jojon 123}
				(dissoc-meta (merge {:name "this" :jojon 123}
														{:$gtype "asd" :$ntype "well"})))

(expect []
				(filter #(zero? (first %))
								nil))

(expect []
				(map first nil))

(expect false
				(empty? (all-rels cb :test-rel-01)))

(expect true
				(coll? (all-rels cb :test-rel-01)))

(expect (uuid)
				(first (all-rels cb :test-rel-01)))

(expect false
				(let [this (lookup-type cb :node)]
					(and (nil? this)
							 (empty? this))))

(expect String
				(from-each [a (all-nodes cb :test-node-02)]
									 a))

(expect String
				(from-each [a (all-rels cb :test-rel-01)]
									 a))

(expect (more not-empty map?)
				(dissoc-meta (lookup-type cb :rel)))

(expect (more not-empty map?)
				(lookup-type cb :rel))














































