(ns wallace.core
  (:require [couchbase-clj.client :as cc]))

(defmacro defdb
	"Simple macro to wrap couchbase-clj defclient, it makes users do not have to add couchbase-clj to deps"
	[dbname exp]
	(list 'cc/defclient dbname exp))

(defn substring?
	[sb st]
	(.contains st sb))


(defn uuid
  "Simple function to generate a string containing a newly generated uuid"
  []
  (str (java.util.UUID/randomUUID)))

(defn set-conj
	"Performs set like behavior for vector conj to deal with json that do not have set datatype"
	[col elmt]
	(if (some #(= elmt %) col)
			col 
			(conj col elmt)))

(declare create-index-lookup!)

(defn create-graph!
	"Function to initialise db, it creates meta data for the graph"
	[db]
	(do	(create-index-lookup! db :node-index-lookup)
			(create-index-lookup! db :rel-index-lookup)
			(and (cc/set-json db :ntype-lookup {:$gtype "meta"})
			 		 (cc/set-json db :rtype-lookup {:$gtype "meta"}))))

(defn cbkey
	"A simple function that either accepts a string or otherwise and convert them into keyword"
	[st]
	(if (keyword? st)
			st
			(if (string? st)
					(keyword st)
					(keyword (str st)))))

(defn get-uuid
	"Returns the uuid string of a node/rel doc, if it's already a uuid string then simply pass it"
	[doc-or-uuid]
	(if (or (keyword? doc-or-uuid)
					(string? doc-or-uuid))
			doc-or-uuid
			(:$uuid doc-or-uuid)))

(defn lookup-type
	"returns the one of two lookup, gtype either :node or :rel"
	[db gtype]
	(if (= :node (cbkey gtype))
			(cc/get-json db :ntype-lookup)
			(cc/get-json db :rtype-lookup)))

(defn all-types
	"Returns all types either ntype(s) and rtype(s)"
	[db]
	(concat (keys (dissoc (cc/get-json db :ntype-lookup)
												:$gtype))
					(keys (dissoc (cc/get-json db :rtype-lookup)
												:$gtype))))

(defn lookup-nodes
	"Returns the lookup doc uuid for a particular ntype"
	[db ntype]
	((cbkey ntype) (lookup-type db :node)))

(defn lookup-rels
	"Returns the lookup doc uuid for a particular rtype"
	[db rtype]
	((cbkey rtype) (lookup-type db :rel)))

(defn all-nodes
	"Returns all the nodes uuids for a particular ntype"
	[db ntype]
	(let [lookup ((cbkey ntype) (lookup-type db :node))]
		(if (nil? lookup)
				nil
				(vals (dissoc (cc/get-json db (cbkey lookup))
											:$gtype :$ntype)))))

(defn all-rels
	"Returns all the rels uuids for a particular rtype"
	[db rtype]
	(let [lookup (lookup-rels db (cbkey rtype))]
		(if (or (empty? lookup)
						(nil? lookup))
				nil
				(vals (dissoc (cc/get-json db (cbkey lookup))
											:$gtype :$rtype)))))

(defn assoc-lookup!
	"Assoc the lookup doc with a new key-value pair, dbkey can be :node or :rel"
	[db dbkey somekey somevalue]
	(let [old-data (lookup-type db (cbkey dbkey))
				dockey (if (= :node (cbkey dbkey))
								 	 :ntype-lookup
									 :rtype-lookup)]
		(do (cc/set-json db dockey
										 (assoc old-data
														somekey
														somevalue))
				(assoc old-data
							 somekey
							 somevalue))))

(defn assoc-doc!
	"Assoc a pair of key-value to a doc specified by its dbkey"
	[db nrdoc-or-uuid somekey somevalue]
	(let [dbkey (get-uuid nrdoc-or-uuid)
				old-doc (cc/get-json db (cbkey dbkey))]
		(do (cc/set-json db (cbkey dbkey)
										 (assoc old-doc
										  			 somekey
											  		 somevalue))
				(assoc old-doc
							 somekey
							 somevalue))))

(defn merge-doc!
	"Merge data into a doc in db with dbkey as its identifier"
	[db nrdoc-or-uuid data]
	(let [dbkey (get-uuid nrdoc-or-uuid)
				old-doc (cc/get-json db (cbkey dbkey))]
		(do (cc/set-json db
										 (cbkey dbkey)
										 (merge old-doc
														data))
				(merge old-doc
							 data))))

(defn ntype?
	"Returns true if ntype exist in db, false when otherwise"
	[db ntype]
	(let [lookup (lookup-type db :node)]
		(if (contains? lookup (cbkey ntype))
				true
				false)))

(defn add-ntype!
	"Add a new ntype into db"
	[db ntype]
	(if (ntype? db ntype)
			{:status false :message "ntype already in database"}
			(let [uuid (uuid)]
				(do (cc/set-json db (cbkey uuid)
												 {:$gtype "lookup"
													:$ntype ntype})
						(assoc-lookup! db :node (cbkey ntype) uuid)))))

(defn rtype?
	"Returns true if rtype exist in db, false if otherwise"
	[db rtype]
	(let [lookup (lookup-type db :rel)]
		(if (contains? lookup (cbkey rtype))
				true
				false)))

(defn add-rtype!
	"Add a new rtype into db, rtype can be a string/number/keyword"
	[db rtype]
	(if (rtype? db rtype)
			{:status false :message "rtype already in database"}
			(let [uuid (uuid)]
				(do (cc/set-json db uuid
												 {:$gtype "lookup"})
						(assoc-lookup! db :rel (cbkey rtype) uuid)))))

(defn register-node!
	[db ntype node-or-uuid]
	(let [node (get-uuid node-or-uuid)]
			 (assoc-doc! db
									 (lookup-nodes db ntype)
									 (cbkey node)
									 node)))

(defn add-node!
	"Add a new node with a given ntype and a given data as properties of the node"
	[db ntype & data]
	(let [uuid (uuid)
				final-data (merge (first data)
													{:$gtype "node"
													 :$rels {}
													 :$ntype ntype
													 :$uuid uuid})]
		(do (cc/set-json db
										 (cbkey uuid)
										 final-data)
				(add-ntype! db (cbkey ntype))
				(register-node! db ntype uuid)
				final-data)))

(defn dissoc-meta
	"Dissoc some meta keys from the doc, to get the actual users data"
	[doc]
	(dissoc doc :$gtype :$ntype :$rtype :$uuid :$rels))

(defn get-node
	"Get a node using its valid uuid"
	[db uuid]
	(cc/get-json db uuid))

(defn get-rel
	"Get a relation using its valid uuid"
	[db uuid]
	(cc/get-json db uuid))

(defn add-rel!
	"Specifically create a rel doc into db, with data (a map) as rel properties merged with metas"
	[db rtype data]
	(let [uuid (uuid)]
		(do (add-rtype! db rtype)
				(cc/set-json db uuid
										 (merge data
														{:$gtype "rel"
														 :$rtype rtype
														 :$uuid uuid}))
				uuid)))

(defn assoc-rels
	"Assoc a newly created rel into the nodes' data"
	[db node rtype rel]
	(let [nd (get-uuid node)
				rl (get-uuid rel)
				node-rels (:$rels (get-node db nd))]
		(merge node-rels
					 {(cbkey rtype) (set-conj (vec ((cbkey rtype) node-rels))
																		rl)})))

(defn set-rel!
	"Set the rel of a node with the rel-uuid"
	[db node rtype rel]
	(let [nd (get-uuid node)
				rl (get-uuid rel)
				final-data (assoc-rels db nd rtype rl)]
		(assoc-doc! db nd :$rels final-data)))

(defn register-rel!
	[db rtype rel-or-uuid]
	(let [rel (get-uuid rel-or-uuid)]
			 (assoc-doc! db
									 (lookup-rels db rtype)
									 (cbkey rel)
									 rel)))

(defn get-node-rel
	"Returns the uuid of a rel if there is a certain rtype relation between start-node and end-node"
	[db start-node rtype end-node]
	(let [node-doc (get-node db (get-uuid start-node))
				node-rel-rtype ((cbkey rtype) (:$rels node-doc))
				rel (first (filter #(and (= (:$start %) (get-uuid start-node))
																 (= (:$end %) (get-uuid end-node)))
													 (map #(get-rel db %)
																node-rel-rtype)))]
		(if (or (nil? rel)
						(empty? rel))
				nil
				(get-uuid rel))))

(defn update-rel!
	"Update the data of a rel with a specified uuid by merging the data into existing data"
	[db rel-or-uuid data]
	(merge-doc! db rel-or-uuid data))

(defn relate!
	"Relate start-node to end-node with a specific rtype type of relation, data is the property of
	the relation, this function either add or update the relation between those two nodes.
	start-node and end-node are maps with at least :uuid key exist in both. rtype can be an arbitrary
	relation in string or keyword, either already exist in database or not."
	[db start-node rtype end-node & data]
	(let [start-uuid (get-uuid start-node)
				end-uuid (get-uuid end-node)
				rel (get-node-rel db start-node rtype end-node)]
		(if (or (empty? rel)
						(nil? rel))
				(->> (add-rel! db rtype (merge (first data)
																			 {:$start start-uuid
																				:$end end-uuid}))
						 (set-rel! db start-uuid rtype)
						 (register-rel! db rtype))
				(update-rel! db rel data))))

(defn all-nodes-rels
	"Get all relations possessed by a particular node, node can be a node uuid or a complete node"
	[db node]
	(let [nd (get-uuid node)]
			 (:$rels (get-node db nd))))


; Index lookup :node-index-lookup & :rel-index-lookup

(defn create-index-lookup!
	"Create a new index lookup, only used in the beginning of graph creation"
	[db gtype]
	(cc/set-json db gtype
							 {:$gtype "meta"}))

(defn type-indexed?
	"Check whether or not a certain type already indexed in database, can check either for
	ntype or rtype, gtype needs to be specified with either :node or :rel"
	[db gtype nrtype]
	(if (= :node (cbkey gtype))
			(contains? (cc/get-json db :node-index-lookup)
								 (cbkey nrtype))
			(contains? (cc/get-json db :rel-index-lookup)
								 (cbkey nrtype))))

(defn key-indexed?
	"Check whether certain key properties of node/rel already indexed"
	[db gtype nrtype field-key]
	(let [index-data (if (= :node (cbkey gtype))
											 (cc/get-json db :node-index-lookup)
											 (cc/get-json db :rel-index-lookup))]
		(contains? ((cbkey nrtype) index-data)
							 (cbkey field-key))))

(defn get-index-lookup
	"Get the index-lookup doc for a certain n/r-type"
	[db gtype nrtype]
	(let [nrkey (if (= :node (cbkey gtype))
									:node-index-lookup
									:rel-index-lookup)]
		((cbkey nrtype) (cc/get-json db nrkey))))

; FIXME I'M STILL BROKEN
(defn create-type-index!
	"Create a type index"
	[db {:keys [$gtype $nrtype]}]
	(if (type-indexed? db $gtype $nrtype)
			{:status false :message "Type already indexed"}
			(let [nrkey (if (= :node (cbkey $gtype))
											:node-index-lookup
											:rel-index-lookup)]
				(assoc-doc! db nrkey
										(cbkey $nrtype)
										{}))))

(defn create-key-index!
	"Create a new index for a specific nrkey"
	[db {:keys [$gtype $nrtype]} field-key]
	(if (key-indexed? db $gtype $nrtype field-key)
			{:status false :message "Key already indexed"}
			(do (create-type-index! db $gtype $nrtype)
					(let [uid (uuid)
								nrkey (if (= :node (cbkey $gtype))
													:node-index-lookup
													:rel-index-lookup)
								key-doc {:$gtype "index"
												 :$key field-key
												 :$uuid uuid}
								old-data (get-index-lookup db $gtype $nrtype)]
						(do (cc/set-json db uuid
														 key-doc)
								(assoc-doc! db nrkey
														(cbkey $nrtype)
														(assoc old-data
																	 (cbkey $nrtype)
																	 uuid)))))))

; FIXME I dont know what is this actually

(defn get-key-index
	[db doc field-key]
	(let [{:keys [$gtype $ntype $rtype]} doc
				lookup (get-index-lookup db $gtype (if (= :node (cbkey $gtype))
																						 	 $ntype
																							 $rtype))]
		nil))

; FIXME This is also a messed up function

(defn index-node-key
	"Assoc a new index key into the existing (or newly created) index doc for a particular nrtype"
	[db nrkey doc pair]
	(let [{:keys [$gtype $ntype $uuid]} doc]
		(do (create-key-index! db $gtype $ntype (cbkey (key pair)))
				(let [old-data (get-key-index db)]))))

; TODO define this more clearly

(defn index-doc
	"FIXME I dont know what this is"
	[db doc]
	(let [{:keys [$gtype $ntype $rtype $uuid]} doc]
		(if (= :node (cbkey $gtype))
				(let [nrkey :node-index-lookup]
					(doseq [pair doc]
						(index-node-key db nrkey doc pair)))
				(let [nrkey :rel-index-lookup]))))









