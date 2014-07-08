(ns wallace.core
  (:require [couchbase-clj.client :as cc]))

(defmacro defdb
	"Simple macro to wrap couchbase-clj defclient, it makes users do not have to add couchbase-clj to deps"
	[dbname exp]
	(list 'cc/defclient dbname exp))

(defn uuid
  "Simple function to generate a string containing a newly generated uuid"
  []
  (str (java.util.UUID/randomUUID)))

(defdb cb {:bucket "wallace"
					 :uris ["http://127.0.0.1:8091/pools"]})

(defn set-conj
	"Performs set like behavior for vector conj to deal with json that do not have set datatype"
	[col elmt]
	(if (some #(= elmt %) col)
			col 
			(conj col elmt)))

(defn create-graph!
	"Function to initialise db, it creates meta data for the graph"
	[db]
	(and (cc/set-json db :ntype-lookup {:$gtype "meta"})
			 (cc/set-json db :rtype-lookup {:$gtype "meta"})))

(defn cbkey
	"A simple function that either accepts a string or otherwise and convert them into keyword"
	[st]
	(if (keyword? st)
			st
			(if (string? st)
					(keyword st)
					(keyword (str st)))))

(defn lookup-type
	"returns the one of two lookup, gtype either :node or :rel"
	[db gtype]
	(if (= :node (cbkey gtype))
			(cc/get-json db :ntype-lookup)
			(cc/get-json db :rtype-lookup)))

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
				(cc/get-json db (cbkey lookup)))))

(defn all-rels
	"Returns all the rels uuids for a particular rtype"
	[db rtype]
	(let [lookup ((cbkey rtype) (lookup-type db :rel))]
		(if (nil? lookup)
				nil
				(cc/get-json db (cbkey lookup)))))

(defn- assoc-lookup!
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

(defn- assoc-doc!
	[db dbkey somekey somevalue]
	(let [old-doc (cc/get-json db (cbkey dbkey))]
		(do (cc/set-json db (cbkey dbkey)
										 (assoc old-doc
										  			 somekey
											  		 somevalue))
				(assoc old-doc
							 somekey
							 somevalue))))

(defn- merge-doc!
	[db dbkey data]
	(let [old-doc (cc/get-json db (cbkey dbkey))]
		(do (cc/set-json db
										 (cbkey dbkey)
										 (merge old-doc
														data))
				(merge old-doc
							 data))))

(defn- ntype?
	[db ntype]
	(let [lookup (lookup-type db :node)]
		(if (contains? lookup (cbkey ntype))
				true
				false)))

(defn- add-ntype!
	[db ntype]
	(if (ntype? db ntype)
			{:status false :message "ntype already in database"}
			(let [uuid (uuid)]
				(do (cc/set-json db (cbkey uuid)
												 {:$gtype "lookup"})
						(assoc-lookup! db :node (cbkey ntype) uuid)))))

(defn- rtype?
	[db rtype]
	(let [lookup (lookup-type db :rel)]
		(if (contains? lookup (cbkey rtype))
				true
				false)))

(defn- add-rtype!
	[db rtype]
	(if (rtype? rtype)
			{:status false :message "rtype already in database"}
			(let [uuid (uuid)]
				(do (cc/set-json db uuid
												 {:$gtype "lookup"})
						(assoc-lookup! db :rel (cbkey rtype) uuid)))))

(defn add-node!
	"Add a node to the db with a specific ntype, data is optional, it would be the property of this node,
	the data would be merged with meta data map (keywords of these meta-data prefixed with $ sign)"
	[db ntype & data]
	(let [uuid (uuid)
				final-data (merge (first data)
													{:$gtype "node"
													 :$rels {}
													 :$ntype ntype
													 :$uuid uuid})]
		(do (add-ntype! db ntype)
				(assoc-doc! db
										(lookup-nodes db :node)
										(cbkey uuid)
										uuid)
				(cc/set-json db
										 (cbkey uuid)
										 final-data)
				final-data)))

(defn get-node
	"Get a node using its valid uuid"
	[db uuid]
	(cc/get-json db uuid))

(defn get-rel
	"Get a relation using its valid uuid"
	[db uuid]
	(cc/get-json db uuid))

(defn- add-rel!
	[db rtype data]
	(let [uuid (uuid)]
		(do (add-rtype! db rtype)
				(cc/set-json db uuid
										 (merge data
														{:$gtype "rel"
														 :$rtype rtype
														 :$uuid uuid}))
				uuid)))

(defn- assoc-rels
	[db node-uuid rtype rel-uuid]
	(let [node-rels (:rels (get-node db node-uuid))]
		(merge node-rels
					 {(cbkey rtype) (set-conj (vec ((cbkey rtype) node-rels))
																		rel-uuid)})))

(defn- set-rel!
	[db node-uuid rtype rel-uuid]
	(let [final-data (assoc-rels db node-uuid rtype rel-uuid)]
		(assoc-doc! db node-uuid :rels final-data)))

(defn- nodes-rel
	[db start-uuid rtype end-uuid]
	(let [node-doc (get-node db start-uuid)
				node-rel-rtype ((cbkey rtype) (:rels node-doc))]
		(if-let [rel (first (filter #(and (= (:$start %) start-uuid)
																			(= (:$end %) end-uuid))
																(map #(get-rel db %)
																		 node-rel-rtype)))]
						(:uuid rel)
						false)))

(defn- update-rel!
	[db uuid data]
	(merge-doc! db uuid data))

(defn relate!
	"Relate start-node to end-node with a specific rtype type of relation, data is the property of
	the relation, this function either add or update the relation between those two nodes.
	start-node and end-node are maps with at least :uuid key exist in both. rtype can be an arbitrary
	relation in string or keyword, either already exist in database or not."
	[db start-node rtype end-node & data]
	(let [start-uuid (:$uuid start-node)
				end-uuid (:$uuid end-node)]
		(if-let [rel (nodes-rel db start-uuid rtype end-uuid)]
						(update-rel! db rel data)
						(->> (add-rel! db rtype (merge (first data)
																					 {:$start start-uuid
																						:$end end-uuid}))
								 (set-rel! db start-uuid rtype)))))






