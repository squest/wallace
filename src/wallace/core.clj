(ns wallace.core
  (:require [couchbase-clj.client :as cc]))

(defn uuid
  "Simple function to generate a string containing a newly generated uuid"
  []
  (str (java.util.UUID/randomUUID)))

(defmacro defdb
	"Simple macro to wrap couchbase-clj defclient, it makes users do not have to add couchbase-clj to deps"
	[dbname exp]
	(list 'cc/defclient dbname exp))

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
	(do (cc/set-json db :eid-generator {:gtype "meta"})
			(cc/set-json db :ntype-lookup {:gtype "meta"})))

(defn cbkey
	"A simple function that either accepts a string or otherwise and convert them into keyword"
	[st]
	(if (keyword? st)
			st
			(if (string? st)
					(keyword st)
					(keyword (str st)))))

(defn assoc-doc!
	"A simple assoc function for a specific doc in db with a specific id, it simply assocs whatever data
	in the db with the supplied key-value pair"
	[db id some-key some-val]
	(cc/set-json db id
							 (assoc (cc/get-json db id)
								 			some-key
											some-val)))

(defn dissoc-doc!
	"A simple dissoc a key from a doc data obtained from the db"
	[db id some-key]
	(cc/set-json db id
							 (dissoc (cc/get-json db id)
											 some-key)))

(defn ntype?
	"To check whether a certain ntype exist in database"
	[db ntype]
	(contains? (cc/get-json db :ntype-lookup)
						 (cbkey ntype)))

(defn set-ntype!
	"Add a new ntype in graph database"
	[db ntype]
	(if (ntype? db (cbkey ntype))
			{:status false :message "ntype already exists"}
			(let [new-uuid (uuid)]
				(do (cc/set-json db (cbkey new-uuid)
												 {:gtype "lookup"})
						(assoc-doc! db :eid-generator
												(cbkey ntype)
												0)
						(assoc-doc! db :ntype-lookup
												(cbkey ntype)
												new-uuid)))))

(defn get-eid
	[db ntype]
	((cbkey ntype) (cc/get-json db :eid-generator)))

(defn gen-eid!
	"Return a usable eid for a particular ntype"
	[db ntype]
	(do (when-not (ntype? db ntype)
								(set-ntype! db (cbkey ntype)))
			(let [new-eid (inc ((cbkey ntype) (cc/get-json db :eid-generator)))]
				(do (assoc-doc! db :eid-generator
												(cbkey ntype)
												new-eid)
						new-eid))))

(defn lookup-ntype
	"Returns the lookup doc for a particular ntype"
	[db ntype]
	(let [lookup-id ((cbkey ntype) (cc/get-json db :ntype-lookup))]
		(if-not (nil? lookup-id)
						(cc/get-json db lookup-id)
						nil)))

(defn all-nodes-uuid
	[db ntype]
	(map val (lookup-ntype db ntype)))

(defn get-ntype-uuid
	"Returns the uuid of a doc (either node or rel) with supplied eid+ntype"
	[db eid ntype]
	(if (ntype? db (cbkey ntype))
			((cbkey eid) (lookup-ntype db (cbkey ntype)))
			{:status false :message "ntype is not in database"}))

(defn add-node!
	"Add a node with ntype as ntype and data"
	[db ntype data]
	(let [new-uuid (uuid)
				new-eid (gen-eid! db ntype)
				ntype-uuid ((cbkey ntype) (cc/get-json db :ntype-lookup))
				final-data {:gtype "node"
										:eid new-eid
										:ntype ntype
										:rels []
										:data data
										:uuid new-uuid}]
		(do (assoc-doc! db ntype-uuid
										(cbkey new-eid)
										new-uuid)
				(cc/set-json db new-uuid
										 final-data)
				final-data)))

(defn get-node
	"Get a node data either by its uuid or eid+ntype combination"
	([db uuid]
	 (cc/get-json db uuid))
	([db eid ntype]
	 (get-node db (get-ntype-uuid db eid (cbkey ntype)))))

(defn all-nodes
	"Returns all nodes with a particular ntype"
	[db ntype]
	(let [nodes (lookup-ntype db ntype)]
		(map #(get-node db (val %))
				 (dissoc nodes :gtype))))

(defn merge-node!
	"Merge the data of existing node identified by its uuid with the supplied node-data"
	([db uuid node-data]
		(let [old-data (cc/get-json db uuid)
					meta-old-data (dissoc old-data :data)
					meta-node-data (dissoc node-data :data)]
			(cc/set-json db uuid
									 (assoc (merge meta-old-data
																 meta-node-data)
													:data
													(merge (old-data :data)
																 (node-data :data))))))
	([db eid ntype node-data]
	 (merge-node! db
								(get-ntype-uuid db eid (cbkey ntype))
								node-data)))

(defn set-node!
	"Update the existing node with supplied uuid or eid+ntype combo with the data supplied.
	The data is not including the meta-data for the node, it will be merge with existing 'data'
	part of the node"
	([db uuid data]
	 (merge-node! db uuid {:data data}))
	([db ntype eid data]
	 (set-node! db
							(get-ntype-uuid db eid (cbkey ntype))
							data)))

(defn add-node-rel!
	"Add a specific rel-uuid into node's rel"
	([db uuid rel-uuid]
	 (let [node-rels ((get-node db uuid) :rels)]
		 (merge-node! db 
									uuid 
									{:rel (set-conj node-rels rel-uuid)})))
	([db eid ntype rel-uuid]
	 (add-node-rel! db
									(get-ntype-uuid db eid (cbkey ntype))
									rel-uuid)))

(defn add-rel!
	"The actual function to put the relation into the database, it's ignorant of what the data is,
	the data should at least contain :start :end :rtype. It returns the new-uuid"
	[db data]
	(let [new-uuid (uuid)]
		(do (cc/set-json db new-uuid
										 (merge {:gtype "rel"}
														data))
				new-uuid)))

(defn node-uuid
	"Get the node-uuid from db based on supplied map that may content uuid, eid, or ntype"
	[db {:keys [uuid eid ntype]}]
	(if (nil? uuid)
			(get-ntype-uuid db eid (cbkey ntype))
			uuid))

(defn relate!
	"add a relation between 2 nodes, each node is a map with mandatory key :uuid or combination of
	:eid and :ntype"
	[db start-node rel-type end-node & data]
	(let [start-uuid (node-uuid db
															{:uuid (start-node :uuid)
															 :eid (start-node :eid)
															 :ntype (start-node :ntype)})
				end-uuid (node-uuid db
														{:uuid (end-node :uuid)
														 :eid (end-node :eid)
														 :ntype (end-node :ntype)})

				final-data (merge (first data)
													{:start start-uuid
													 :end end-uuid
													 :rtype rel-type})]
		(add-node-rel! db start-uuid
									 (add-rel! db final-data))))

(defn clean-delete-node!
	"The cleanup function when deleting a node, we need to unregister it in the lookup"
	[db eid ntype]
	(let [rels ((get-node db eid ntype) :rels)]
		(do (when-not (nil? rels)
									(doseq [rel rels]
												 (cc/delete db rel)))
				(dissoc-doc! db eid (cbkey ntype)))))

(defn delete-node!
	"Delete a node, either by uuid or eid+ntype, also performs cleaning up the lookup data"
	([db uuid]
	 (let [data (cc/get-json db uuid)
				 eid (data :eid)
				 ntype (data :ntype)]
		 (do (clean-delete-node! db eid (cbkey ntype))
				 (cc/delete db uuid))))
	([db eid ntype]
	 (let [uuid (get-ntype-uuid db eid (cbkey ntype))]
		 (do (clean-delete-node! db eid (cbkey ntype))
				 (cc/delete db uuid)))))











