package de.julielab.neo4j.plugins;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.server.plugins.ServerPlugin;

/**
 * 
 * The AhoCorasick class implements the algorithms of the research paper of Alfred V. Aho and Margaret J. Corasick from 1975. See <a
 * href="http://dl.acm.org/citation.cfm?doid=360825.360855">article</a> It adds some special functions like changing, deleting and appending attributes if the Tree is already
 * prepared for searching. Also the user can choose between a full search (like in the article), progressiv search (finds just the first longest match) and search without
 * overlapping (longest match).
 * 
 * @author SebOh
 * @deprecated
 */
public class AhoCorasick extends ServerPlugin {

	// EDGE TYPS
	public static enum EdgeTypes implements RelationshipType {
		FAIL
	}

	// LABEL TYPS
	public static enum LabelTypes implements Label {
		DICTIONARY
	}

	// CONSTANT FOR THE MODE OF THE SEARCHING PROCESS
	private final int FULL_WITH_OVERLAPPING = 0;
	private final int FULL_WITHOUT_OVERLAPPING = 1;
	private final int PROGRESSIV = 2;

	// CONSTANT FOR INDEX IN DATABASE
	private final String INDEX_DIC = "index_dic";
	private final String LETTER = "letter";

	// ENTRIES IN THE DICT-TREE
	public final String ENTRY = "entry";
	public final String ATTRIBUTES = "attributes";

	// CONSTANTS FOR MATCHING
	public final String MATCH = "match";
	public final String BEGIN = "begin";
	public final String END = "end";
	public final String NODE = "node";

	// PROPERTIES FOR A NODE
	private final String PROPERTY = "property_";
	private final String STATE = PROPERTY + "state";
	private final String DEPTH = PROPERTY + "depth";
	private final String NUMBER_OUTPUT = PROPERTY + "number_Output";
	private final String OUTPUT = PROPERTY + "Output";
	private final String ORIGINAL = PROPERTY + "original";

	// PROPERTIES FOR THE ROOT OF THE DICT-TREE
	public final String NODES_IN_TREE = "nodes_in_Tree";
	public final String DICTIONARY_NAME = "dict_name";
	public final String NUMBER_OF_ENTRIES = "number_of_entries";
	public final String PREPARED = "prepared";

	// NODE BEFORE RELATION TYP
	private final String RELATION_TYPE = "relation_typ";

	// MODUS ATTRIBUT FUNCTION
	private final int DELETE_ATTRIBUTE = 1;
	private final int CHANGE_ATTRIBUTE = 2;
	private final int ADD_ATTRIBUTE = 3;

	// ///////////////////////////////////////////////////////////////////
	// PUBLIC FUNCTION FOR CREATING AND CHANGING OF THE DICT-TREE

	/******** CREATE DICT-TREE *********************/

//	/**
//	 * Creates a new root of a DictTree, where entries could be add.
//	 * 
//	 * @param graphDb
//	 *            - Database where the Dict-Tree should be created
//	 * @param name
//	 *            - Name of the Dictionary
//	 * @return <code>true</code> if a new root was created, <code>false</code> if a root already exisits
//	 */
//	@Name("create_dict_tree")
//	@Description("Creates a new root of a DictTree, " + "where entries could be add. If root already exists" + "then the function returns false.")
//	@PluginTarget(GraphDatabaseService.class)
//	public boolean createDictTree(@Source GraphDatabaseService graphDb, @Description("Name of the dictionary.") @Parameter(name = "name") String name) {
//
//		// Auto Index to search faster for the next node
//		AutoIndexer<Relationship> relAutoIndexer = graphDb.index().getRelationshipAutoIndexer();
//		if (!relAutoIndexer.isEnabled()) {
//			relAutoIndexer.startAutoIndexingProperty(LETTER);
//			relAutoIndexer.setEnabled(true);
//		}
//
//		// Start Transaction
//		Transaction tx = graphDb.beginTx();
//
//		// Load IndexManager of GraphDatabase
//		IndexManager indMan = graphDb.index();
//		// Load all Nodes which are in the Index for the Dictionaries
//		Index<Node> index = indMan.forNodes(INDEX_DIC);
//
//		// Check if a root with the name as DICTIONARY_NAME property already
//		// exists
//		if (getRootOfDictTree(graphDb, name) == null) {
//
//			Node root = graphDb.createNode(LabelTypes.DICTIONARY);
//
//			// // Special Properties of the root
//			// Set Property dictionry name
//			root.setProperty(DICTIONARY_NAME, name);
//			// Set the number of nodes in tree to 1
//			root.setProperty(NODES_IN_TREE, 1);
//			// Set the numbers of entries in this dictionary to 0
//			root.setProperty(NUMBER_OF_ENTRIES, 0);
//			// Set the tree to be unprepared for searching
//			root.setProperty(PREPARED, false);
//
//			// // General Properties of the node
//			// Set the state
//			root.setProperty(STATE, 0);
//			// Set the deepth
//			root.setProperty(DEPTH, 0);
//			// Set the number of entries, which the node has
//			// to return if the system reach the node
//			root.setProperty(NUMBER_OUTPUT, 0);
//
//			// Adds the root to the index of Dictionaries of the GraphDatabase
//			// for easier finding
//			index.add(root, DICTIONARY_NAME, name);
//
//			tx.success();
//
//			// New root was created in the GraphDatabase
//			return true;
//		}
//
//		tx.failure();
//
//		// Root already exists in the GraphDatabase
//		return false;
//	}
//
//	/******** DICT-TREE DELETE ***********************/
//
//	/**
//	 * Deletes a complete DictTree in a GraphDatabase.
//	 * 
//	 * @param graphDb
//	 *            - GraphDatabase
//	 * @param name
//	 *            - Name of the Dict-Tree
//	 * @return <code>true</code> if the tree is deleted, <code>false</code> if the tree with the name does not exist
//	 * @throws IOException
//	 */
//	@Name("delete_dict_tree")
//	@Description("Deletes a complete DictTree in a GraphDatabase.")
//	@PluginTarget(GraphDatabaseService.class)
//	public boolean deleteDictTree(@Source GraphDatabaseService graphDb, @Description("Name of the dictionary.") @Parameter(name = "name") String name) throws IOException {
//
//		long groupCounter = 0;
//		final long maxGroupCount = 20000;
//
//		// Start Transaction
//		Transaction tx = graphDb.beginTx();
//
//		// Find the root of the Dict-Tree
//		Node root = getRootOfDictTree(graphDb, name);
//
//		// if root equals null, then return false, because root does not exists
//		// and so can't be deleted
//		if (root == null) {
//			tx.failure();
//			tx.finish();
//			return false;
//		}
//
//		// Delete the root in the Dictionary Index of the GraphDatatbase
//		IndexManager indMan = graphDb.index();
//		Index<Node> index = indMan.forNodes(INDEX_DIC);
//		index.remove(root);
//
//		// Process to delete all nodes and relations in the tree.
//		// So it starts with the root and then all nodes which
//		// has a relation to the root and then so on.
//		// So each depth of the tree will be deleted step by step.
//		Deque<Node> queue = new ArrayDeque<Node>();
//		queue.add(root);
//
//		while (!queue.isEmpty()) {
//			Node node = queue.pop();
//			// Get all Outgoint reltions from the current node.
//			Iterator<Relationship> iterRe = node.getRelationships(Direction.OUTGOING).iterator();
//
//			while (iterRe.hasNext()) {
//
//				Relationship rel = iterRe.next();
//
//				// Find all nodes which has to be deletet in the next depth and
//				// belongs to the current node
//				if (!rel.getType().name().equals(EdgeTypes.FAIL.name())) {
//					queue.add(rel.getEndNode());
//				}
//
//				// Delete the relation to the next nodes.
//				rel.delete();
//			}
//
//			// Delete the current node.
//			node.delete();
//
//			groupCounter++;
//
//			if (groupCounter >= maxGroupCount) {
//				groupCounter = 0;
//				tx.success();
//				tx.finish();
//
//				tx = graphDb.beginTx();
//			}
//		}
//
//		tx.success();
//		tx.finish();
//
//		return true;
//
//	}
//
//	/******** APPEND DICT-TREE *********************/
//
//	/**
//	 * Adds a list of entries to a DictTree, which is not prepared for searching
//	 * 
//	 * @param graphDb
//	 *            - GraphDatabase
//	 * @param name
//	 *            - name of the DictTree
//	 * @param dictEntries
//	 *            - The dictionary entries as a JSON map of the form { 'dictEntries':[{'entry':'entry1','attributes':{'attribute1':'attValue1','attribute2':'attValue2'}}] } The
//	 *            Entry is mapped with the constant ENTRY and the Attributes is a Map, which has the key ATRRIBUTES
//	 * @return <code>true</code> if all entries could be added, <code>false</code> if the tree is already prepared for searching
//	 * @throws JSONException
//	 * @throws IOException
//	 */
//	@SuppressWarnings("unchecked")
//	@Name("add_list_to_dicttree")
//	@Description("Adds a list of entries to a DictTree.")
//	@PluginTarget(GraphDatabaseService.class)
//	public boolean addListToDictTree(@Source GraphDatabaseService graphDb, @Description("Name of the dictionary.") @Parameter(name = "name") String name,
//			@Description("The dictionary entries as a JSON map of the form"
//					+ "{'dictEntries':[{'entry':'entry1','attributes':{'attribute1':'attValue1','attribute2':'attValue2'}}]}") @Parameter(name = "dictEntries") String dictEntries)
//			throws JSONException, IOException {
//
//		RelationshipIndex index = graphDb.index().forRelationships(LETTER);
//		((LuceneIndex<Relationship>) index).setCacheCapacity(LETTER, 10000000); // 10.000.000
//
//		// Start Transaction
//		Transaction tx = graphDb.beginTx();
//
//		// Parse String to JSONArry
//		JSONArray entries = new JSONArray(dictEntries);
//
//		// Get root of DictTree
//		Node root = getRootOfDictTree(graphDb, name);
//
//		// root has to be not equal null
//		if (root != null && !isDictTreePrepared(root)) {
//
//			for (int i = 0; i < entries.length(); i++) {
//
//				// Get the Map for the entry
//				JSONObject entry = entries.getJSONObject(i);
//				String entryString = entry.getString(ENTRY);
//
//				// Check whether the entry has Attributes or not
//				JSONObject attributes = new JSONObject();
//				if (entry.has(ATTRIBUTES)) {
//					attributes = entry.getJSONObject(ATTRIBUTES);
//				}
//				// Add the entry with his attributes to the DictTree
//				addEntryToTree(graphDb, root, entryString, attributes);
//			}
//
//			tx.success();
//			tx.finish();
//			return true;
//
//		} else {
//			tx.failure();
//			tx.finish();
//			return false;
//		}
//	}
//
//	/**
//	 * Adds a entry to the DictTree
//	 * 
//	 * @param graphDb
//	 *            - GraphDatabase
//	 * @param root
//	 *            - Root of the DictTree
//	 * @param word
//	 *            - Word which should be add to the DictTree
//	 * @throws JSONException
//	 */
//	private List<Node> addEntryToTree(GraphDatabaseService graphDb, Node root, String word, JSONObject attributes) throws JSONException {
//
//		// List for new nodes
//		List<Node> nodesNew = new ArrayList<Node>();
//
//		// Get the id number for the next node, which has to be created
//		long newState = toLong(root.getProperty(NODES_IN_TREE));
//
//		// Set the root as a starting point for searching the word in the tree
//		int j = 0;
//		Node node = root;
//		Node nodeNext = getNextNodeCreate(graphDb, String.valueOf(root.getProperty(DICTIONARY_NAME)), node, word.substring(j, j + 1));
//
//		// Lookup for the word in tree until a relation does not exist for a
//		// letter
//		while (nodeNext != null) {
//			node = nodeNext;
//			j = j + 1;
//
//			// Check if the whole word already is in the tree
//			if (j == word.length())
//				break;
//
//			// Try to get the next node for the next letter in the word
//			nodeNext = getNextNodeCreate(graphDb, String.valueOf(root.getProperty(DICTIONARY_NAME)), node, word.substring(j, j + 1));
//		}
//
//		// Create new nodes and relations for letter which do not appear
//		// currently in the
//		// DictTree at the correct place
//		for (int p = j; p < word.length(); p++) {
//
//			Node newNode = graphDb.createNode();
//
//			// Set node properties
//			newNode.setProperty(STATE, newState);
//			newNode.setProperty(NUMBER_OUTPUT, 0);
//			newNode.setProperty(DEPTH, p);
//
//			// Create Relation between current and new node
//			Relationship rel = node.createRelationshipTo(newNode, withName(word.substring(p, p + 1)));
//			// Create the key for autoindex to search faster for the next node
//			StringBuilder key = new StringBuilder(String.valueOf(root.getProperty(DICTIONARY_NAME)));
//			key.append(String.valueOf(node.getProperty(STATE)));
//			key.append(word.substring(p, p + 1));
//			rel.setProperty(LETTER, key.toString());
//
//			nodesNew.add(newNode);
//			node = newNode;
//			newState = newState + 1;
//		}
//
//		// Add word and attribut to the final node
//		addOutput(node, word, attributes);
//
//		// Check the Output
//		// If the node has already Fail-Relation the Output has to be updated
//		if (Boolean.valueOf(String.valueOf(root.getProperty(PREPARED)))) {
//
//			// The order is important otherwise the variable node is not the
//			// same anymore!!!
//			if (node.hasRelationship(EdgeTypes.FAIL, Direction.OUTGOING)) {
//				Node nodeEnd = node.getSingleRelationship(EdgeTypes.FAIL, Direction.OUTGOING).getEndNode();
//				unionOutput(node, nodeEnd);
//			}
//
//			while (node.hasRelationship(EdgeTypes.FAIL, Direction.INCOMING)) {
//				Node nodeStart = node.getSingleRelationship(EdgeTypes.FAIL, Direction.INCOMING).getStartNode();
//				unionOutput(nodeStart, node);
//				node = nodeStart;
//			}
//
//		}
//
//		// Update the number of nodes and number of entries in the tree
//		root.setProperty(NODES_IN_TREE, newState);
//		root.setProperty(NUMBER_OF_ENTRIES, toLong(root.getProperty(NUMBER_OF_ENTRIES)) + 1);
//
//		return nodesNew;
//	}
//
//	/**
//	 * Adds a list of entries to a DictTree, which is prepared for searching
//	 * 
//	 * @param graphDb
//	 *            - GraphDatabase
//	 * @param name
//	 *            - name of the DictTree
//	 * @param dictEntries
//	 *            - an array of the form [{ 'entry':'entry1','attributes':{'attribute1':'attValue1','attribute2':'attValue2'}} ,
//	 *            {'entry':'entry2','attributes':{'attribute3':'attValue1'}}] The Entry is mapped with the constant ENTRY and the Attributes is a Map, which has the key ATRRIBUTES
//	 * @return <code>true</code> if all entries could be added, <code>false</code> if the tree is already prepared for searching
//	 * @throws JSONException
//	 */
//	@Name("add_list_to_prepared_dicttree")
//	@Description("Adds entries from a list to the DictTree.")
//	@PluginTarget(GraphDatabaseService.class)
//	public boolean addListToPreparedDictTree(@Source GraphDatabaseService graphDb, @Description("Name of the dictionary.") @Parameter(name = "name") String name,
//			@Description("The dictionary entries as a JSON map of the form"
//					+ "{'dictEntries':[{'entry':'entry1','attributes':{'attribute1':'attValue1','attribute2':'attValue2'}}]}") @Parameter(name = "dictEntries") String dictEntries)
//			throws JSONException {
//
//		// Start Transaction
//		Transaction tx = graphDb.beginTx();
//
//		// New Nodes to add Fail-Relation
//		List<Node> nodesNew = new ArrayList<Node>();
//
//		// Parse String to JSONArry
//		JSONArray entries = new JSONArray(dictEntries);
//
//		// Get root of DictTree
//		Node root = getRootOfDictTree(graphDb, name);
//
//		// root has to be not equal null
//		if (root != null && isDictTreePrepared(root)) {
//
//			for (int i = 0; i < entries.length(); i++) {
//
//				// Get the Map for the entry
//				JSONObject entry = entries.getJSONObject(i);
//				String entryString = entry.getString(ENTRY);
//
//				// Check whether the entry has Attributes or not
//				JSONObject attributes = null;
//				if (entry.has(ATTRIBUTES)) {
//					attributes = entry.getJSONObject(ATTRIBUTES);
//				}
//
//				// Add the entry with his attributes to the DictTree
//				nodesNew.addAll(addEntryToTree(graphDb, root, entryString, attributes));
//
//			}
//
//			// sort the nodes in the list according the deepth in the tree
//			Comparator<Node> comper = new Deepth();
//			Collections.sort(nodesNew, comper);
//
//			// Step by step setting new Failrelation
//			for (int i = 0; i < nodesNew.size(); i++) {
//				Node node = nodesNew.get(i);
//
//				// Get Information about the node before and the relation from
//				// it
//				// to the current node
//				HashMap<String, Object> nodeBeforeInfo = getNodeBefore(node);
//				Node nodeBefore = (Node) nodeBeforeInfo.get(NODE);
//				RelationshipType relTyp = (RelationshipType) nodeBeforeInfo.get(RELATION_TYPE);
//
//				// Set the Fail-Relation as known in the Aho-Corasick Paper
//				if (nodeBefore.equals(root)) {
//					node.createRelationshipTo(root, EdgeTypes.FAIL);
//				} else {
//					Node state = getFailNode(nodeBefore);
//
//					// As long the fail node has no relation with the name of
//					// the current relation
//					// get the next fail node of the current fail node
//					// If the next fail node is the root, the while loop will
//					// stop
//
//					Node toNode = null;
//
//					while ((toNode = getNextNodeSearch(graphDb, name, state, relTyp)) == null) {
//						state = getFailNode(state);
//					}
//
//					// Set the fail relation
//					node.createRelationshipTo(toNode, EdgeTypes.FAIL);
//					unionOutput(node, toNode);
//				}
//
//				// Now check the other Fail-Relation whether have to be updated
//				// or not
//				Stack<Node> stack = new Stack<Node>();
//
//				// Get all nodes from the Incoming Fail-Relation of the
//				// nodeBefore
//				Iterator<Relationship> iterFailRelations = nodeBefore.getRelationships(Direction.INCOMING, EdgeTypes.FAIL).iterator();
//				while (iterFailRelations.hasNext()) {
//					stack.add(iterFailRelations.next().getStartNode());
//				}
//
//				// Now check whether they have a relation with the current
//				// letter
//				// if not check whether they have Incoming Fail-Relation and add
//				// them to the stack
//				while (!stack.isEmpty()) {
//
//					Node n = stack.pop();
//					Node nEnd = null;
//
//					// Check if a relation with the current letter exist
//					if ((nEnd = getNextNodeCreate(graphDb, name, n, relTyp.name())) != null && nEnd.hasRelationship(EdgeTypes.FAIL, Direction.OUTGOING)) {
//
//						subtractOutput(nEnd, nEnd.getSingleRelationship(EdgeTypes.FAIL, Direction.OUTGOING).getEndNode());
//						nEnd.getSingleRelationship(EdgeTypes.FAIL, Direction.OUTGOING).delete();
//						nEnd.createRelationshipTo(node, EdgeTypes.FAIL);
//						unionOutput(nEnd, node);
//
//						// If not does a incoming Fail-Relation exist
//					} else if (n.hasRelationship(Direction.INCOMING, EdgeTypes.FAIL)) {
//
//						iterFailRelations = n.getRelationships(Direction.INCOMING, EdgeTypes.FAIL).iterator();
//						while (iterFailRelations.hasNext()) {
//							stack.add(iterFailRelations.next().getStartNode());
//						}
//					}
//				}
//			}
//
//			tx.success();
//			tx.finish();
//			return true;
//
//		} else {
//
//			tx.failure();
//			tx.finish();
//			return false;
//		}
//
//	}
//
//	/******** WÖRTERBUCH VORBEREITEN FÜR SUCHE *********/
//
//	/**
//	 * Prepares the DictTree for searching. It sets all fail relations like in the algorithm 3 of the paper from 1975. See <a
//	 * href="http://dl.acm.org/citation.cfm?doid=360825.360855">article</a>
//	 * 
//	 * @param graphDb
//	 *            - Graphdatabase
//	 * @param name
//	 *            - Name of the dictionary
//	 * @return <code>true</code> if the tree is prepared, <code>false</code> if tree does not exist
//	 * @throws IOException
//	 * @throws JSONException
//	 */
//	@SuppressWarnings("unchecked")
//	@Name("prepare_dicttree_for_search")
//	@Description("Sets all Fail-Relation in the DictTree for searching.")
//	@PluginTarget(GraphDatabaseService.class)
//	public boolean prepareDictTreeForSearch(@Source GraphDatabaseService graphDb, @Description("Name of the dictionary.") @Parameter(name = "name") String name)
//			throws IOException, JSONException {
//
//		RelationshipIndex index = graphDb.index().forRelationships(LETTER);
//		((LuceneIndex<Relationship>) index).setCacheCapacity(LETTER, 1000000);
//
//		// Start Transaction
//		Transaction tx = graphDb.beginTx();
//		long groupCounter = 0;
//		final long maxGroupCount = 20000;
//
//		try {
//			// Find root for DictTree
//			Node root = getRootOfDictTree(graphDb, name);
//
//			// Check whether DictTree exist or not
//			if (isDictTreePrepared(root)) {
//				return false;
//			}
//
//			// Set root as a starting point for the process
//			// List has to be "First In - First Out" so that the algorith look
//			// up
//			// level by level (depth by depth) the tree!
//			Deque<Node> queue = new ArrayDeque<Node>();
//			Iterator<Relationship> iterRe = root.getRelationships(Direction.OUTGOING).iterator();
//
//			// Set the fail relations of all nodes with depth one the to the
//			// root
//			while (iterRe.hasNext()) {
//
//				Node endNode = iterRe.next().getEndNode();
//				endNode.createRelationshipTo(root, EdgeTypes.FAIL);
//				queue.add(endNode);
//				groupCounter++;
//			}
//
//			// From each node in the list look for the next node in the tree and
//			// set for them the fail relation
//			while (!queue.isEmpty()) {
//
//				Node node = queue.pop();
//				iterRe = node.getRelationships(Direction.OUTGOING).iterator();
//
//				// Look up for all nodes which has a relation to the node and
//				// the relation is no fail relation
//				while (iterRe.hasNext()) {
//
//					Relationship rela = iterRe.next();
//
//					if (!rela.isType(EdgeTypes.FAIL)) {
//
//						Node endNode = rela.getEndNode();
//						queue.add(endNode);
//
//						if (!endNode.hasRelationship(Direction.OUTGOING, EdgeTypes.FAIL)) {
//							// Get fail node of the current node
//							Node state = getFailNode(node);
//
//							// As long the fail node has no relation with the
//							// name of the current relation
//							// get the next fail node of the current fail node
//							// If the next fail node is the root, the while loop
//							// will stop
//
//							Node toNode = null;
//							while ((toNode = getNextNodeSearch(graphDb, name, state, rela.getType())) == null) {
//								state = getFailNode(state);
//							}
//
//							// Set the fail relation
//							endNode.createRelationshipTo(toNode, EdgeTypes.FAIL);
//
//							// If the fail node has an output then union both in
//							// the endNode
//							unionOutput(endNode, toNode);
//
//							groupCounter++;
//						}
//
//					}
//
//					// Renew Transaction to get no overhead
//					if (groupCounter >= maxGroupCount) {
//						System.out.println("Send");
//						groupCounter = 0;
//						tx.success();
//						tx.finish();
//						tx = graphDb.beginTx();
//					}
//				}
//			}
//
//			root.setProperty(PREPARED, true);
//
//			tx.success();
//		} finally {
//			tx.finish();
//		}
//
//		return true;
//	}
//
//	/**
//	 * Unprepare the DictTree, so that it can't be searched anymore.
//	 * 
//	 * @param graphDb
//	 *            - GraphDatabase
//	 * @param name
//	 *            - Name of the dictionary
//	 * @return <code>true</code> if the DictTree is unprepared, <code>false</code> if the DictTree does not exist
//	 * @throws IOException
//	 * @throws JSONException
//	 */
//	@SuppressWarnings("unchecked")
//	@Name("unprepare_dicttree")
//	@Description("Deletes all Fail-Relation in the DictTree.")
//	@PluginTarget(GraphDatabaseService.class)
//	public boolean unprepareDictTree(@Source GraphDatabaseService graphDb, @Description("Name of the dictionary.") @Parameter(name = "name") String name) throws IOException,
//			JSONException {
//
//		// Start Transaction
//		Transaction tx = graphDb.beginTx();
//
//		// Find root for DictTree
//		Node root = getRootOfDictTree(graphDb, name);
//
//		// If root does not exist return false
//		if (!isDictTreePrepared(root)) {
//			return false;
//		}
//
//		// Set root as a starting point for the process
//		// List has to be "First In - First Out" so that the algorith look up
//		// level by level (depth by depth) the tree!
//		Deque<Node> queue = new ArrayDeque<Node>();
//		queue.add(root);
//
//		long groupCounter = 0;
//		final long maxGroupCount = 20000;
//
//		// From each node in the list look for the next node in the tree and
//		// set for them the fail relation
//		while (!queue.isEmpty()) {
//			Node node = queue.pop();
//			Iterator<Relationship> iterRe = node.getRelationships(Direction.OUTGOING).iterator();
//			// Look up for all nodes which has a relation to the node and
//			// the relation is no fail relation
//			while (iterRe.hasNext()) {
//
//				Relationship rel = iterRe.next();
//
//				if (rel.getType().name().equals(EdgeTypes.FAIL.name())) {
//					// Deletes Fail-Relation
//					rel.delete();
//				} else {
//					// If no Fail-Relation add the node to a queue. Otherwise
//					// it's
//					// a endless loop here
//					queue.add(rel.getEndNode());
//				}
//			}
//
//			// Deletes all entries in the node which were created during the
//			// union function
//			if (toLong(node.getProperty(NUMBER_OUTPUT)) > 1) {
//				String original = String.valueOf(node.getProperty(ORIGINAL));
//
//				JSONObject ob = new JSONObject(String.valueOf(node.getProperty(OUTPUT)));
//				Iterator<String> iterOb = ob.keys();
//
//				while (iterOb.hasNext()) {
//					String key = iterOb.next();
//					if (key.startsWith(PROPERTY) || key.equals(original)) {
//						// Geht ins leere
//					} else {
//						iterOb.remove();
//						node.setProperty(NUMBER_OUTPUT, ((Integer) node.getProperty(NUMBER_OUTPUT) - 1));
//					}
//				}
//
//				node.setProperty(OUTPUT, ob.toString());
//			}
//
//			groupCounter++;
//			if (groupCounter > maxGroupCount) {
//				tx.success();
//				tx.finish();
//
//				groupCounter = 0;
//
//				tx = graphDb.beginTx();
//			}
//		}
//
//		// Update root property
//		root.setProperty(PREPARED, false);
//
//		tx.success();
//		tx.finish();
//
//		return true;
//	}
//
//	/******** DELETE ENTRY ************************/
//
//	/**
//	 * 
//	 * @param graphDb
//	 * @param name
//	 * @param entry
//	 * @return
//	 * @throws NoSuchFieldException
//	 * @throws SecurityException
//	 * @throws IllegalArgumentException
//	 * @throws IllegalAccessException
//	 * @throws JSONException
//	 */
//	@Name("delete_entry_in_dicttree")
//	@Description("Delete entry in the DictTree.")
//	@PluginTarget(GraphDatabaseService.class)
//	public boolean deleteEntryInDictTree(@Source GraphDatabaseService graphDb, @Description("Name of the dictionary.") @Parameter(name = "name") String name,
//			@Description("Entry to delete") @Parameter(name = "entry") String entry) throws NoSuchFieldException, SecurityException, IllegalArgumentException,
//			IllegalAccessException, JSONException {
//
//		// Initialisierung des Graphens
//		Transaction tx = graphDb.beginTx();
//
//		Node root = getRootOfDictTree(graphDb, name);
//
//		Node node = getExactNode(graphDb, root, entry);
//
//		if (node != null) {
//
//			boolean original = true;
//
//			Deque<Node> queue = new ArrayDeque<Node>();
//			queue.add(node);
//
//			ArrayList<Node> failNodes = new ArrayList<Node>();
//
//			// Lösche zu nächste alle Knoten am äußeren Ränd
//			while (!queue.isEmpty()) {
//
//				node = queue.pop();
//
//				if (isBoundaryNode(graphDb, node, root, entry)) {
//
//					Iterator<Relationship> iterRel = node.getRelationships(Direction.INCOMING).iterator();
//
//					while (iterRel.hasNext()) {
//						Relationship rel = iterRel.next();
//
//						if (!rel.getType().name().equals(EdgeTypes.FAIL.name())) {
//							queue.add(rel.getStartNode());
//						} else {
//							failNodes.add(rel.getStartNode());
//						}
//
//						rel.delete();
//					}
//
//					node.getSingleRelationship(EdgeTypes.FAIL, Direction.OUTGOING).delete();
//					node.delete();
//
//					original = false;
//				}
//			}
//
//			// Eintrag löschen, wenn Knoten auf der Strecke liegt
//			if (!isBoundaryNode(graphDb, node, root, entry) && original) {
//
//				Stack<Node> stackNodeFail = new Stack<Node>();
//				stackNodeFail.add(node);
//
//				while (!stackNodeFail.isEmpty()) {
//					Node relToNodeFail = stackNodeFail.pop();
//
//					Iterator<Relationship> iterRelFail = relToNodeFail.getRelationships(Direction.INCOMING, EdgeTypes.FAIL).iterator();
//
//					while (iterRelFail.hasNext()) {
//						stackNodeFail.add(iterRelFail.next().getStartNode());
//					}
//
//					deleteOutput(relToNodeFail, entry);
//				}
//
//			}
//
//			// Neu Failrelationen setzen
//			Collections.sort(failNodes, new Deepth());
//
//			for (int i = 0; i < failNodes.size(); i++) {
//
//				Node nodeFail = failNodes.get(i);
//
//				// Delete all
//				if (nodeFail.hasProperty(OUTPUT)) {
//
//					Stack<Node> stackNodeFail = new Stack<Node>();
//					stackNodeFail.add(nodeFail);
//
//					while (!stackNodeFail.isEmpty()) {
//						Node relToNodeFail = stackNodeFail.pop();
//
//						Iterator<Relationship> iterRelFail = relToNodeFail.getRelationships(Direction.INCOMING, EdgeTypes.FAIL).iterator();
//
//						while (iterRelFail.hasNext()) {
//							stackNodeFail.add(iterRelFail.next().getStartNode());
//						}
//
//						deleteOutput(relToNodeFail, entry);
//					}
//				}
//
//				Iterator<Relationship> iterRelNodeBefore = nodeFail.getRelationships(Direction.INCOMING).iterator();
//				Relationship rela = null;
//				while (iterRelNodeBefore.hasNext()) {
//					rela = iterRelNodeBefore.next();
//					if (!rela.getType().name().equals(EdgeTypes.FAIL.name())) {
//						node = rela.getStartNode();
//						break;
//					}
//				}
//
//				Node state = getFailNode(node);
//				Node toNode = null;
//
//				while ((toNode = getNextNodeSearch(graphDb, name, state, rela.getType())) == null) {
//					state = getFailNode(state);
//				}
//
//				nodeFail.createRelationshipTo(toNode, EdgeTypes.FAIL);
//
//				// UPDATE DER ANDEREN FAIL-VERBINDUNGEN !!
//				Stack<Node> stackNodeOutput = new Stack<Node>();
//				stackNodeOutput.add(nodeFail);
//
//				while (!stackNodeOutput.isEmpty()) {
//					Node relToNodeFail = stackNodeOutput.pop();
//
//					Iterator<Relationship> iterRelFail = relToNodeFail.getRelationships(Direction.INCOMING, EdgeTypes.FAIL).iterator();
//
//					while (iterRelFail.hasNext()) {
//						stackNodeOutput.add(iterRelFail.next().getStartNode());
//					}
//
//					unionOutput(relToNodeFail, toNode);
//				}
//
//			}
//
//		}
//
//		tx.success();
//		tx.finish();
//
//		return node != null;
//
//	}
//
//	/******** GET ALL ENTRIES ********************/
//
//	// TODO: Implementieren --- KOMMENTIEREN!!!
//	/**
//	 * 
//	 * @param graphDb
//	 * @param name
//	 * @return
//	 * @deprecated
//	 */
//	public ListRepresentation getAllEntriesInDictTree(@Source GraphDatabaseService graphDb, @Description("Name of the dictionary.") @Parameter(name = "name") String name) {
//		return null;
//	}
//
//	/******* GET/CHANGE/ADD/DELETE ATTRIBUTES OF ENTRY ************************/
//
//	/**
//	 * Returns a list of attribute names
//	 * 
//	 * @param graphDb
//	 *            - Graphdatabase
//	 * @param name
//	 *            - Name of the DictTree
//	 * @param entry
//	 *            - Name of the entry for the attributes
//	 * @return list with attribute names
//	 * @throws SecurityException
//	 * @throws NoSuchFieldException
//	 * @throws IllegalArgumentException
//	 * @throws IllegalAccessException
//	 * @throws JSONException
//	 */
//	@SuppressWarnings("unchecked")
//	@Name("get_all_attributes_of_entry")
//	@Description("Get a list of all Attributes of an entry.")
//	@PluginTarget(GraphDatabaseService.class)
//	public ListRepresentation getAllAttributesOfEntry(@Source GraphDatabaseService graphDb, @Description("Name of the dictionary.") @Parameter(name = "name") String name,
//			@Description("Entry to Change") @Parameter(name = "entry") String entry) throws SecurityException, NoSuchFieldException, IllegalArgumentException,
//			IllegalAccessException, JSONException {
//
//		// Find root for DictTree
//		Node root = getRootOfDictTree(graphDb, name);
//
//		// Check if root exists, If not return null
//		if (root != null) {
//			// Find node with entry
//			Node node = getExactNode(graphDb, root, entry);
//			// Check of a output in node exist
//			if (toLong(node.getProperty(NUMBER_OUTPUT)) > 0) {
//				// Get all Attributenames for the first output of the node
//				JSONObject jsonOb = new JSONObject(String.valueOf(node.getProperty(OUTPUT)));
//				JSONObject attributes = (JSONObject) jsonOb.get(entry);
//				String[] keys = new String[attributes.length()];
//				Iterator<String> iterKeys = attributes.keys();
//				int j = 0;
//				while (iterKeys.hasNext()) {
//					keys[j] = iterKeys.next();
//					j++;
//				}
//				return ListRepresentation.strings(keys);
//			}
//		}
//
//		// Returns null, if no root exists
//		return null;
//	}
//
//	/**
//	 * Adds attributes to an entry of the DictTree. The attributes has to be in map in the form {att1: attVal1, att2: attVal2, ...}
//	 * 
//	 * @param graphDb
//	 *            - GraphDatabase
//	 * @param name
//	 *            - Name of the DictTree
//	 * @param entry
//	 *            - Name of the Entry where new attributes has to be set
//	 * @param attributeMap
//	 *            - Map auf attributes for the entry
//	 * @return <code>true</code> all attributes could be add, <code>false</code> if one attribute already exist
//	 * @throws JSONException
//	 */
//	@Name("add_attribute_to_entry")
//	@Description("Adds a map of attributes to the DictTree for an entry.")
//	@PluginTarget(GraphDatabaseService.class)
//	public boolean addAttributeToEntry(@Source GraphDatabaseService graphDb, @Description("Name of the dictionary.") @Parameter(name = "name") String name,
//			@Description("Entry to change") @Parameter(name = "entry") String entry, @Description("Attributemap to change") @Parameter(name = "dictEntries") String attributeMap)
//			throws JSONException {
//
//		return editAttribute(graphDb, name, entry, attributeMap, ADD_ATTRIBUTE);
//	}
//
//	/**
//	 * Deletes Attributes for an entry in the DictTree. The Function needs the names of the attributes as a list. It will returns false, if a attribute is missing, but their will
//	 * be no stop in the process!!!
//	 * 
//	 * @param graphDb
//	 *            - GraphDatabase
//	 * @param name
//	 *            - Name of the DictTree
//	 * @param entry
//	 *            - Name of the Entry where the attributes has to be deleted
//	 * @param attributeName
//	 *            - A List of Strings with the names of the attributes which has to be deleted.
//	 * @return <code>true</code> if success, <code>false</code> if an attribute does not exist
//	 * @throws JSONException
//	 */
//	@Name("delete_attribute_of_entry")
//	@Description("Deletes Attributes of an entry in a DictTree.")
//	@PluginTarget(GraphDatabaseService.class)
//	public boolean deleteAttributeOfEntry(@Source GraphDatabaseService graphDb, @Description("Name of the dictionary.") @Parameter(name = "name") String name,
//			@Description("Entry to change") @Parameter(name = "entry") String entry,
//			@Description("Attributelist of names which should be deleted") @Parameter(name = "attributeName") String attributeName) throws JSONException {
//
//		return editAttribute(graphDb, name, entry, attributeName, DELETE_ATTRIBUTE);
//	}
//
//	/**
//	 * Change attributes of an entry in the DictTree. The function get this attributes as a map where the key is the name of the attribute which has to be change and the value is
//	 * the value which should be new. Example: {att1: attVal1, att2: attVal2} NO BREAK UP IF AN ATTRIBUTE DOES NOT EXIST!!!
//	 * 
//	 * @param graphDb
//	 *            - GraphDatabase
//	 * @param name
//	 *            - Name of the DictTree
//	 * @param entry
//	 *            - Name of the entry
//	 * @param attribute
//	 *            - Map of the attributes which has to be changed with the new value
//	 * @return <code>true</code> if success, <code>false</code> if the attribute does not exist
//	 * @throws JSONException
//	 */
//	@Name("change_attribute_of_entry")
//	@Description("Change Attributevalues of an entry in a DictTree.")
//	@PluginTarget(GraphDatabaseService.class)
//	public boolean changeAttributesOfEntry(@Source GraphDatabaseService graphDb, @Description("Name of the dictionary.") @Parameter(name = "name") String name,
//			@Description("Entry to change") @Parameter(name = "entry") String entry, @Description("Attributemap to change") @Parameter(name = "dictEntries") String attribute)
//			throws JSONException {
//
//		return editAttribute(graphDb, name, entry, attribute, CHANGE_ATTRIBUTE);
//	}
//
//	@SuppressWarnings("unchecked")
//	private boolean editAttribute(GraphDatabaseService graphDb, String name, String entry, String attribute, int modus) throws JSONException {
//		// Initialization of the transaction of the graph
//		Transaction tx = graphDb.beginTx();
//		boolean success = true;
//
//		// Find the root of the DictTree
//		Node root = getRootOfDictTree(graphDb, name);
//
//		if (root != null) {
//			// Find original node
//			Node node = getExactNode(graphDb, root, entry);
//
//			// Parse Attribute Map
//			JSONObject jsonOb = null;
//
//			// Change Properties in the original node
//			if (node != null) {
//				jsonOb = new JSONObject(String.valueOf(node.getProperty(OUTPUT)));
//				JSONObject attributesJson = (JSONObject) jsonOb.get(entry);
//
//				switch (modus) {
//				// Delete Attribut
//				case DELETE_ATTRIBUTE:
//					JSONArray attributArray = new JSONArray(attribute);
//					for (int i = 0; i < attributArray.length(); i++) {
//						attributesJson.remove(attributArray.getString(i));
//					}
//					break;
//				// Change and Add Attribut
//				default:
//					JSONObject attribut = new JSONObject(attribute);
//					Iterator<String> iterAtt = attribut.keys();
//					while (iterAtt.hasNext()) {
//						String key = iterAtt.next();
//						attributesJson.remove(key);
//						attributesJson.putOnce(key, String.valueOf(attribut.get(key)));
//					}
//					break;
//				}
//
//				jsonOb.remove(entry);
//				jsonOb.putOnce(entry, attributesJson);
//
//			} else {
//				tx.success();
//				tx.finish();
//				return false;
//			}
//
//			// Change the properties of the node which are connected with a fail
//			// relation to the node
//			Deque<Node> queue = new ArrayDeque<Node>();
//			queue.add(node);
//
//			while (!queue.isEmpty()) {
//
//				Node startNode = queue.pop();
//				JSONObject jsonObOut = new JSONObject(String.valueOf(startNode.getProperty(OUTPUT)));
//				jsonObOut.putOpt(entry, jsonOb.get(entry));
//				startNode.setProperty(OUTPUT, jsonObOut.toString());
//
//				Iterator<Relationship> iterRel = startNode.getRelationships(Direction.INCOMING).iterator();
//				while (iterRel.hasNext()) {
//					Relationship rel = iterRel.next();
//					if (rel.isType(EdgeTypes.FAIL)) {
//						Node failNode = rel.getStartNode();
//						queue.add(failNode);
//					}
//				}
//			}
//
//		}
//
//		tx.success();
//		tx.finish();
//
//		return success;
//	}
//
//	/**
//	 * Find the node where the entry was saved first and is the main output!
//	 * 
//	 * @param graphDb
//	 *            - GraphDatabase
//	 * @param root
//	 *            - Root of the DictTree
//	 * @param entry
//	 *            - Name of the entry which has to be found in the DictTree
//	 * @return The node where the entry has the id 0, so is the main output
//	 */
//	private Node getExactNode(GraphDatabaseService graphDb, Node root, String entry) {
//
//		Node node = root;
//
//		for (int i = 0; i < entry.length(); i++) {
//			node = getNextNodeCreate(graphDb, String.valueOf(root.getProperty(DICTIONARY_NAME)), node, entry.substring(i, i + 1));
//			if (node == null) {
//				return null;
//			}
//		}
//
//		return node;
//	}
//
//	// ///////////////////////////////////////////////////////////////////
//	// PUBLIC FUNCTION FOR SEARCHING IN THE DICTTREE
//
//	/**
//	 * Returns the first match of a query for a DictTree. So if the query is "Monday he likes her shoes." And the DictTree is {he, she, his, her} then it will return "he" with his
//	 * attributes.
//	 * 
//	 * @param graphDb
//	 *            - GraphDatabase
//	 * @param name
//	 *            - Name of the DictTree
//	 * @param query
//	 *            - the Query for searching
//	 * @return the first longest match in the query
//	 */
//	@Name("progressiv_search")
//	@Description("Get the first match for the query.")
//	@PluginTarget(GraphDatabaseService.class)
//	public ListRepresentation progressivSearch(@Source GraphDatabaseService graphDb, @Description("Name of the dictionary.") @Parameter(name = "name") String name,
//			@Description("Query to look for substrings") @Parameter(name = "query") String query) {
//
//		try {
//			List<Map<String, Object>> matchListRep = search(graphDb, name, query, PROGRESSIV);
//			return listToRepresentation(matchListRep);
//		} catch (Exception e) {
//			return null;
//		}
//	}
//
//	/**
//	 * Returns all matches of the query with the DictTree. So if for example the query is "she goes swimming" and the dictionary is {she, he, swim, go} the function will find all 4
//	 * entries of the dictionary
//	 * 
//	 * @param graphDb
//	 *            - GraphDatabase
//	 * @param name
//	 *            - Name of the DictTree
//	 * @param query
//	 *            - the Query for searching
//	 * @return List of all matches
//	 */
//	@Name("complete_search")
//	@Description("Get all matches of the query with the dictionary.")
//	@PluginTarget(GraphDatabaseService.class)
//	public ListRepresentation completeSearch(@Source GraphDatabaseService graphDb, @Description("Name of the dictionary.") @Parameter(name = "name") String name,
//			@Description("Query to look for substrings") @Parameter(name = "query") String query) {
//
//		try {
//			List<Map<String, Object>> matchListRep = search(graphDb, name, query, FULL_WITH_OVERLAPPING);
//			return listToRepresentation(matchListRep);
//		} catch (Exception e) {
//			return null;
//		}
//	}
//
//	/**
//	 * Returns all matches of the query with the DictTree but without overlapping. So if for example the query is "she goes swimming" and the dictionary is {she, he, swim, go} the
//	 * function will find {she, swim, go}. The function will not return he, because it's in the word she.
//	 * 
//	 * @param graphDb
//	 *            - GraphDatabase
//	 * @param name
//	 *            - Name of the DictTree
//	 * @param query
//	 *            - the Query for searching
//	 * @return List of matches
//	 * @throws NoSuchFieldException
//	 * @throws SecurityException
//	 * @throws IllegalArgumentException
//	 * @throws IllegalAccessException
//	 */
//	@Name("complete_search_without_overlapping")
//	@Description("Get all matches of the query with the dictionary but without overlapping.")
//	@PluginTarget(GraphDatabaseService.class)
//	public ListRepresentation completeSearchWithoutOverlappingResults(@Source GraphDatabaseService graphDb,
//			@Description("Name of the dictionary.") @Parameter(name = "name") String name, @Description("Query to look for substrings") @Parameter(name = "query") String query)
//			throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
//
//		try {
//			List<Map<String, Object>> matchListRep = search(graphDb, name, query, FULL_WITHOUT_OVERLAPPING);
//
//			return listToRepresentation(matchListRep);
//		} catch (Exception e) {
//			return null;
//		}
//	}
//
//	/**
//	 * Returns all matches of the query with the DictTree but without overlapping. So if for example the query is "she goes swimming" and the dictionary is {she, he, swim, go} the
//	 * function will find {she, swim, go}. The function will not return he, because it's in the word she.
//	 * 
//	 * @param graphDb
//	 *            - GraphDatabase
//	 * @param name
//	 *            - Name of the DictTree
//	 * @param query
//	 *            - the Query for searching
//	 * @return List of matches
//	 * @throws NoSuchFieldException
//	 * @throws SecurityException
//	 * @throws IllegalArgumentException
//	 * @throws IllegalAccessException
//	 * @throws JSONException
//	 */
//	@Name("complete_bulk_search_without_overlapping")
//	@Description("Get all matches of the query with the dictionary but without overlapping.")
//	@PluginTarget(GraphDatabaseService.class)
//	public ListRepresentation completeBulkSearchWithoutOverlappingResults(@Source GraphDatabaseService graphDb,
//			@Description("Name of the dictionary.") @Parameter(name = "name") String name,
//			@Description("Queries to look for substrings") @Parameter(name = "queries") String queries) throws NoSuchFieldException, SecurityException, IllegalArgumentException,
//			IllegalAccessException, JSONException {
//		JSONArray queriesArray = new JSONArray(queries);
//		List<ListRepresentation> results = new ArrayList<>(queriesArray.length());
//		for (int i = 0; i < queriesArray.length(); i++) {
//			String query = queriesArray.getString(i);
//			ListRepresentation result = completeSearchWithoutOverlappingResults(graphDb, name, query);
//			results.add(result);
//
//		}
//		return new ListRepresentation(RepresentationType.MAP, results);
//	}
//
//	/**
//	 * Implementation of the algorithm 1 in the paper of ahoCorasick See <a href="http://dl.acm.org/citation.cfm?doid=360825.360855">article</a>
//	 * 
//	 * @param graphDb
//	 *            - GraphDatabase
//	 * @param name
//	 *            - Name of the DictTree
//	 * @param query
//	 *            - the Query for searching
//	 * @param mode
//	 *            - PROGRESSIV, FULL_WITH_OVERLAPPING, FULL_WITHOUT_OVERLAPPING
//	 * @return List of maps which contains start, end, node and match
//	 * @throws JSONException
//	 */
//	@SuppressWarnings("unchecked")
//	private List<Map<String, Object>> search(GraphDatabaseService graphDb, String name, String query, int mode) throws JSONException {
//
//		// List for all matches in the query
//		List<Map<String, Object>> matchList = new ArrayList<Map<String, Object>>();
//
//		// Getting root of the DictTree
//		Node currentState = getRootOfDictTree(graphDb, name);
//		// If Null return null
//		if (currentState == null) {
//			return null;
//		}
//
//		// If Tree is not prepared for searching return null
//		if (!isDictTreePrepared(currentState)) {
//			return null;
//		}
//
//		// Letter of the query
//		String letter;
//
//		// Constants for the non overlapping search mode
//		long beginLast = 0;
//		long endLast = 0;
//		Map<String, Object> matchOld = null;
//
//		// Search letter by letter in the DictTree
//		for (int i = 0; i < query.length(); i++) {
//			// Getting next letter in the query
//			letter = query.substring(i, i + 1);
//
//			Node toNode = null;
//			// As long their is no relation with name of the letter get failNode
//			while ((toNode = getNextNodeSearch(graphDb, name, currentState, withName(letter))) == null) {
//				currentState = getFailNode(currentState);
//			}
//			currentState = toNode;
//
//			int numberFounds = (Integer) currentState.getProperty(NUMBER_OUTPUT);
//
//			// If current state has a output then get this output
//			if (numberFounds > 0) {
//
//				// Constants important for Overlapping and Progessiv search
//				long beginLongest = 0;
//				long endLongest = 0;
//				Map<String, Object> matchLongest = null;
//
//				// Get each output of the node
//				JSONObject jsonObOut = new JSONObject(String.valueOf(currentState.getProperty(OUTPUT)));
//
//				Iterator<String> iterOut = jsonObOut.keys();
//				while (iterOut.hasNext()) {
//					String key = iterOut.next();
//					Object attributesEl = jsonObOut.get(key);
//
//					if (attributesEl.getClass().equals(JSONObject.class)) {
//						JSONObject attributesJsonOb = (JSONObject) attributesEl;
//
//						Map<String, Object> entry = new HashMap<String, Object>();
//						entry.put(ENTRY, key);
//
//						Map<String, String> attributes = new HashMap<String, String>();
//
//						Iterator<String> iterAtt = attributesJsonOb.keys();
//						while (iterAtt.hasNext()) {
//							String keyAt = iterAtt.next();
//							attributes.put(keyAt, attributesJsonOb.getString(keyAt).replaceAll("\"", ""));
//						}
//						entry.put(ATTRIBUTES, attributes);
//
//						// Determine begin and end of the match
//						long begin = i + 1 - key.length();
//						long end = i + 1;
//
//						// Create Match Map for the match
//						Map<String, Object> match = new HashMap<String, Object>();
//						match.put(BEGIN, String.valueOf(begin));
//						match.put(END, String.valueOf(end));
//						match.put(MATCH, entry);
//						match.put(NODE, currentState);
//
//						switch (mode) {
//						// WITH Overlapping
//						case FULL_WITH_OVERLAPPING:
//							matchList.add(match);
//							break;
//
//						default:
//							// Look which is the longest match in the node
//							if ((end - begin) > (endLongest - beginLongest)) {
//								beginLongest = begin;
//								endLongest = end;
//								matchLongest = match;
//							}
//						}
//
//					}
//				}
//
//				switch (mode) {
//				case FULL_WITHOUT_OVERLAPPING:
//					// Is the current match overlapping to a match before
//					// and if so which match is longer? The longest match
//					// is here important
//					if (endLast <= beginLongest) {
//						beginLast = beginLongest;
//						endLast = endLongest;
//
//						if (matchOld != null)
//							matchList.add(matchOld);
//
//						matchOld = matchLongest;
//
//						break;
//					}
//
//					if (beginLongest <= endLast) {
//						if ((endLast - beginLast) < (endLongest - beginLongest)) {
//							beginLast = beginLongest;
//							endLast = endLongest;
//							matchOld = matchLongest;
//						}
//					}
//					break;
//				case PROGRESSIV:
//					// Return first longest match
//					matchList.add(matchLongest);
//					return matchList;
//				default:
//					break;
//				}
//			}
//		}
//
//		// Adds the last match
//		if (mode == FULL_WITHOUT_OVERLAPPING) {
//			matchList.add(matchOld);
//		}
//
//		return matchList;
//	}
//
//	// ///////////////////////////////////////////////////////////////////
//	// PRIVATE FUNCTION = HILFSFUNKTIONEN
//
//	/**
//	 * Searching for the root of a DictTree. It's not allowed to have a DictTree with the same name/id. So the function will give back just the first root which it finds in the
//	 * database
//	 * 
//	 * @param graphDb
//	 *            the GraphDatabaseService
//	 * @param name
//	 *            - id of the DictTree
//	 * @return Root node of the DictTree
//	 */
//	private Node getRootOfDictTree(GraphDatabaseService graphDb, String name) {
//
//		IndexManager indMan = graphDb.index();
//		Index<Node> index = indMan.forNodes(INDEX_DIC);
//
//		IndexHits<Node> indexHit = index.get(DICTIONARY_NAME, name);
//		return indexHit.getSingle();
//
//	}
//
//	/**
//	 * Determines whether the DictTree already has his Fail-Relations or not
//	 * 
//	 * @param root
//	 *            - Root of the DictTree
//	 * @return true or false
//	 */
//	private boolean isDictTreePrepared(Node root) {
//		return Boolean.valueOf(String.valueOf(root.getProperty(PREPARED)));
//	}
//
//	/**
//	 * 
//	 * @param graphDb
//	 * @param node
//	 * @param prepared
//	 * @return
//	 */
//	private boolean isBoundaryNode(GraphDatabaseService graphDb, Node node, Node root, String entry) {
//
//		if (root.equals(node)) {
//			return false;
//		}
//
//		if (toLong(node.getProperty(NUMBER_OUTPUT)) > 0 & node.hasProperty(ORIGINAL)) {
//			if (!node.getProperty(ORIGINAL).equals(entry)) {
//				return false;
//			}
//		}
//
//		Iterator<Relationship> iterRel = node.getRelationships(Direction.OUTGOING).iterator();
//
//		int count = 0;
//
//		while (iterRel.hasNext() && count < 2) {
//			iterRel.next();
//			count++;
//		}
//
//		boolean prepared = isDictTreePrepared(root);
//
//		if (prepared && count == 1) {
//			return true;
//		}
//
//		if (count == 0) {
//			return true;
//		}
//
//		return false;
//
//	}
//
//	/**
//	 * Gibt den nächsten Knoten zurück, der mit dem Knoten die verbunden ist, über den gegeben Buchstaben. Sollte kein Knoten existieren, der mit dem Knoten über den Buchstaben
//	 * verbunden ist, wird NULL zurückgegeben. Bsp: 1 --R--> 2 .... Knoten mit der Idee 1 wird übergeben und R ist der Typ der Verbindung zwischen 1 und 2. Wenn nach der Verbindung
//	 * R gesucht wird, erhält man Knoten 2 zurück. Falls nach einem anderen Typ gesucht wird, erhält mann Null
//	 * 
//	 * Der Unterschied zwischen CREATE und SEARCH Mode ist, dass CREATE immer NULL zurückgibt, wenn kein Knoten gefunden wird. Bei SEARCH wird noch geprüft, ob es sich beim Knoten
//	 * um die Wurzel handelt. Falls ja, wird die Wurzel wieder zurückgegben, sonst NULL.
//	 * 
//	 * @param node
//	 *            - Ausgangsknoten
//	 * @param letter
//	 *            - Verbindungstyp
//	 * @param mode
//	 *            - Such- oder Creation-Mode
//	 * @return
//	 */
//	private Node getNextNodeCreate(GraphDatabaseService graphDb, String dictName, final Node node, String letter) {
//
//		Relationship rel = node.getSingleRelationship(withName(letter), Direction.OUTGOING);
//		if (rel != null) {
//			return rel.getEndNode();
//		}
//
//		return null;
//	}
//
//	private Node getNextNodeSearch(GraphDatabaseService graphDb, String dictName, final Node node, RelationshipType type) {
//		ReadableIndex<Relationship> autoRelIndex = graphDb.index().getRelationshipAutoIndexer().getAutoIndex();
//		
//		StringBuilder key = new StringBuilder(dictName);
//		key.append(node.getProperty(STATE));
//		key.append(type.name());
//		
//		Relationship rel = autoRelIndex.get(LETTER, key.toString()).getSingle();
//		if (rel != null) {
//			return rel.getEndNode();
//		}
//		if (toLong(node.getProperty(STATE)) == 0) {
//			return node;
//		}
//		return null;
//	}
//
//	private HashMap<String, Object> getNodeBefore(final Node node) {
//		Iterator<Relationship> iterRel = node.getRelationships(Direction.INCOMING).iterator();
//		while (iterRel.hasNext()) {
//			Relationship rel = iterRel.next();
//			if (!rel.getType().equals(EdgeTypes.FAIL)) {
//				HashMap<String, Object> map = new HashMap<String, Object>();
//				map.put(RELATION_TYPE, rel.getType());
//				map.put(NODE, rel.getStartNode());
//				return map;
//			}
//		}
//		return null;
//	}
//
//	/**
//	 * Erhält den Knoten, der über die FAIL-Beziehung mit dem Ausgangsknoten verbunden ist.
//	 * 
//	 * @param node
//	 *            - Ausgangsknoten
//	 * @return FAIL-Knoten
//	 */
//	private Node getFailNode(final Node node) {
//		return node.getSingleRelationship(EdgeTypes.FAIL, Direction.OUTGOING).getEndNode();
//	}
//
//	/**
//	 * Fügt zu einen Knoten die Daten hinzu, die Ausgegeben werden sollen, wenn dieser Knoten erreicht wird
//	 * 
//	 * @param node
//	 *            - Knoten
//	 * @param output
//	 *            - Ausgabewort
//	 * @param attributes
//	 *            - Attribute des Ausgabewortes
//	 * @throws JSONException
//	 */
//	private void addOutput(Node node, String output, JSONObject attributes) throws JSONException {
//
//		JSONObject jsonOB = new JSONObject();
//		jsonOB.put(output, attributes);
//
//		// Attribute mit übergeben
//		node.setProperty(ORIGINAL, output);
//		node.setProperty(OUTPUT, jsonOB.toString());
//		node.setProperty(NUMBER_OUTPUT, 1);
//	}
//
//	/**
//	 * Vereinigt die Ausgabe von zwei Knoten auf den ersten Knoten der übergeben wird
//	 * 
//	 * @param node1
//	 *            - Knoten auf den die beiden Ausgaben vereinigt wird
//	 * @param node2
//	 *            - Menge die zur Ausgabe auf Node1 hinzugefügt wird
//	 * @throws JSONException
//	 */
//	@SuppressWarnings("unchecked")
//	private void unionOutput(Node node1, Node node2) throws JSONException {
//
//		if (toLong(node2.getProperty(NUMBER_OUTPUT)) == 0) {
//			return;
//		}
//
//		if (toLong(node1.getProperty(NUMBER_OUTPUT)) == 0) {
//			node1.setProperty(OUTPUT, node2.getProperty(OUTPUT));
//			node1.setProperty(NUMBER_OUTPUT, ((Integer) node1.getProperty(NUMBER_OUTPUT)) + 1);
//		}
//
//		JSONObject jsonObNode1 = new JSONObject(String.valueOf(node1.getProperty(OUTPUT)));
//		JSONObject jsonObNode2 = new JSONObject(String.valueOf(node2.getProperty(OUTPUT)));
//
//		// alles was in node2 als Ausgabe exisitiert wird in node1 gespeichert
//		Iterator<String> keys = jsonObNode2.keys();
//		while (keys.hasNext()) {
//
//			String key = keys.next();
//
//			if (!key.startsWith(PROPERTY) && !jsonObNode1.has(key)) {
//				jsonObNode1.putOnce(key, jsonObNode2.get(key));
//				node1.setProperty(NUMBER_OUTPUT, ((Integer) node1.getProperty(NUMBER_OUTPUT)) + 1);
//			}
//		}
//
//		node1.setProperty(OUTPUT, jsonObNode1.toString());
//	}
//
//	@SuppressWarnings("unchecked")
//	private void subtractOutput(Node node1, Node node2) throws JSONException {
//
//		if (toLong(node2.getProperty(NUMBER_OUTPUT)) == 0) {
//			return;
//		}
//
//		if (toLong(node1.getProperty(NUMBER_OUTPUT)) == 0) {
//			return;
//		}
//
//		JSONObject obNode1 = new JSONObject(String.valueOf(node1.getProperty(OUTPUT)));
//		JSONObject obNode2 = new JSONObject(String.valueOf(node2.getProperty(OUTPUT)));
//
//		// alles was in node2 als Ausgabe exisitiert wird in node1 gespeichert
//		Iterator<String> keys = obNode2.keys();
//		while (keys.hasNext()) {
//
//			String key = keys.next();
//			obNode1.remove(key);
//
//		}
//
//		node1.setProperty(NUMBER_OUTPUT, ((Integer) node1.getProperty(NUMBER_OUTPUT)) - ((Integer) node2.getProperty(NUMBER_OUTPUT)));
//		node1.setProperty(OUTPUT, obNode1.toString());
//	}
//
//	private void deleteOutput(Node node, String output) throws JSONException {
//		JSONObject ob = new JSONObject(String.valueOf(node.getProperty(OUTPUT)));
//		if (ob.has(output)) {
//			ob.remove(output);
//			node.setProperty(OUTPUT, ob.toString());
//			node.setProperty(NUMBER_OUTPUT, ((Integer) node.getProperty(NUMBER_OUTPUT)) - 1);
//		}
//
//		if (node.hasProperty(ORIGINAL)) {
//			if (node.getProperty(ORIGINAL).equals(output)) {
//				node.removeProperty(ORIGINAL);
//			}
//		}
//	}
//
//	// ///////////////////////////////////////////////////////////////////
//	// PRIVATE FUNCTION = CONVERTER
//
//	/**
//	 * Convert Object to Long
//	 * 
//	 * @param ob
//	 * @return
//	 */
//	private long toLong(Object ob) {
//		return Long.valueOf(String.valueOf(ob));
//	}
//
//	/**
//	 * Convert a list of Maps to a ListRepresentation
//	 * 
//	 * @param list
//	 * @return
//	 */
//	private ListRepresentation listToRepresentation(List<Map<String, Object>> list) {
//
//		if (list == null)
//			return null;
//
//		List<Representation> matchList = new ArrayList<Representation>();
//
//		for (int i = 0; i < list.size(); i++) {
//			MappingRepresentation matchRep = new RecursiveMappingRepresentation(Representation.MAP, list.get(i));
//			matchList.add(matchRep);
//		}
//
//		ListRepresentation matchListRep = new ListRepresentation(RepresentationType.MAP, matchList);
//
//		return matchListRep;
//	}
//
//	private class Deepth implements Comparator<Node> {
//
//		private long getDeepth(Node o1) {
//			return toLong(o1.getProperty(DEPTH));
//		}
//
//		@Override
//		public int compare(Node o1, Node o2) {
//			long deepthN1 = getDeepth(o1);
//			long deepthN2 = getDeepth(o2);
//
//			if (deepthN1 - deepthN2 < 0) {
//				return -1;
//			}
//
//			if (deepthN1 - deepthN2 > 0) {
//				return 1;
//			}
//
//			return 0;
//		}
//
//	}

}
