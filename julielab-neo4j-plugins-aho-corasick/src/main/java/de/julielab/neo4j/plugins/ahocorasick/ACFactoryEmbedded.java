package de.julielab.neo4j.plugins.ahocorasick;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.Name;
import org.neo4j.server.plugins.Parameter;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;
import org.neo4j.shell.util.json.JSONArray;
import org.neo4j.shell.util.json.JSONException;
import org.neo4j.shell.util.json.JSONObject;

import de.julielab.neo4j.plugins.ahocorasick.property.ACDictionary;
import de.julielab.neo4j.plugins.ahocorasick.property.ACEntry;

public class ACFactoryEmbedded extends ServerPlugin{

	
	/******** CREATE DICT-TREE *********************/

	/**
	 * Creates a new root of a DictTree, where entries could be add.
	 * 
	 * @param graphDb
	 *            - Database where the Dict-Tree should be created
	 * @param name
	 *            - Name of the Dictionary
	 * @return <code>true</code> if a new root was created, <code>false</code> if a root already exisits
	 * @throws JSONException 
	 */
	@Name("create_dict_tree")
	@Description("Creates a new root of a DictTree, " + "where entries could be add. If root already exists" + "then the function returns false.")
	@PluginTarget(GraphDatabaseService.class)
	public static boolean createDictTree(@Source GraphDatabaseService graphDb, 
			@Description("ACDictionary as string.") @Parameter(name = "name") String jsonACDict) throws JSONException {

		ACDictionary dict = new ACDictionary(new JSONObject(jsonACDict));
		
		// Start Transaction
		Transaction tx = graphDb.beginTx();

		try{
			// Check if a root with the name as DICTIONARY_NAME property already
			// exists
			if (ACUtil.getRootNode(graphDb, dict.name()) == null) {
				
				Node root = graphDb.createNode(ACProperties.LabelTypes.DICTIONARY);
				
				// // Special Properties of the root
				// Set Property dictionry name
				root.setProperty(ACProperties.DICTIONARY_NAME, dict.name());
				// Set the number of nodes in tree to 1
				root.setProperty(ACProperties.NODES_IN_TREE, 1);
				// Set the numbers of entries in this dictionary to 0
				root.setProperty(ACProperties.NUMBER_OF_ENTRIES, 0);
				// Set the tree to be unprepared for searching
				root.setProperty(ACProperties.PREPARED, false);
				// Mode to create
				root.setProperty(ACProperties.MODECREATE, dict.getCreateMode());
				// Mode to search
				root.setProperty(ACProperties.MODESEARCH, dict.isLocalSearch());
				
				// // General Properties of the node
				// Set the state
				root.setProperty(ACProperties.STATE, 0);
				// Set the deepth
				root.setProperty(ACProperties.DEPTH, 0);
				// Set the number of entries, which the node has
				// to return if the system reach the node
				root.setProperty(ACProperties.NUMBER_OUTPUT, 0);
				// Set a JSONObject for all Relationships
				JSONObject jsonRelMap = new JSONObject();
				root.setProperty(ACProperties.RELATIONSHIP, jsonRelMap.toString());
				// Number of Outgoing Relationships
				root.setProperty(ACProperties.NUMBER_NEXT, 0);
				
				// Adds the root to the index of Dictionaries of the GraphDatabase
				// for easier finding
				IndexManager manager = graphDb.index();
				Index<Node> index = manager.forNodes(ACProperties.INDEX_DIC);
				
				index.add(root, ACProperties.DICTIONARY_NAME, dict.name());
				
				tx.success();
				
				// New root was created in the GraphDatabase
				return true;
			}
			
			tx.failure();
			
			// Root already exists in the GraphDatabase
			return false;
			
		}finally{
			tx.close();
		}
	}
	
	/******** DICT-TREE DELETE ***********************/

	/**
	 * Deletes a complete DictTree in a GraphDatabase.
	 * 
	 * @param graphDb
	 *            - GraphDatabase
	 * @param name
	 *            - Name of the Dict-Tree
	 * @return <code>true</code> if the tree is deleted, <code>false</code> if the tree with the name does not exist
	 * @throws IOException
	 */
	@Name("delete_dict_tree")
	@Description("Deletes a complete DictTree in a GraphDatabase.")
	@PluginTarget(GraphDatabaseService.class)
	public static boolean deleteDictTree(@Source GraphDatabaseService graphDb, 
			@Description("Name of the dictionary.") @Parameter(name = "name") String name) throws IOException {

		long groupCounter = 0;
		final long maxGroupCount = 20000;

		// Start Transaction
		Transaction tx = graphDb.beginTx();
		try{
			// Find the root of the Dict-Tree
			Node root = ACUtil.getRootNode(graphDb, name);
			
			// if root equals null, then return false, because root does not exists
			// and so can't be deleted
			if (root == null) {
				tx.failure();
				return false;
			}
			
			// Delete the root in the Dictionary Index of the GraphDatatbase
			IndexManager indMan = graphDb.index();
			Index<Node> index = indMan.forNodes(ACProperties.INDEX_DIC);
			index.remove(root);
			
			// Process to delete all nodes and relations in the tree.
			// So it starts with the root and then all nodes which
			// has a relation to the root and then so on.
			// So each depth of the tree will be deleted step by step.
			Deque<Node> queue = new ArrayDeque<Node>();
			queue.add(root);
			
			while (!queue.isEmpty()) {
				Node node = queue.pop();
				// Get all Outgoint reltions from the current node.
				Iterator<Relationship> iterRe = node.getRelationships(Direction.OUTGOING).iterator();
				
				while (iterRe.hasNext()) {
					
					Relationship rel = iterRe.next();
					
					// Find all nodes which has to be deletet in the next depth and
					// belongs to the current node
					if (!rel.getType().name().equals(ACProperties.getFailName())) {
						queue.add(rel.getEndNode());
					}
					
					// Delete the relation to the next nodes.
					rel.delete();
				}
				
				// Delete the current node.
				node.delete();
				
				groupCounter++;
				
				if (groupCounter >= maxGroupCount) {
					groupCounter = 0;
					tx.success();
					tx.close();
					
					tx = graphDb.beginTx();
				}
			}
			
			tx.success();
			
			return true;
			
		}finally{
			tx.close();
		}

	}
	
	/******** APPEND DICT-TREE *********************/

	/**
	 * Adds a list of entries to a DictTree, which is not prepared for searching
	 * 
	 * @param graphDb
	 *            - GraphDatabase
	 * @param name
	 *            - name of the DictTree
	 * @param listEntries
	 *            - The dictionary entries as a JSON map of the form { 'dictEntries':[{'entry':'entry1','attributes':{'attribute1':'attValue1','attribute2':'attValue2'}}] } The
	 *            Entry is mapped with the constant ENTRY and the Attributes is a Map, which has the key ATRRIBUTES
	 * @return <code>true</code> if all entries could be added, <code>false</code> if the tree is already prepared for searching
	 * @throws JSONException
	 * @throws IOException
	 */
	@Name("add_list_to_dicttree")
	@Description("Adds a list of entries to a DictTree.")
	@PluginTarget(GraphDatabaseService.class)
	public static boolean addListToDictTree(@Source GraphDatabaseService graphDb, 
			@Description("ACDictionary as string.") @Parameter(name = "name") String jsonACDict,
			@Description("The dictionary entries as a JSONArray of the form"
					+ "[{'entry':'entry1','attributes':{'attribute1':'attValue1','attribute2':'attValue2'}}]") 
			@Parameter(name = "dictEntries") String listEntries)
			throws JSONException, IOException {

		// Start Transaction
		Transaction tx = graphDb.beginTx();
		
		try{
			// ACDictionary Pars
			ACDictionary dict = new ACDictionary(new JSONObject(jsonACDict));
			
			// Parse String to JSONArry
			JSONArray entries = new JSONArray(listEntries);
			
			// Get root of DictTree
			Node root = ACUtil.getRootNode(graphDb, dict.name());
			
			// root has to be not equal null
			if (root != null && !ACUtil.isDictTreePrepared(root)) {

				for (int i = 0; i < entries.length(); i++) {
					
					// Get the Map for the entry
					JSONObject entryJSON = entries.getJSONObject(i);
					ACEntry entry = new ACEntry(entryJSON);
					
					// Add the entry with his attributes to the DictTree
					addEntryToTree(graphDb, dict, root, entry);
				}
				
				tx.success();
				return true;
				
			} else {
				tx.failure();
				return false;
			}
		}catch (Exception e) {
			e.printStackTrace();
			tx.failure();
			return false;
		}finally{
			tx.close();
		}

	}

	/**
	 * Adds a entry to the DictTree
	 * 
	 * @param graphDb
	 *            - GraphDatabase
	 * @param root
	 *            - Root of the DictTree
	 * @param word
	 *            - Word which should be add to the DictTree
	 * @throws JSONException
	 */
	private static List<Node> addEntryToTree(GraphDatabaseService graphDb, ACDictionary dict, 
			Node root, ACEntry entry) throws JSONException {

		String[] toAdd = entry.getTokens();
		
		// List for new nodes
		List<Node> nodesNew = new ArrayList<Node>();

		// Get the id number for the next node, which has to be created
		long newState = ACUtil.toLong(root.getProperty(ACProperties.NODES_IN_TREE));

		// Set the root as a starting point for searching the word in the tree
		int j = 0;
		Node node = root;
		Node nodeNext = ACUtil.getNextNodeCreate(dict, graphDb, null, node, toAdd[j]);

		// Lookup for the word in tree until a relation does not exist for a
		// letter
		while (nodeNext != null) {
			node = nodeNext;
			j = j + 1;

			// Check if the whole word already is in the tree
			if (j == toAdd.length)
				break;

			// Try to get the next node for the next letter in the word
			nodeNext = ACUtil.getNextNodeCreate(dict, graphDb, null, node, toAdd[j]);
		}

		// Create new nodes and relations for letter which do not appear
		// currently in the
		// DictTree at the correct place
		for (int p = j; p < toAdd.length; p++) {

			Node newNode = graphDb.createNode();
			JSONObject jsonRelMap = new JSONObject();

			// Set node properties
			newNode.setProperty(ACProperties.STATE, newState);
			newNode.setProperty(ACProperties.NUMBER_OUTPUT, 0);
			newNode.setProperty(ACProperties.DEPTH, p);
			newNode.setProperty(ACProperties.NUMBER_NEXT, 0);
			newNode.setProperty(ACProperties.RELATIONSHIP, jsonRelMap.toString());

			// Create Relation between current and new node
			Relationship rel = node.createRelationshipTo(newNode, ACProperties.EdgeTypes.NEXT);
			rel.setProperty(ACProperties.LETTER, toAdd[p]);
			
			// Update Number Next of Node
			ACUtil.updateAddNode(dict, node, newNode, toAdd[p]);
			ACUtil.increaseNumberNext(node);
			
			nodesNew.add(newNode);
			node = newNode;
			newState = newState + 1;
		}

		// Add word and attribut to the final node
		ACUtil.addOutput(node, entry);

		// Check the Output
		// If the node has already Fail-Relation the Output has to be updated
		if (ACUtil.isDictTreePrepared(root)) {

			// The order is important otherwise the variable node is not the
			// same anymore!!!
			if (node.hasRelationship(ACProperties.EdgeTypes.FAIL, Direction.OUTGOING)) {
				Node nodeEnd = node.getSingleRelationship(ACProperties.EdgeTypes.FAIL, 
						Direction.OUTGOING).getEndNode();
				ACUtil.unionOutput(node, nodeEnd);
			}

			while (node.hasRelationship(ACProperties.EdgeTypes.FAIL, Direction.INCOMING)) {
				Node nodeStart = node.getSingleRelationship(ACProperties.EdgeTypes.FAIL, 
						Direction.INCOMING).getStartNode();
				ACUtil.unionOutput(nodeStart, node);
				node = nodeStart;
			}

		}

		// Update the number of nodes and number of entries in the tree
		root.setProperty(ACProperties.NODES_IN_TREE, newState);
		root.setProperty(ACProperties.NUMBER_OF_ENTRIES, 
				ACUtil.toLong(root.getProperty(ACProperties.NUMBER_OF_ENTRIES)) + 1);

		return nodesNew;
	}
	
	/**
	 * Adds a list of entries to a DictTree, which is prepared for searching
	 * 
	 * @param graphDb
	 *            - GraphDatabase
	 * @param name
	 *            - name of the DictTree
	 * @param dictEntries
	 *            - an array of the form [{ 'entry':'entry1','attributes':{'attribute1':'attValue1','attribute2':'attValue2'}} ,
	 *            {'entry':'entry2','attributes':{'attribute3':'attValue1'}}] The Entry is mapped with the constant ENTRY and the 
	 *            Attributes is a Map, which has the key ATRRIBUTES
	 * @return <code>true</code> if all entries could be added, <code>false</code> if the tree is already prepared for searching
	 * @throws JSONException
	 */
	@Name("add_list_to_prepared_dicttree")
	@Description("Adds entries from a list to the DictTree.")
	@PluginTarget(GraphDatabaseService.class)
	public static boolean addListToPreparedDictTree(@Source GraphDatabaseService graphDb, 
			@Description("ACDictionary as string.") @Parameter(name = "name") String jsonACDict,
			@Description("The dictionary entries as a JSON map of the form"
					+ "{'dictEntries':[{'entry':'entry1','attributes':{'attribute1':'attValue1','attribute2':'attValue2'}}]}") 
			@Parameter(name = "dictEntries") String dictEntries)
			throws JSONException {

		// Start Transaction
		Transaction tx = graphDb.beginTx();

		try{
			ACDictionary dict = new ACDictionary(new JSONObject(jsonACDict));
			
			// New Nodes to add Fail-Relation
			List<Node> nodesNew = new ArrayList<Node>();
			
			// Parse String to JSONArry
			JSONArray entries = new JSONArray(dictEntries);
			
			// Get root of DictTree
			Node root = ACUtil.getRootNode(graphDb, dict.name());
			
			// root has to be not equal null
			if (root != null && ACUtil.isDictTreePrepared(root)) {
				
				for (int i = 0; i < entries.length(); i++) {
					
					// Get the Map for the entry
					JSONObject entryJSON = entries.getJSONObject(i);
					ACEntry entry = new ACEntry(entryJSON);
					
					// Add the entry with his attributes to the DictTree
					nodesNew.addAll(addEntryToTree(graphDb, dict, root, entry));
				}
				
				// sort the nodes in the list according the depth in the tree
				Comparator<Node> comper = ACUtil.getComperator();
				// Sort ascending by depth
				Collections.sort(nodesNew, comper);
				
				// Step by step setting new Failrelation
				for (int i = 0; i < nodesNew.size(); i++) {
					Node node = nodesNew.get(i);
					
					// Get Information about the node before and the relation from
					// it to the current node
					HashMap<String, Object> nodeBeforeInfo = getNodeBefore(node);
					Node nodeBefore = (Node) nodeBeforeInfo.get(ACProperties.STATE);
					String relLetter = (String) nodeBeforeInfo
							.get(ACProperties.RELATION_TYPE);
					
					// Set the Fail-Relation as known in the Aho-Corasick Paper
					if (nodeBefore.equals(root)) {
						node.createRelationshipTo(root, 
								ACProperties.EdgeTypes.FAIL);
						// UPDATE NODES
						ACUtil.updateAddNode(dict, node, root, ACProperties.getFailName());
						ACUtil.increaseNumberNext(node);
					} else {
						// Get Fail Node of Node before
						Node state = null;
						
						if(dict.isLocalSearch()){
							state = graphDb.getNodeById(ACUtil.getFailNode(nodeBefore, root));
						}else{
							if(nodeBefore.getId() != root.getId()){
								state = nodeBefore.getSingleRelationship(ACProperties.EdgeTypes.FAIL, Direction.OUTGOING).getEndNode();
							}else{
								state = root;
							}
						}
						
						// As long the fail node has no relation with the name of
						// the current relation
						// get the next fail node of the current fail node
						// If the next fail node is the root, the while loop will
						// stop
						
						Node toNode = null;
						
						while ((toNode = ACUtil.getNextNodeSearch(dict, graphDb, state, relLetter)) == null) {
							state = graphDb.getNodeById(ACUtil.getFailNode(state, root));
						}
						
						// Set the fail relation
						node.createRelationshipTo(toNode, 
								ACProperties.EdgeTypes.FAIL);
						// UPDATE
						ACUtil.updateAddNode(dict, node, toNode, ACProperties.getFailName());
						ACUtil.increaseNumberNext(node);
						ACUtil.unionOutput(node, toNode);
						
						// Update
						if(toNode.getId() != root.getId()){
							JSONObject jsonRelMap = new JSONObject(
									String.valueOf(node.getProperty(ACProperties.RELATIONSHIP)));
							jsonRelMap.putOpt(ACProperties.getFailName(), toNode.getId());
							node.setProperty(ACProperties.RELATIONSHIP, jsonRelMap.toString());
						}
					}
					
					// Now check the other Fail-Relation whether have to be updated
					// or not
					Stack<Node> stack = new Stack<Node>();
					
					// Get all nodes from the Incoming Fail-Relation of the
					// nodeBefore
					Iterator<Relationship> iterFailRelations = nodeBefore.getRelationships(Direction.INCOMING, 
							ACProperties.EdgeTypes.FAIL).iterator();
					while (iterFailRelations.hasNext()) {
						stack.add(iterFailRelations.next().getStartNode());
					}
					
					// Now check whether they have a relation with the current
					// letter
					// if not check whether they have Incoming Fail-Relation and add
					// them to the stack
					while (!stack.isEmpty()) {
						
						Node n = stack.pop();
						Node nEnd = null;
						// Check if a relation with the current letter exist
						if ((nEnd = ACUtil.getNextNodeCreate(dict, graphDb, root, n, relLetter)) != null 
								&& nEnd.hasRelationship(ACProperties.EdgeTypes.FAIL, 
										Direction.OUTGOING)) {
							
							ACUtil.subtractOutput(nEnd, nEnd.getSingleRelationship(ACProperties.EdgeTypes.FAIL, 
									Direction.OUTGOING).getEndNode());
							nEnd.getSingleRelationship(ACProperties.EdgeTypes.FAIL, 
									Direction.OUTGOING).delete();
							nEnd.createRelationshipTo(node, 
									ACProperties.EdgeTypes.FAIL);
							ACUtil.unionOutput(nEnd, node);
							
							// Update
							ACUtil.updateAddNode(dict, nEnd, node, ACProperties.getFailName());
							
							JSONObject jsonRelMap = new JSONObject(
									String.valueOf(nEnd.getProperty(ACProperties.RELATIONSHIP)));
							if(jsonRelMap.has(ACProperties.getFailName())){
								jsonRelMap.remove(ACProperties.getFailName());
							}
							if(node.getId() != root.getId()){
								jsonRelMap.putOnce(ACProperties.getFailName(), node.getId());
							}
							nEnd.setProperty(ACProperties.RELATIONSHIP, jsonRelMap.toString());
							
							// If not does a incoming Fail-Relation exist
						} else if (n.hasRelationship(Direction.INCOMING, 
								ACProperties.EdgeTypes.FAIL)) {
							
							iterFailRelations = n.getRelationships(Direction.INCOMING, 
									ACProperties.EdgeTypes.FAIL).iterator();
							while (iterFailRelations.hasNext()) {
								stack.add(iterFailRelations.next().getStartNode());
							}
						}
					}
				}
				
				tx.success();
				return true;
				
			} else {
				
				tx.failure();
				return false;
			}
		}finally{
			tx.close();
		}

	}
	
	/**
	 * Prepares the DictTree for searching. It sets all fail relations like in the algorithm 3 of the paper from 1975. See <a
	 * href="http://dl.acm.org/citation.cfm?doid=360825.360855">article</a>
	 * 
	 * @param graphDb
	 *            - Graphdatabase
	 * @param name
	 *            - Name of the dictionary
	 * @return <code>true</code> if the tree is prepared, <code>false</code> if tree does not exist
	 * @throws IOException
	 * @throws JSONException
	 */
	@Name("prepare_dicttree_for_search")
	@Description("Sets all Fail-Relation in the DictTree for searching.")
	@PluginTarget(GraphDatabaseService.class)
	public static boolean prepareDictTreeForSearch(@Source GraphDatabaseService graphDb, 
			@Description("ACDictionary as string.") @Parameter(name = "name") String jsonACDict)
			throws IOException, JSONException {

		// Start Transaction
		Transaction tx = graphDb.beginTx();
		long groupCounter = 0;
		final long maxGroupCount = 20000;
		
		try {
			ACDictionary dict = new ACDictionary(new JSONObject(jsonACDict));
			// Find root for DictTree
			Node root = ACUtil.getRootNode(graphDb, dict.name());
			int counter = 0;
			
			// Check whether DictTree exist or not
			if (ACUtil.isDictTreePrepared(root)) {
				return false;
			}
			
			// Set root as a starting point for the process
			// List has to be "First In - First Out" so that the algorith look
			// up
			// level by level (depth by depth) the tree!
			
			if(dict.getCreateMode()!=ACDictionary.DEFAULT_MODE_CREATE){
				/**************** LOCAL VERSION *********************************/
				Deque<Long> queue = new ArrayDeque<Long>();
				Iterator<Relationship> iter = root.getRelationships(Direction.OUTGOING).iterator();
				
				while (iter.hasNext()) {
					// Create Relationship
					Relationship rel = iter.next();
					Node endNode = rel.getEndNode();
					
					endNode.createRelationshipTo(root, ACProperties.EdgeTypes.FAIL);
					queue.add(endNode.getId());
					
					// Update the EndNode only if the FAIL Relation goes not to the root
					// See NOTE at the beginning of the method
					endNode.setProperty(ACProperties.NUMBER_NEXT, 
							ACUtil.toLong(endNode.getProperty(ACProperties.NUMBER_NEXT))+1);
					
					JSONObject jsonRelMap = new JSONObject(
							String.valueOf(endNode.getProperty(ACProperties.RELATIONSHIP)));
					jsonRelMap.putOnce(ACProperties.getFailName(), root.getId());
					endNode.setProperty(ACProperties.RELATIONSHIP, jsonRelMap.toString());
					
					counter++;
				}
				
				// From each node in the list look for the next node in the tree and
				// set for them the fail relation
				while (!queue.isEmpty()) {
					
					Node node = graphDb.getNodeById(queue.pop());
					
					JSONObject ob = new JSONObject(String.valueOf(node.getProperty(ACProperties.RELATIONSHIP)));
					iter = node.getRelationships(Direction.OUTGOING).iterator();
					
					// Look up for all nodes which has a relation to the node and
					// the relation is no fail relation
					while (iter.hasNext()) {
						
						Relationship rel = iter.next();
						if(!rel.getType().name().equals(ACProperties.EdgeTypes.FAIL.name())){
							
							Node endNode = rel.getEndNode();
							queue.add(endNode.getId());
							
							// Get fail node of the current node
							Node state = graphDb.getNodeById(ACUtil.toLong(ob.get(ACProperties.EdgeTypes.FAIL.name())));
							
							// As long the fail node has no relation with the
							// name of the current relation
							// get the next fail node of the current fail node
							// If the next fail node is the root, the while loop
							// will stop
							
							Node toNode = null;
							
							while ((toNode = ACUtil.getNextNodeCreate(dict, graphDb, 
									root, state, (String) rel.getProperty(ACProperties.LETTER))) == null) {
								
								JSONObject obBetween = new JSONObject(
										String.valueOf(state.getProperty(ACProperties.RELATIONSHIP)));
								
								state = root;
								if(obBetween.has(ACProperties.EdgeTypes.FAIL.name())){
									state = graphDb.getNodeById(obBetween.getLong(ACProperties.EdgeTypes.FAIL.name()));
								}
							}
							
							// Set the fail relation
							endNode.createRelationshipTo(toNode, ACProperties.EdgeTypes.FAIL);
							
							// Update the EndNode only if the FAIL Relation goes not to the root
							// See NOTE at the beginning of the method
							endNode.setProperty(ACProperties.NUMBER_NEXT, 
									ACUtil.toLong(endNode.getProperty(ACProperties.NUMBER_NEXT))+1);
							
							JSONObject jsonRelMap = new JSONObject(
									String.valueOf(endNode.getProperty(ACProperties.RELATIONSHIP)));
							jsonRelMap.putOnce(ACProperties.getFailName(), toNode.getId());
							endNode.setProperty(ACProperties.RELATIONSHIP, jsonRelMap.toString());
							
							// If the fail node has an output then union both in
							// the endNode
							ACUtil.unionOutput(endNode, toNode);
							counter++;

							if(counter%10000==0){
								System.out.println(counter);
							}
						}
						
						
					}
				}
			}else{
				/**************** DEFAULT VERSION *********************************/
				
				Deque<Long> queue = new ArrayDeque<Long>();
				Iterator<Relationship> iter = root.getRelationships(Direction.OUTGOING).iterator();
				
				while (iter.hasNext()) {
					// Create Relationship
					Relationship rel = iter.next();
					Node endNode = rel.getEndNode();
					endNode.createRelationshipTo(root, ACProperties.EdgeTypes.FAIL);
					queue.add(endNode.getId());
					
					// Update Number Next
					endNode.setProperty(ACProperties.NUMBER_NEXT, 
							ACUtil.toLong(endNode.getProperty(ACProperties.NUMBER_NEXT))+1);
					
					counter++;
				}
				
				// From each node in the list look for the next node in the tree and
				// set for them the fail relation
				while (!queue.isEmpty()) {
					
					Node node = graphDb.getNodeById(queue.pop());
					iter = node.getRelationships(Direction.OUTGOING).iterator();
					
					// Look up for all nodes which has a relation to the node and
					// the relation is no fail relation
					while (iter.hasNext()) {
						Relationship typ = iter.next();
						Node endNode = typ.getEndNode();
						
						if(!typ.getType().name().equals(ACProperties.EdgeTypes.FAIL.name())){
							queue.add(endNode.getId());
							
							// Get fail node of the current node
							Node state = node.getSingleRelationship(ACProperties.EdgeTypes.FAIL, 
									Direction.OUTGOING).getEndNode();
							
							// As long the fail node has no relation with the
							// name of the current relation
							// get the next fail node of the current fail node
							// If the next fail node is the root, the while loop
							// will stop
							
							Node toNode = null;
							while ((toNode = ACUtil.getNextNodeCreate(dict, graphDb, 
									root, state, (String) typ.getProperty(ACProperties.LETTER))) == null) {
								
								state = state.getSingleRelationship(ACProperties.EdgeTypes.FAIL, 
										Direction.OUTGOING).getEndNode();
							}
							
							// Set the fail relation
							endNode.createRelationshipTo(toNode, ACProperties.EdgeTypes.FAIL);
							
							// Update the EndNode only if the FAIL Relation goes not to the root
							// See NOTE at the beginning of the method
							endNode.setProperty(ACProperties.NUMBER_NEXT, 
									ACUtil.toLong(endNode.getProperty(ACProperties.NUMBER_NEXT))+1);
							
							// If the fail node has an output then union both in
							// the endNode
							ACUtil.unionOutput(endNode, toNode);
							counter++;
							
							if(counter%10000==0){
								System.out.println(counter);
							}
						}
					}
				}
			}
			
			// Set the DictTree to be prepared for search
			root.setProperty(ACProperties.PREPARED, true);
			
			tx.success();
			return true;
		}catch (Exception e) {
			e.printStackTrace();
			System.out.println(e);
			tx.failure();
			return false;
		}finally {
			tx.close();
		}
		
	}
	
	/**
	 * Unprepare the DictTree, so that it can't be searched anymore.
	 * 
	 * @param graphDb
	 *            - GraphDatabase
	 * @param name
	 *            - Name of the dictionary
	 * @return <code>true</code> if the DictTree is unprepared, <code>false</code> if the DictTree does not exist
	 * @throws IOException
	 * @throws JSONException
	 */
	@SuppressWarnings("unchecked")
	@Name("unprepare_dicttree")
	@Description("Deletes all Fail-Relation in the DictTree.")
	@PluginTarget(GraphDatabaseService.class)
	public static boolean unprepareDictTree(@Source GraphDatabaseService graphDb, @Description("Name of the dictionary.") 
	@Parameter(name = "name") String name) throws IOException,
			JSONException {

		// Start Transaction
		Transaction tx = graphDb.beginTx();
		
		try{
			// Find root for DictTree
			Node root = ACUtil.getRootNode(graphDb, name);
			
			// If root does not exist return false
			if (!ACUtil.isDictTreePrepared(root)) {
				return false;
			}
			
			// Set root as a starting point for the process
			// List has to be "First In - First Out" so that the algorith look up
			// level by level (depth by depth) the tree!
			Deque<Node> queue = new ArrayDeque<Node>();
			queue.add(root);
			
			long groupCounter = 0;
			final long maxGroupCount = 20000;
			
			// From each node in the list look for the next node in the tree and
			// set for them the fail relation
			while (!queue.isEmpty()) {
				Node node = queue.pop();
				Iterator<Relationship> iterRe = node.getRelationships(Direction.OUTGOING).iterator();
				// Look up for all nodes which has a relation to the node and
				// the relation is no fail relation
				while (iterRe.hasNext()) {
					
					Relationship rel = iterRe.next();
					
					if (rel.getType().name().equals(ACProperties.getFailName())) {
						// Deletes Fail-Relation
						rel.delete();
						ACUtil.decreaseNumberNext(node);
					} else {
						// If no Fail-Relation add the node to a queue. Otherwise
						// it's
						// a endless loop here
						queue.add(rel.getEndNode());
					}
				}
				
				// Deletes all entries in the node which were created during the
				// union function
				if (ACUtil.toLong(node.getProperty(ACProperties.NUMBER_OUTPUT)) > 1) {
					String original = String.valueOf(node
							.getProperty(ACProperties.ORIGINAL));
					
					JSONObject ob = new JSONObject(
							String.valueOf(node.getProperty(ACProperties.OUTPUT)));
					Iterator<String> iterOb = ob.keys();
					
					while (iterOb.hasNext()) {
						String key = iterOb.next();
						if (key.startsWith(ACProperties.PROPERTY) || key.equals(original)) {
							// Geht ins leere
						} else {
							iterOb.remove();
							node.setProperty(ACProperties.NUMBER_OUTPUT, 
									((Integer) node.getProperty(ACProperties.NUMBER_OUTPUT) - 1));
						}
					}
					
					node.setProperty(ACProperties.OUTPUT, 
							ob.toString());
				}
				
				groupCounter++;
				if (groupCounter > maxGroupCount) {
					tx.success();
					tx.close();
					
					groupCounter = 0;
					
					tx = graphDb.beginTx();
				}
			}
			
			// Update root property
			root.setProperty(ACProperties.PREPARED, false);
			
			tx.success();
			
			return true;
			
		}finally{
			tx.close();
		}
	}
	
	/////////////////////////////////////////////////
	///////////// HELP FUNCTIONS
	
	/**
	 * Get Informations about the node before
	 * @param node
	 * @return Map with the Relation_typ and Id (Safed as State) before
	 */
	private static HashMap<String, Object> getNodeBefore(final Node node) {
		Iterator<Relationship> iterRel = node.getRelationships(Direction.INCOMING).iterator();
		while (iterRel.hasNext()) {
			Relationship rel = iterRel.next();
			if (!rel.getType().equals(ACProperties.EdgeTypes.FAIL)) {
				HashMap<String, Object> map = new HashMap<String, Object>();
				map.put(ACProperties.RELATION_TYPE, rel.getProperty(ACProperties.LETTER));
				map.put(ACProperties.STATE, rel.getStartNode());
				return map;
			}
		}
		return null;
	}
	
//	@Deprecated
//	private static void updateNode(ACDictionary dict, Node node, Node newNode, String letter) throws JSONException{
//
//		if(dict.isLocal()){
//			JSONObject jsonRelMap = new JSONObject(
//					String.valueOf(node.getProperty(ACProperties.RELATIONSHIP)));
//			jsonRelMap.putOpt(letter, newNode.getId());
//			node.setProperty(ACProperties.RELATIONSHIP, jsonRelMap.toString());
//		}
//				
//		// Update Global Map
//		if(dict.isGlobal()){
//			dict.addNode(newNode.getId());
//			dict.addRel(node.getId(), letter, newNode.getId());
//		}
//	}
//	
//	@Deprecated
//	private static void increaseNumberNext(Node node){
//		node.setProperty(ACProperties.NUMBER_NEXT, 
//				ACUtil.toLong(node.getProperty(ACProperties.NUMBER_NEXT))+1);
//	}
//	
//	@Deprecated
//	private static void decreaseNumberNext(Node node){
//		node.setProperty(ACProperties.NUMBER_NEXT, 
//				ACUtil.toLong(node.getProperty(ACProperties.NUMBER_NEXT))-1);
//	}
	
}
