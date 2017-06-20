package de.julielab.neo4j.plugins.ahocorasick;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.shell.util.json.JSONException;
import org.neo4j.shell.util.json.JSONObject;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchRelationship;

import de.julielab.neo4j.plugins.ahocorasick.property.ACDataBase;
import de.julielab.neo4j.plugins.ahocorasick.property.ACDictionary;
import de.julielab.neo4j.plugins.ahocorasick.property.ACEntry;
import de.julielab.neo4j.plugins.ahocorasick.property.ACGlobalMap;



public class ACFactoryBatch {

	/**
	 * Creates a new root of a DictTree, where entries could be add.
	 * 
	 * @param batchInserter
	 *            - BatchInserter of the Database
	 * @param name
	 *            - Name of the Dictionary
	 * @param tokenizer
	 * 			  - Is the DictTree a version with tokens instead of letters
	 * @return <code>true</code> if a new root was created, <code>false</code> if a root already exist
	 */
	public static boolean createDictTree(BatchInserter batchInserter, ACDictionary dict) {
		
		// Check if a root already exists
		long rootID = ACUtil.getRootID(batchInserter, dict.name());
		
		if (rootID==-1) {

			// Creates the HashMap for all properties of the node
			Map<String, Object> propertiesRoot = new HashMap<>();
			// Name of the Dictionary
			propertiesRoot.put(ACProperties.DICTIONARY_NAME, dict.name());
			// Number of Nodes in the Tree
			propertiesRoot.put(ACProperties.NODES_IN_TREE, 1);
			// Number of Entries in the Tree
			propertiesRoot.put(ACProperties.NUMBER_OF_ENTRIES, 0);
			// Is the Tree prepared for search?
			propertiesRoot.put(ACProperties.PREPARED, false);
			// The State if the node
			propertiesRoot.put(ACProperties.STATE, 0);
			// The Deepth of the node
			propertiesRoot.put(ACProperties.DEPTH, 0);
			// Number of outputs in the node
			propertiesRoot.put(ACProperties.NUMBER_OUTPUT, 0);
			// Outgoing Relationships of the node
			JSONObject jsonRelMap = new JSONObject();
			propertiesRoot.put(ACProperties.RELATIONSHIP, jsonRelMap.toString());
			// Number of Outgoing Relationships
			propertiesRoot.put(ACProperties.NUMBER_NEXT, 0);
			// Mode to create
			propertiesRoot.put(ACProperties.MODECREATE, dict.getCreateMode());
			// Mode to search
			propertiesRoot.put(ACProperties.MODESEARCH, dict.isLocalSearch());
			
			// Create the root
			long id = batchInserter.createNode(propertiesRoot, ACProperties.LabelTypes.DICTIONARY);
			Map<String, Object> propertie = MapUtil.map(ACProperties.DICTIONARY_NAME, dict.name());
			
			// Add the root to the index for easy find
			BatchInserterIndexProvider indexProvider = new LuceneBatchInserterIndexProvider( batchInserter );
			BatchInserterIndex dictIndex = indexProvider.nodeIndex( ACProperties.INDEX_DIC
					, MapUtil.stringMap( "type", "exact" ) );
			dictIndex.add(id, propertie);
			
			// Flush (for searching) and shutdown the index 
			dictIndex.flush();
			indexProvider.shutdown();
			
			return true;
		}
			
		// Root already exists in the GraphDatabase
		return false;
	}
	
	
	/**
	 * Adds a list of entries to a DictTree, which is not prepared for searching
	 * 
	 * @param batchInserter
	 *            - BatchInserter
	 * @param name
	 *            - name of the DictTree
	 * @param dictEntries
	 *            - The dictionary entries as a JSON map of the form 
	 *            [{'entry':'entry1','attributes':{'attribute1':'attValue1','attribute2':'attValue2'}}] The
	 *            Entry is mapped with the constant ENTRY and the Attributes is a Map, which has the key ATRRIBUTES
	 * @return <code>true</code> if all entries could be added, <code>false</code> if the tree is already prepared for searching
	 * @throws JSONException
	 * @throws IOException
	 */
	public static boolean addListToDictTree(ACDataBase dataBase, ACDictionary dict, List<ACEntry> listEntries)
			throws JSONException, IOException {

		// Find Dictionary
		BatchInserter batchInserter = dataBase.startBatchInserter();
		long id = ACUtil.getRootID(batchInserter, dict.name());
		
		// Dictionary has to exists
		if(id!=-1){
			Map<String, Object> rootProperties = batchInserter.getNodeProperties(id);
			boolean prepared = (boolean) rootProperties.get(ACProperties.PREPARED);

			ACGlobalMap globalMap = null;
			if(dict.getCreateMode()==ACDictionary.GLOBAL_MODE_CREATE){
				globalMap = dataBase.getMap(dict.name());
			}
			
			// Dictionary is not allowed to be prepared
			if(!prepared){
				
				for (int i = 0; i < listEntries.size(); i++) {
					// Get the Map for the entry
					ACEntry entry = listEntries.get(i);
					
					// Add the entry with his attributes to the DictTree
					rootProperties = addEntryToTree(batchInserter, dict, 
							rootProperties, globalMap, id, entry);
				}
				
				// Update the root Properties which has changed during adding new entries
				batchInserter.setNodeProperty(id, ACProperties.NODES_IN_TREE, 
						rootProperties.get(ACProperties.NODES_IN_TREE));
				batchInserter.setNodeProperty(id, ACProperties.NUMBER_OF_ENTRIES, 
						rootProperties.get(ACProperties.NUMBER_OF_ENTRIES));
				
				return true;
			}
		}
		return false;
	}
	

	
	/**
	 * Adds a entry to the DictTree
	 * 
	 * @param batchInserter
	 *            - BatchInserter
	 * @param root
	 *            - Root of the DictTree
	 * @param word
	 *            - Word which should be add to the DictTree
	 * @throws JSONException
	 */
	private static Map<String, Object> addEntryToTree(BatchInserter batchInserter, ACDictionary dict, 
			Map<String, Object> rootProperties, ACGlobalMap globalMap, long rootId, 
			ACEntry entry) throws JSONException {

		String[] toAdd = entry.getTokens();
		
		// Get the state number for the next node, which has to be created
		long newState =  ACUtil.toLong(rootProperties
				.get(ACProperties.NODES_IN_TREE));

		// Set the root as a starting point for searching the word in the tree
		int j = 0;
		long nodeId = rootId; // rootId doesn't has to be Zero!!!
		long nodeNextId = ACUtil.getNextNodeCreate(dict, batchInserter, -1,
				globalMap, nodeId, toAdd[j]);
		
		// Lookup for the word in tree until a relation does not exist for a
		// letter
		while (nodeNextId != -1) {
			nodeId = nodeNextId;
			j = j + 1;

			// Check if the whole word already is in the tree
			if (j == toAdd.length)
				break;

			// Try to get the next node for the next letter in the word
			nodeNextId = ACUtil.getNextNodeCreate(dict, batchInserter, -1, 
					globalMap, nodeId, toAdd[j]);
		}

		// Create new nodes and relations for letter which do not appear
		// currently in the DictTree at the correct place
		for (int p = j; p < toAdd.length; p++) {

			Map<String, Object> properties = new HashMap<>();
			JSONObject jsonRelMap = new JSONObject();
			
			// Set node properties
			properties.put(ACProperties.STATE, newState);
			properties.put(ACProperties.NUMBER_OUTPUT, 0);
			properties.put(ACProperties.NUMBER_NEXT, 0);
			properties.put(ACProperties.DEPTH, p);
			properties.put(ACProperties.RELATIONSHIP, jsonRelMap.toString());
			
			long newNodeId = batchInserter.createNode(properties);

			// Create Relation between current and new node
			// Relationship has no Property
			// CREATES ALSO A DUMMY RELATION FROM THE ENDNODE TO THE ENDNODE
			// AS LONG NO OTHER RELATION IS SET FROM THE ENDNODE TO ANOTHER NODE
			
			Map<String, Object> mapRel = new HashMap<>();
			mapRel.put(ACProperties.LETTER, toAdd[p]);
			
			batchInserter.createRelationship(nodeId, newNodeId, ACProperties.EdgeTypes.NEXT, mapRel);
			
			// Update the starting node of the Relationship with the
			// new Relationship
			Map<String, Object> map = batchInserter.getNodeProperties(nodeId);
			map.put(ACProperties.NUMBER_NEXT, 
					ACUtil.toLong(map.get(ACProperties.NUMBER_NEXT))+1);
			
			if(dict.isLocalCreate()){
				jsonRelMap = new JSONObject(
						String.valueOf(map.get(ACProperties.RELATIONSHIP)));
				jsonRelMap.putOpt(toAdd[p], newNodeId);
				map.put(ACProperties.RELATIONSHIP, jsonRelMap.toString());
			}
			batchInserter.setNodeProperties(nodeId, map);
			
			// Update Global Map
			if(dict.isGlobalCreate()){
				globalMap.addNode(newNodeId);
				globalMap.addRel(nodeId, toAdd[p], newNodeId);
			}
			
			// Update Variables
			nodeId = newNodeId;
			newState = newState + 1;
		}
		// Add word and attribut to the final node
		addOutput(batchInserter, nodeId, entry);
		
		// Update Root Properties
		rootProperties.put(ACProperties.NODES_IN_TREE, newState);
		rootProperties.put(ACProperties.NUMBER_OF_ENTRIES, 
				ACUtil.toLong(rootProperties.get(ACProperties.NUMBER_OF_ENTRIES)) + 1);
		
		return rootProperties;
	}
	
	/**
	 * Prepares the DictTree for searching. It sets all fail relations like in the algorithm 3 of the paper from 1975. See <a
	 * href="http://dl.acm.org/citation.cfm?doid=360825.360855">article</a>
	 * 
	 * @param batchInserter
	 *            - BatchInserter
	 * @param name
	 *            - Name of the dictionary
	 * @return <code>true</code> if the tree is prepared, <code>false</code> if tree does not exist
	 * @throws IOException
	 * @throws JSONException
	 */
	public static boolean prepareDictTreeForSearch(ACDataBase dataBase, ACDictionary dict)
			throws IOException, JSONException {
		
		// NOTE: A NODE WITH A FAIL RELATION TO THE ROOT HAS NO FAIL RELATION IN THE HASHMAP!!
		// THE REASION IS TO SAVE PERFORMANCE TIME. SO IF ROOT WITH THE ID 4 HAS A FAIL RELATION
		// TO THE ROOT 1 THEN THE HASHMAP "RELATIONSHIPS" HAS NO ENTRY WITH THE NAME "FAIL". IF THE
		// NODE 5 HAS A FAIL RELATION TO 2 THEN IT HAS THE ENTRY {"FAIL": 2}!!

		BatchInserter batchInserter = dataBase.startBatchInserter();
		
		// Find root for DictTree
		long id = ACUtil.getRootID(batchInserter, dict.name());
		int counter = 0;
		
		// Check whether DictTree exist or not
		if(id!=-1){
			Map<String, Object> rootProperties = batchInserter.getNodeProperties(id);
			Boolean prepared = (Boolean) rootProperties.get(ACProperties.PREPARED);
			
			// Is DictTree prepared
			if (prepared) {
				return false;
			}
			
			System.out.println(rootProperties.get(ACProperties.NODES_IN_TREE));
			
			// Set root as a starting point for the process
			// List has to be "First In - First Out" so that the algorithm look
			// up level by level (depth by depth) the tree!
			
			ACGlobalMap globalMap = null;
			
			if(dict.isGlobalCreate()){
				/**************** GLOBAL VERSION *********************************/
				
				globalMap = dataBase.getMap(dict.name());
				
				// Update Root
				if(dict.isLocalSearch()){
					Map<String, Object> map = batchInserter.getNodeProperties(id);
					JSONObject jsonRelMap = new JSONObject(globalMap.getRelMap().get(id));
					map.put(ACProperties.RELATIONSHIP, jsonRelMap.toString());
					batchInserter.setNodeProperties(id, map);
				}
				
				Deque<Long> queue = new ArrayDeque<Long>();
				Iterator<String> iter = globalMap.iteratorNextNodes(id);
				
				while (iter.hasNext()) {
					// Create Relationship
					long endNode = ACUtil.toLong(globalMap.getNodeID(id, iter.next()));
					batchInserter.createRelationship(endNode, id, ACProperties.EdgeTypes.FAIL, null);
					queue.add(endNode);
					
					// Update Global Map
					globalMap.addRel(endNode, ACProperties.getFailName(), id);
					
					// Update Number Next
					Map<String, Object> map = batchInserter.getNodeProperties(endNode);
					map.put(ACProperties.NUMBER_NEXT, 
							ACUtil.toLong(map.get(ACProperties.NUMBER_NEXT))+1);
					
					// Update Local Information if Local Search
					if(dict.isLocalSearch()){
						JSONObject jsonRelMap = new JSONObject(globalMap.getRelMap().get(endNode));
						map.put(ACProperties.RELATIONSHIP, jsonRelMap.toString());
					}
					
					batchInserter.setNodeProperties(endNode, map);
					
					counter++;
				}
				
				// From each node in the list look for the next node in the tree and
				// set for them the fail relation
				while (!queue.isEmpty()) {
					
					long node = queue.pop();
					iter = globalMap.iteratorNextNodes(node);
					
					// Look up for all nodes which has a relation to the node and
					// the relation is no fail relation
					while (iter.hasNext()) {
						String typ = iter.next();
						if(!typ.equals(ACProperties.EdgeTypes.FAIL.name())){
							long endNode = ACUtil.toLong(globalMap.getNodeID(node, typ));
							queue.add(endNode);
							
							// Get fail node of the current node
							long state = globalMap.getNodeID(node, ACProperties.getFailName());
							
							// As long the fail node has no relation with the
							// name of the current relation
							// get the next fail node of the current fail node
							// If the next fail node is the root, the while loop
							// will stop
							
							long toNode = -1;
							while ((toNode = ACUtil.getNextNodeCreate(dict, batchInserter, id, globalMap, state, typ)) == -1) {
								
								state = globalMap.getNodeID(state, ACProperties.getFailName());
							}
							
							// Set the fail relation
							batchInserter.createRelationship(endNode, toNode, ACProperties.EdgeTypes.FAIL, null);
							globalMap.addRel(endNode, ACProperties.getFailName(), toNode);
							
							// Update the EndNode only if the FAIL Relation goes not to the root
							// See NOTE at the beginning of the method
							Map<String, Object> map = batchInserter.getNodeProperties(endNode);
							map.put(ACProperties.NUMBER_NEXT, 
									ACUtil.toLong(map.get(ACProperties.NUMBER_NEXT))+1);
							// Update Local Information if Local Search
							if(dict.isLocalSearch()){
								JSONObject jsonRelMap = new JSONObject(globalMap.getRelMap().get(endNode));
								map.put(ACProperties.RELATIONSHIP, jsonRelMap.toString());
							}
							batchInserter.setNodeProperties(endNode, map);

							// If the fail node has an output then union both in
							// the endNode
							unionOutput(batchInserter, endNode, toNode);
							counter++;
							
							if(counter%100==0){
								System.out.println(counter);
							}
						}
					}
				}
				
				dataBase.deleteMap(dict.name());
				
			}else if(dict.isLocalCreate()){
				/**************** LOCAL VERSION *********************************/
				Deque<Long> queue = new ArrayDeque<Long>();
				Iterator<BatchRelationship> iter = batchInserter.getRelationships(id).iterator();
				
				while (iter.hasNext()) {
					// Create Relationship
					BatchRelationship rel = iter.next();
					if(rel.getStartNode() == id && rel.getStartNode() != rel.getEndNode()){
						long endNode = rel.getEndNode();
						
						batchInserter.createRelationship(endNode, id, ACProperties.EdgeTypes.FAIL, null);
						queue.add(endNode);
						
						// Update the EndNode only if the FAIL Relation goes not to the root
						// See NOTE at the beginning of the method
						Map<String, Object> map = batchInserter.getNodeProperties(endNode);
						map.put(ACProperties.NUMBER_NEXT, 
								ACUtil.toLong(map.get(ACProperties.NUMBER_NEXT))+1);
						
						JSONObject jsonRelMap = new JSONObject(
								String.valueOf(map.get(ACProperties.RELATIONSHIP)));
						jsonRelMap.putOnce(ACProperties.getFailName(), id);
						map.put(ACProperties.RELATIONSHIP, jsonRelMap.toString());
						
						batchInserter.setNodeProperties(endNode, map);
						
						counter++;
					}
				}
				
				// From each node in the list look for the next node in the tree and
				// set for them the fail relation
				while (!queue.isEmpty()) {
					
					long node = queue.pop();
					JSONObject ob = new JSONObject(String.valueOf(batchInserter
							.getNodeProperties(node).get(ACProperties.RELATIONSHIP)));
					iter = batchInserter.getRelationships(node).iterator();
					
					// Look up for all nodes which has a relation to the node and
					// the relation is no fail relation
					while (iter.hasNext()) {
						
						BatchRelationship rel = iter.next();
						Map<String, Object> mapRel = batchInserter.getRelationshipProperties(rel.getId());
						String nameRel = (String) mapRel.get(ACProperties.LETTER);
						
						if(!rel.getType().name().equals(ACProperties.EdgeTypes.FAIL.name())
								&& rel.getStartNode() == node
								&& rel.getStartNode() != rel.getEndNode()){
							
							long endNode = rel.getEndNode();
							queue.add(endNode);
							
							// Get fail node of the current node
							long state = ACUtil.toLong(ob.get(ACProperties.EdgeTypes.FAIL.name()));
							
							// As long the fail node has no relation with the
							// name of the current relation
							// get the next fail node of the current fail node
							// If the next fail node is the root, the while loop
							// will stop
							
							long toNode = -1;
							
							while ((toNode = ACUtil.getNextNodeCreate(dict, batchInserter, 
									id, globalMap, state, nameRel)) == -1) {
								
								JSONObject obBetween = new JSONObject(
										String.valueOf(batchInserter.getNodeProperties(state)
												.get(ACProperties.RELATIONSHIP)));
								
								state = id;
								if(obBetween.has(ACProperties.EdgeTypes.FAIL.name())){
									state = obBetween.getLong(ACProperties.EdgeTypes.FAIL.name());
								}
							}
							
							// Set the fail relation
							batchInserter.createRelationship(endNode, toNode, ACProperties.EdgeTypes.FAIL, null);
							
							// Update the EndNode only if the FAIL Relation goes not to the root
							// See NOTE at the beginning of the method
							Map<String, Object> map = batchInserter.getNodeProperties(endNode);
							map.put(ACProperties.NUMBER_NEXT, 
									ACUtil.toLong(map.get(ACProperties.NUMBER_NEXT))+1);
							
							JSONObject jsonRelMap = new JSONObject(
									String.valueOf(map.get(ACProperties.RELATIONSHIP)));
							jsonRelMap.putOnce(ACProperties.getFailName(), toNode);
							map.put(ACProperties.RELATIONSHIP, jsonRelMap.toString());
							
							batchInserter.setNodeProperties(endNode, map);
							
							// If the fail node has an output then union both in
							// the endNode
							unionOutput(batchInserter, endNode, toNode);
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
				Iterator<BatchRelationship> iter = batchInserter.getRelationships(id).iterator();
				
				while (iter.hasNext()) {
					// Create Relationship
					BatchRelationship rel = iter.next();
					if(rel.getStartNode() == id && rel.getStartNode() != rel.getEndNode()){
						long endNode = rel.getEndNode();
						batchInserter.createRelationship(endNode, id, ACProperties.EdgeTypes.FAIL, null);
						queue.add(endNode);
						
						// Update Number Next
						Map<String, Object> map = batchInserter.getNodeProperties(endNode);
						map.put(ACProperties.NUMBER_NEXT, 
								ACUtil.toLong(map.get(ACProperties.NUMBER_NEXT))+1);
						batchInserter.setNodeProperties(endNode, map);
						
						counter++;
					}
				}
				
				// From each node in the list look for the next node in the tree and
				// set for them the fail relation
				while (!queue.isEmpty()) {
					
					long node = queue.pop();
					iter = batchInserter.getRelationships(node).iterator();
					
					// Look up for all nodes which has a relation to the node and
					// the relation is no fail relation
					while (iter.hasNext()) {
						BatchRelationship typ = iter.next();
						long endNode = typ.getEndNode();
						
						Map<String, Object> mapRel = batchInserter.getRelationshipProperties(typ.getId());
						String nameRel = (String) mapRel.get(ACProperties.LETTER);
						
						if(!typ.getType().name().equals(ACProperties.EdgeTypes.FAIL.name())
								&& typ.getStartNode() == node
								&& typ.getStartNode() != typ.getEndNode()){
							queue.add(endNode);
							
							// Get fail node of the current node
							long state = 0;
							
							Iterator<BatchRelationship> iterFai
								=batchInserter.getRelationships(node).iterator();
						
							while(iterFai.hasNext()){
								BatchRelationship batRel = iterFai.next();
								if(batRel.getType().name().equals(ACProperties.getFailName())){
									if(batRel.getStartNode() == node){
										state = batRel.getEndNode();
										break;
									}
								}
							}
							
							// As long the fail node has no relation with the
							// name of the current relation
							// get the next fail node of the current fail node
							// If the next fail node is the root, the while loop
							// will stop
							
							long toNode = -1;
							while ((toNode = ACUtil.getNextNodeCreate(dict, batchInserter, 
									id, globalMap, state, nameRel)) == -1) {
								
								Iterator<BatchRelationship> iterFai2
									=batchInserter.getRelationships(state).iterator();
								
								while(iterFai2.hasNext()){
									BatchRelationship batRel = iterFai2.next();
									if(batRel.getType().name().equals(ACProperties.getFailName())){
										if(batRel.getStartNode()==state){
											state = batRel.getEndNode();
											break;
										}
									}
								}
								
							}
							
							// Set the fail relation
							batchInserter.createRelationship(endNode, toNode, ACProperties.EdgeTypes.FAIL, null);
							
							// Update the EndNode only if the FAIL Relation goes not to the root
							// See NOTE at the beginning of the method
							Map<String, Object> map = batchInserter.getNodeProperties(endNode);
							map.put(ACProperties.NUMBER_NEXT, 
									ACUtil.toLong(map.get(ACProperties.NUMBER_NEXT))+1);
							batchInserter.setNodeProperties(endNode, map);
							
							// If the fail node has an output then union both in
							// the endNode
							unionOutput(batchInserter, endNode, toNode);
							counter++;
							
							if(counter%10000==0){
								System.out.println(counter);
							}
						}
					}
				}
			}
			
			// Set the DictTree to be prepared for search
			batchInserter.setNodeProperty(id, ACProperties.PREPARED, true);
			return true;
		}
		
		return false;
	}
	
	
	/**
	 * Adds an Output to the given node
	 * @param batchInserter
	 * @param nodeId
	 * @param output
	 * @param attributes
	 * @throws JSONException
	 */
	private static void addOutput(BatchInserter batchInserter, long nodeId, ACEntry entry) throws JSONException {

		JSONObject jsonOB = new JSONObject();
		jsonOB.put(entry.entryString(), entry.getAllAttributes());

		Map<String, Object> prop = batchInserter.getNodeProperties(nodeId);
		// Attribute mit übergeben
		prop.put(ACProperties.ORIGINAL, entry.entryString());
		prop.put(ACProperties.OUTPUT, jsonOB.toString());
		prop.put(ACProperties.NUMBER_OUTPUT, 1);
		
		batchInserter.setNodeProperties(nodeId, prop);
	}
	
	/**
	 * Union the output of two nodes to the first one
	 * @param node1
	 *            - Node which holds the union of both node after the process
	 * @param node2
	 *            - Node which holds the other output
	 * @throws JSONException
	 */
	@SuppressWarnings("unchecked")
	private static void unionOutput(BatchInserter batchInserter, long node1, long node2) throws JSONException {

		// Node 2 has no output so all stays the same
		Map<String, Object> prop2 = batchInserter.getNodeProperties(node2);
		if (ACUtil.toLong(prop2.get(ACProperties.NUMBER_OUTPUT)) == 0) {
			return;
		}

		// Node 1 has no output so all entries of 2 are copied to 1
		Map<String, Object> prop1 = batchInserter.getNodeProperties(node1);
		if (ACUtil.toLong(prop1.get(ACProperties.NUMBER_OUTPUT)) == 0) {
			prop1.put(ACProperties.OUTPUT, 
					prop2.get(ACProperties.OUTPUT));
			prop1.put(ACProperties.NUMBER_OUTPUT
					, ((Integer) prop2.get(ACProperties.NUMBER_OUTPUT)));
			batchInserter.setNodeProperties(node1, prop1);
			return;
		}

		JSONObject jsonObNode1 = new JSONObject(String.valueOf(prop1
				.get(ACProperties.OUTPUT)));
		JSONObject jsonObNode2 = new JSONObject(String.valueOf(prop2
				.get(ACProperties.OUTPUT)));

		// All outputs of 2 will be add to 1
		Iterator<String> keys = jsonObNode2.keys();
		while (keys.hasNext()) {

			String key = keys.next();

			if (!key.startsWith(ACProperties.PROPERTY) && !jsonObNode1.has(key)) {
				jsonObNode1.putOnce(key, jsonObNode2.get(key));
				prop1.put(ACProperties.NUMBER_OUTPUT
						, ((Integer) prop1.get(ACProperties.NUMBER_OUTPUT)) + 1);
			}
		}

		prop1.put(ACProperties.OUTPUT, jsonObNode1.toString());
		batchInserter.setNodeProperties(node1, prop1);
	}
	
}
