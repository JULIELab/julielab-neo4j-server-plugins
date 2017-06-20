package de.julielab.neo4j.plugins.ahocorasick;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.shell.util.json.JSONException;
import org.neo4j.shell.util.json.JSONObject;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchRelationship;

import de.julielab.neo4j.plugins.ahocorasick.property.ACDictionary;
import de.julielab.neo4j.plugins.ahocorasick.property.ACEntry;
import de.julielab.neo4j.plugins.ahocorasick.property.ACGlobalMap;

public class ACUtil {

	// ////////////////////////////////////////////
	// //////// ROOT FINDER

	/**
	 * Find a Dict-Tree Root in a given Database
	 * 
	 * @param batchInserter
	 * @param name
	 * @return
	 */
	static long getRootID(BatchInserter batchInserter, String name) {
		BatchInserterIndexProvider indexProvider = new LuceneBatchInserterIndexProvider(batchInserter);
		BatchInserterIndex dict = indexProvider.nodeIndex(ACProperties.INDEX_DIC, MapUtil.stringMap("type", "exact"));

		try {
			// Check if a root with the name as DICTIONARY_NAME property already
			// exists
			IndexHits<Long> hits = dict.get(ACProperties.DICTIONARY_NAME, name);

			if (hits.hasNext()) {
				return hits.getSingle();
			}

			return -1;
		} finally {
			indexProvider.shutdown();
		}
	}

	/**
	 * Find a Dict-Tree Root in a given Database
	 * 
	 * @param graphDb
	 * @param name
	 * @return
	 */
	static Node getRootNode(GraphDatabaseService graphDb, String name) {
		IndexManager manager = graphDb.index();
		Index<Node> index = manager.forNodes(ACProperties.INDEX_DIC);

		try {
			// Check if a root with the name as DICTIONARY_NAME property already
			// exists
			IndexHits<Node> hits = index.get(ACProperties.DICTIONARY_NAME, name);
			return hits.getSingle();

		} finally {

		}
	}

	// ////////////////////////////////////////////
	// //////// GET NEXT NODE

	/**
	 * 
	 * @param dict
	 *            - Dictionary
	 * @param graphDb
	 *            - GraphDataService
	 * @param root
	 *            - Root of Dictionary
	 * @param node
	 *            - Node
	 * @param letter
	 *            - Letter/Token to search
	 * @return
	 * @throws JSONException
	 */
	static Node getNextNodeSearch(ACDictionary dict, GraphDatabaseService graphDb, Node node, String letter)
			throws JSONException {

		if (dict.isLocalSearch()) {
			JSONObject jsonRelMap = new JSONObject(String.valueOf(node.getProperty(ACProperties.RELATIONSHIP)));

			if (jsonRelMap.has(letter)) {
				return graphDb.getNodeById(Long.valueOf(String.valueOf(jsonRelMap.get(letter))));
			}

			// Check if the number of rel is consitent
			long numberNext = ACUtil.toLong(node.getProperty(ACProperties.NUMBER_NEXT));

			if (jsonRelMap.length() != numberNext) {

				Iterator<Relationship> iter = node.getRelationships(Direction.OUTGOING).iterator();
				while (iter.hasNext()) {
					Relationship rel = iter.next();
					if (rel.getStartNode().getId() != rel.getEndNode().getId()) {
						jsonRelMap.putOpt((String) rel.getProperty(ACProperties.LETTER), rel.getEndNode());
					}
				}

				node.setProperty(ACProperties.NUMBER_NEXT, jsonRelMap.length());
				node.setProperty(ACProperties.RELATIONSHIP, jsonRelMap.toString());

				return getNextNodeSearch(dict, graphDb, node, letter);
			}
		} else {
			// JUST NORMAL LOOKING FOR THE NEXT NODE
			Iterator<Relationship> iter = node.getRelationships(Direction.OUTGOING).iterator();
			while (iter.hasNext()) {
				Relationship rel = iter.next();
				if (!rel.getType().name().equals(ACProperties.getFailName())) {
					if (rel.getProperty(ACProperties.LETTER).equals(letter)) {
						return rel.getEndNode();
					}
				}
			}
		}

		if (node.hasProperty(ACProperties.DICTIONARY_NAME)) {
			return node;
		}

		return null;
	}

	static Node getNextNodeCreate(ACDictionary dict, GraphDatabaseService graphDb, Node root, Node node, String letter)
			throws JSONException {

		if (dict.getCreateMode() == 0) {
			// JUST NORMAL LOOKING FOR THE NEXT NODE
			Iterator<Relationship> iter = node.getRelationships(Direction.OUTGOING).iterator();
			while (iter.hasNext()) {
				Relationship rel = iter.next();
				if (!rel.getType().name().equals(ACProperties.getFailName())) {
					if (rel.getProperty(ACProperties.LETTER).equals(letter)) {
						return rel.getEndNode();
					}
				}
			}
		} else {
			JSONObject jsonRelMap = new JSONObject(String.valueOf(node.getProperty(ACProperties.RELATIONSHIP)));

			if (jsonRelMap.has(letter)) {
				return graphDb.getNodeById(Long.valueOf(String.valueOf(jsonRelMap.get(letter))));
			}

			// Check if the number of rel is consitent
			long numberNext = ACUtil.toLong(node.getProperty(ACProperties.NUMBER_NEXT));

			if (jsonRelMap.length() != numberNext) {

				Iterator<Relationship> iter = node.getRelationships(Direction.OUTGOING).iterator();
				while (iter.hasNext()) {
					Relationship rel = iter.next();
					if (rel.getStartNode().getId() != rel.getEndNode().getId()) {
						jsonRelMap.putOpt((String) rel.getProperty(ACProperties.LETTER), rel.getEndNode());
					}
				}

				node.setProperty(ACProperties.NUMBER_NEXT, jsonRelMap.length());
				node.setProperty(ACProperties.RELATIONSHIP, jsonRelMap.toString());

				return getNextNodeCreate(dict, graphDb, root, node, letter);
			}
		}

		// Just return the root if the tree is prepared for search
		if (root != null && node.getId() == root.getId()) {
			return node;
		}

		return null;
	}

	/**
	 * 
	 * @param dataBase
	 * @param batchInserter
	 * @param dictName
	 * @param idNode
	 * @param letter
	 * @param global
	 * @param local
	 * @return
	 * @throws JSONException
	 */
	static long getNextNodeCreate(ACDictionary dict, BatchInserter batchInserter, long rootId, ACGlobalMap globalMap,
			long idNode, String letter) throws JSONException {

		// IT EXISTS A GLOBAL HASH MAP
		// Global Map can be updated on the fly
		if (globalMap != null) {

			long id = globalMap.getNodeID(idNode, letter);
			if (id > -1) {
				return id;
			}

			// Check if the number of rel is consitent
			long numberNext = ACUtil.toLong(batchInserter.getNodeProperties(idNode).get(ACProperties.NUMBER_NEXT));

			if (numberNext != globalMap.numberOfRel(idNode)) {
				id = -2;
			}

			// Update the global map on the fly
			if (id == -2) {
				globalMap.addNode(idNode);
				Iterator<BatchRelationship> iter = batchInserter.getRelationships(idNode).iterator();

				long counter = 0;

				while (iter.hasNext()) {
					BatchRelationship rel = iter.next();
					if (rel.getStartNode() == idNode) {
						Map<String, Object> relMap = batchInserter.getRelationshipProperties(rel.getId());
						if (globalMap.addRel(idNode, (String) relMap.get(ACProperties.LETTER), rel.getEndNode()))
							counter++;
					}
				}

				Map<String, Object> map = batchInserter.getNodeProperties(idNode);
				map.put(ACProperties.NUMBER_NEXT, counter + numberNext);
				batchInserter.setNodeProperties(idNode, map);

				return getNextNodeCreate(dict, batchInserter, rootId, globalMap, idNode, letter);
			}

			// ALL RELATIONS ARE IN THE PROPERTIE OF THE NODE
		} else if (dict.isLocalCreate()) {
			JSONObject jsonRelMap = new JSONObject(String.valueOf(batchInserter.getNodeProperties(idNode).get(
					ACProperties.RELATIONSHIP)));

			if (jsonRelMap.has(letter)) {
				return Long.valueOf(String.valueOf(jsonRelMap.get(letter)));
			}

			// Check if the number of rel is consitent
			long numberNext = ACUtil.toLong(batchInserter.getNodeProperties(idNode).get(ACProperties.NUMBER_NEXT));

			if (jsonRelMap.length() != numberNext) {

				Iterator<BatchRelationship> iter = batchInserter.getRelationships(idNode).iterator();
				while (iter.hasNext()) {
					BatchRelationship rel = iter.next();
					if (rel.getStartNode() == idNode && rel.getStartNode() != rel.getEndNode()) {
						Map<String, Object> relMap = batchInserter.getRelationshipProperties(rel.getId());
						jsonRelMap.putOpt((String) relMap.get(ACProperties.LETTER), rel.getEndNode());
					}
				}

				Map<String, Object> mapRel = batchInserter.getNodeProperties(idNode);
				mapRel.put(ACProperties.RELATIONSHIP, jsonRelMap.toString());
				mapRel.put(ACProperties.NUMBER_NEXT, jsonRelMap.length());
				batchInserter.setNodeProperties(idNode, mapRel);

				return getNextNodeCreate(dict, batchInserter, rootId, globalMap, idNode, letter);
			}

		} else {
			// JUST NORMAL LOOKING FOR THE NEXT NODE
			Iterator<BatchRelationship> iter = batchInserter.getRelationships(idNode).iterator();
			while (iter.hasNext()) {
				BatchRelationship rel = iter.next();
				if (rel.getType().name().equals(ACProperties.getFailName())) {
					// DUMMY RUN
				} else {
					Map<String, Object> relMap = batchInserter.getRelationshipProperties(rel.getId());
					if (relMap.get(ACProperties.LETTER).equals(letter) && rel.getEndNode() != idNode) {
						return rel.getEndNode();
					}
				}
			}
		}

		// Just return the root if the tree is prepared for search
		if (idNode == rootId) {
			return idNode;
		}

		return -1;
	}

	/**
	 * 
	 * @param dict
	 * @param graphDb
	 * @param root
	 * @param entry
	 * @return
	 * @throws JSONException
	 */
	static Node getExactNode(ACDictionary dict, GraphDatabaseService graphDb, Node root, String entry)
			throws JSONException {

		Node node = root;

		for (int i = 0; i < entry.length(); i++) {
			node = ACUtil.getNextNodeCreate(dict, graphDb, null, node, entry.substring(i, i + 1));
			if (node == null) {
				return null;
			}
		}

		return node;
	}

	// ////////////////////////////////////////////
	// //////// UNION OUTPUT

	/**
	 * Fügt zu einen Knoten die Daten hinzu, die Ausgegeben werden sollen, wenn dieser Knoten erreicht wird
	 * 
	 * @param node
	 *            - Knoten
	 * @param output
	 *            - Ausgabewort
	 * @param attributes
	 *            - Attribute des Ausgabewortes
	 * @throws JSONException
	 */
	static void addOutput(Node node, ACEntry entry) throws JSONException {

		JSONObject jsonOB = new JSONObject();
		jsonOB.put(entry.entryString(), entry.getAllAttributes());

		// Attribute mit übergeben
		node.setProperty(ACProperties.ORIGINAL, entry.entryString());
		node.setProperty(ACProperties.OUTPUT, jsonOB.toString());
		node.setProperty(ACProperties.NUMBER_OUTPUT, 1);
	}

	/**
	 * Vereinigt die Ausgabe von zwei Knoten auf den ersten Knoten der übergeben wird
	 * 
	 * @param node1
	 *            - Knoten auf den die beiden Ausgaben vereinigt wird
	 * @param node2
	 *            - Menge die zur Ausgabe auf Node1 hinzugefügt wird
	 * @throws JSONException
	 */
	@SuppressWarnings("unchecked")
	static void unionOutput(Node node1, Node node2) throws JSONException {

		if (ACUtil.toLong(node2.getProperty(ACProperties.NUMBER_OUTPUT)) == 0) {
			return;
		}

		if (ACUtil.toLong(node1.getProperty(ACProperties.NUMBER_OUTPUT)) == 0) {
			node1.setProperty(ACProperties.OUTPUT, node2.getProperty(ACProperties.OUTPUT));
			node1.setProperty(ACProperties.NUMBER_OUTPUT, ((Integer) node2.getProperty(ACProperties.NUMBER_OUTPUT)));
			return;
		}

		JSONObject jsonObNode1 = new JSONObject(String.valueOf(node1.getProperty(ACProperties.OUTPUT)));
		JSONObject jsonObNode2 = new JSONObject(String.valueOf(node2.getProperty(ACProperties.OUTPUT)));

		// alles was in node2 als Ausgabe exisitiert wird in node1 gespeichert
		Iterator<String> keys = jsonObNode2.keys();
		while (keys.hasNext()) {

			String key = keys.next();

			if (!key.startsWith(ACProperties.PROPERTY) && !jsonObNode1.has(key)) {
				jsonObNode1.putOnce(key, jsonObNode2.get(key));
				node1.setProperty(ACProperties.NUMBER_OUTPUT,
						((Integer) node1.getProperty(ACProperties.NUMBER_OUTPUT)) + 1);
			}
		}

		node1.setProperty(ACProperties.OUTPUT, jsonObNode1.toString());
	}

	@SuppressWarnings("unchecked")
	static void subtractOutput(Node node1, Node node2) throws JSONException {

		if (ACUtil.toLong(node2.getProperty(ACProperties.NUMBER_OUTPUT)) == 0) {
			return;
		}

		if (ACUtil.toLong(node1.getProperty(ACProperties.NUMBER_OUTPUT)) == 0) {
			return;
		}

		JSONObject obNode1 = new JSONObject(String.valueOf(node1.getProperty(ACProperties.OUTPUT)));
		JSONObject obNode2 = new JSONObject(String.valueOf(node2.getProperty(ACProperties.OUTPUT)));

		// alles was in node2 als Ausgabe exisitiert wird in node1 gespeichert
		Iterator<String> keys = obNode2.keys();
		while (keys.hasNext()) {

			String key = keys.next();
			obNode1.remove(key);

		}

		node1.setProperty(ACProperties.NUMBER_OUTPUT, ((Integer) node1.getProperty(ACProperties.NUMBER_OUTPUT))
				- ((Integer) node2.getProperty(ACProperties.NUMBER_OUTPUT)));
		node1.setProperty(ACProperties.OUTPUT, obNode1.toString());
	}

	// ////////////////////////////////////////////
	// //////// CONVERTER

	/**
	 * Determines whether the DictTree already has his Fail-Relations or not
	 * 
	 * @param root
	 *            - Root of the DictTree
	 * @return true or false
	 */
	static boolean isDictTreePrepared(Node root) {
		return Boolean.valueOf(String.valueOf(root.getProperty(ACProperties.PREPARED)));
	}

	/**
	 * Convert Object to Long
	 * 
	 * @param ob
	 * @return
	 */
	public static long toLong(Object ob) {
		return Long.valueOf(String.valueOf(ob));
	}

	static boolean toBoolean(Object ob) {
		return Boolean.valueOf(String.valueOf(ob));
	}

	/**
	 * Erhält den Knoten, der über die FAIL-Beziehung mit dem Ausgangsknoten verbunden ist.
	 * 
	 * @param node
	 *            - Ausgangsknoten
	 * @return FAIL-Knoten
	 * @throws JSONException
	 */
	static long getFailNode(final Node node, final Node root) throws JSONException {
		JSONObject ob = new JSONObject(String.valueOf(node.getProperty(ACProperties.RELATIONSHIP)));
		if (ob.has(ACProperties.EdgeTypes.FAIL.name())) {
			return ob.getLong(ACProperties.EdgeTypes.FAIL.name());
		}
		return root.getId();
	}

	/**
	 * Writes into <tt>node</tt> that it has a relationship to <tt>newNode</tt> of type <tt>letter</tt>
	 * <p>
	 * This does not actually create any (Neo4j) relationships but just adds this information as a node property to
	 * <tt>node</tt>.
	 * </p>
	 * <p>
	 * This is to be seen as an inverse function to {@link #updateRemoveNode(ACDictionary, Node, Node, String)}.
	 * </p>
	 * 
	 * @param dict
	 * @param node
	 * @param newNode
	 * @param letter
	 * @throws JSONException
	 */
	static void updateAddNode(ACDictionary dict, Node node, Node newNode, String letter) throws JSONException {

		if (dict.getCreateMode() != ACDictionary.DEFAULT_MODE_CREATE) {
			JSONObject jsonRelMap = new JSONObject(String.valueOf(node.getProperty(ACProperties.RELATIONSHIP)));
			jsonRelMap.putOpt(letter, newNode.getId());
			node.setProperty(ACProperties.RELATIONSHIP, jsonRelMap.toString());
		}

	}

	/**
	 * Deletes the property-stored information that <tt>node</tt> has a relationship to <tt>newNode</tt> of type <tt>letter</tt>.
	 * <p>
	 * This is to be seen as an inverse function to {@link #updateAddNode(ACDictionary, Node, Node, String)}.
	 * </p>
	 * 
	 * @param dict
	 * @param node
	 * @param newNode
	 * @param letter
	 * @throws JSONException
	 */
	static void updateRemoveNode(ACDictionary dict, Node node, Node newNode, String letter) throws JSONException {

		if (dict.getCreateMode() != 0) {
			JSONObject jsonRelMap = new JSONObject(String.valueOf(node.getProperty(ACProperties.RELATIONSHIP)));
			jsonRelMap.remove(letter);
			node.setProperty(ACProperties.RELATIONSHIP, jsonRelMap.toString());
		}

	}

	/**
	 * Increments the counter of the number of descending tree nodes by one. 
	 * @param node
	 */
	static void increaseNumberNext(Node node) {
		node.setProperty(ACProperties.NUMBER_NEXT, ACUtil.toLong(node.getProperty(ACProperties.NUMBER_NEXT)) + 1);
	}

	/**
	 * Decrements the counter of the number of descending tree nodes by one. 
	 * @param node
	 */
	static void decreaseNumberNext(Node node) {
		node.setProperty(ACProperties.NUMBER_NEXT, ACUtil.toLong(node.getProperty(ACProperties.NUMBER_NEXT)) - 1);
	}

	static Comparator<Node> getComperator() {
		return new Deepth();
	}

}

class Deepth implements Comparator<Node> {

	private long getDeepth(Node o1) {
		return ACUtil.toLong(o1.getProperty(ACProperties.DEPTH));
	}

	@Override
	public int compare(Node o1, Node o2) {
		long deepthN1 = getDeepth(o1);
		long deepthN2 = getDeepth(o2);

		if (deepthN1 - deepthN2 < 0) {
			return -1;
		}

		if (deepthN1 - deepthN2 > 0) {
			return 1;
		}

		return 0;
	}

}
