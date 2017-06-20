package de.julielab.neo4j.plugins.ahocorasick;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.shell.util.json.JSONException;
import org.neo4j.shell.util.json.JSONObject;
import org.xml.sax.SAXException;

import com.google.common.collect.Lists;
import com.google.gson.Gson;

import de.julielab.neo4j.plugins.AhoCorasickXmlTreeReader;
import de.julielab.neo4j.plugins.ahocorasick.property.ACDataBase;
import de.julielab.neo4j.plugins.ahocorasick.property.ACDictionary;
import de.julielab.neo4j.plugins.ahocorasick.property.ACQuery;
import de.julielab.neo4j.plugins.test.TestUtilities;

//@Ignore
public class ACFactoryEmbeddedTest {

	private static AhoCorasickXmlTreeReader ahoXmlReader;
	private static Gson g;
	private static ACDataBase db;

	@BeforeClass
	public static void initialize() throws SAXException, IOException, ParserConfigurationException {
		g = new Gson();
		db = new ACDataBase(TestUtilities.GRAPH_DB_DIR);
		
		ahoXmlReader = new AhoCorasickXmlTreeReader("src/test/resources/trees.xml");
	}

	@Before
	public void cleanForTest() throws IOException {
		TestUtilities.deleteEverythingInDB(db.startGraphDatabase());
		db.stopGraphDatabase();
	}
	
	@SuppressWarnings({ "rawtypes" })
	@Test
	public void testCreateAndDelete() throws Exception {
		GraphDatabaseService graphDb = db.startGraphDatabase();
		int start = 0;
		
		
		// CREATE AND PREPARE
		for (int i = start; i < ahoXmlReader.numbersOfTrees(); i++) {
			for(int modeCreate = 2; modeCreate >= 0; modeCreate--){
				for(int modeSearch = 0; modeSearch >= 0 && modeSearch <=modeCreate && modeSearch<2; modeSearch++){
					System.out.println("Dict :"+i+" Mode: "+modeCreate + " "+ modeSearch);
					List entries = ahoXmlReader.getListOfEntriesByTree(i, false);
					String json = g.toJson(entries);
					
					/*** CREATE A DICT TREE ***/
					
					// // Create Root ////
					String DICT = "Dict" + i + modeCreate + modeSearch;
					ACDictionary dict = new ACDictionary(DICT, modeCreate, modeSearch);
					ACDictionary dictFalse = new ACDictionary("Dict", modeCreate, modeSearch);
					boolean createDict = ACFactoryEmbedded.createDictTree(graphDb, g.toJson(dict));
					
					// TEST METHODE
					assertTrue(createDict);
					assertTrue(!ACFactoryEmbedded.createDictTree(graphDb, g.toJson(dict)));
					
					// TEST FSM STRUCTURE
					try (Transaction tx = graphDb.beginTx()) {
						assertTrue(graphDb.findNodesByLabelAndProperty(ACProperties.LabelTypes.DICTIONARY, 
								ACProperties.DICTIONARY_NAME, DICT).iterator().hasNext());
						
						// // Add Entries ////
						tx.success();
					}
					
					assertTrue(ACFactoryEmbedded.addListToDictTree(graphDb, dict.toJSONString(), json));
					assertTrue(!ACFactoryEmbedded.addListToDictTree(graphDb, dictFalse.toJSONString(), json));
					// TEST FSM STRUCTURE
					Node root_dict;
					Map<String, String> propertyMap;
					
					try (Transaction tx = graphDb.beginTx()) {
						root_dict = graphDb.findNodesByLabelAndProperty(ACProperties.LabelTypes.DICTIONARY, 
								ACProperties.DICTIONARY_NAME, DICT).iterator().next();
						propertyMap = ahoXmlReader.getPropertiesOfTree(i);
						
						assertTrue(root_dict != null);
						// Wieviele Knoten hat der Baum "Wurzel-Info"
						assertEquals(ACUtil.toLong(propertyMap.get(ahoXmlReader.ALLNODES)), 
								ACUtil.toLong(root_dict.getProperty(ACProperties.NODES_IN_TREE)));
						// Wieviel Einträge gibt es "Wurzel-Info"
						assertEquals(entries.size(), 
								ACUtil.toLong(root_dict.getProperty(ACProperties.NUMBER_OF_ENTRIES)));
						// Wieviel Fail-Relationen gibt es
						assertEquals(0, numberOfFailRelation(root_dict));
						// Wieviele Knoten hat der Baum "Per Hand gezählt"
						assertEquals(ACUtil.toLong(propertyMap.get(ahoXmlReader.ALLNODES)), 
								nodesInTree(graphDb, root_dict) + 1); // Zähle wurzel nicht mit
						// Wieviel Einträge gibt es "Per Hand gezählt"
						assertEquals(entries.size(), entriesInTree(graphDb, root_dict));
						// Ist Baum vorbereitet? FALSE
						assertTrue(!ACUtil.toBoolean(root_dict.getProperty(ACProperties.PREPARED)));
						// Keine Suche kann stattfinden bisher
						assertTrue("" == ACSearch.search(graphDb, dict.toJSONString(), 
								new ACQuery("test", ACSearch.FULL_WITH_OVERLAPPING).toJSONString()));
						// Number Next korrekt gesetzt?
						assertTrue(numberNext(graphDb, root_dict));
						
						if(dict.isLocalCreate() || dict.isGlobalCreate()){
							// Stimmen die Relationshipsmaps mit den wirklichen überein?
							assertTrue(areRelationsSame(graphDb, root_dict));
						}
						
						tx.success();
					}
					
					// // Prepare Tree /////
					System.out.println("Prepare Start");
					boolean prepareDict = ACFactoryEmbedded.prepareDictTreeForSearch(graphDb, g.toJson(dict));
					System.out.println("Prepare Ende");
					
					try (Transaction tx = graphDb.beginTx()) {
						// TEST FSM STRUCTURE
						// Ist das Wörterbuch vorbereitet?
						assertTrue(prepareDict);
						assertTrue(ACUtil.toBoolean(root_dict.getProperty(ACProperties.PREPARED)));
						// Anzahl der Fail-Relationen
						assertEquals(ACUtil.toLong(propertyMap.get(ahoXmlReader.ALLNODES)) - 1, numberOfFailRelation(root_dict));
						// Wieviele Knoten hat der Baum "Wurzel-Info"
						assertEquals(ACUtil.toLong(propertyMap.get(ahoXmlReader.ALLNODES)), 
								ACUtil.toLong(root_dict.getProperty(ACProperties.NODES_IN_TREE)));
						// Wieviele Knoten hat der Baum "Per Hand gezählt"
						assertEquals(ACUtil.toLong(propertyMap.get(ahoXmlReader.ALLNODES)), 
								nodesInTree(graphDb, root_dict));
						// Wieviele Incoming Fail-Relationen in Root existieren
						assertEquals(ACUtil.toLong(propertyMap.get(ahoXmlReader.FAILRELINROOTS)), 
								countIncomingFailRelationsInNode(graphDb, root_dict));
						// Wieviele Output Nodes gibt es?
						assertEquals(ACUtil.toLong(propertyMap.get(ahoXmlReader.FINALNODES)), 
								countOutputNodes(graphDb, root_dict));
						// Wieviel Original Nodes gibt es?
						assertEquals(entries.size(), countNodesWithOriginal(graphDb, root_dict));
						// Number Next korrekt gesetzt?
						assertTrue(numberNext(graphDb, root_dict));
						
						if(dict.isLocalCreate() || dict.isGlobalCreate()){
							// Stimmen die Relationshipsmaps mit den wirklichen überein?
							assertTrue(areRelationsSame(graphDb, root_dict));
						}
						
						tx.success();
					}
					
					// Darf nichts ergänzen, weil Baum schon prepared ist
					assertTrue(!ACFactoryEmbedded.addListToDictTree(graphDb, g.toJson(dict), json));
					
					// // Unprepare Tree /////
					boolean unprepareDict = ACFactoryEmbedded.unprepareDictTree(graphDb, DICT);
					
					try (Transaction tx = graphDb.beginTx()) {
						assertTrue(unprepareDict);
						// Wieviele Knoten hat der Baum "Wurzel-Info"
						assertEquals(ACUtil.toLong(propertyMap.get(ahoXmlReader.ALLNODES)), 
								ACUtil.toLong(root_dict.getProperty(ACProperties.NODES_IN_TREE)));
						// Wieviel Einträge gibt es "Wurzel-Info"
						assertEquals(entries.size(), 
								ACUtil.toLong(root_dict.getProperty(ACProperties.NUMBER_OF_ENTRIES)));
						// Wieviel Fail-Relationen gibt es
						assertEquals(0, numberOfFailRelation(root_dict));
						// Wieviele Knoten hat der Baum "Per Hand gezählt"
						assertEquals(ACUtil.toLong(propertyMap.get(ahoXmlReader.ALLNODES)), 
								nodesInTree(graphDb, root_dict) + 1); // Zähle wurzel nicht mit
						// Wieviel Einträge gibt es "Per Hand gezählt"
						assertEquals(entries.size(), entriesInTree(graphDb, root_dict));
						// Ist Baum vorbereitet? FALSE
						assertTrue(!ACUtil.toBoolean(root_dict.getProperty(ACProperties.PREPARED)));
						// Keine Suche kann stattfinden bisher
						assertTrue("" == ACSearch.search(graphDb, dict.toJSONString(), 
								new ACQuery("test", ACSearch.FULL_WITH_OVERLAPPING).toJSONString()));
						// Number Next korrekt gesetzt?
						assertTrue(numberNext(graphDb, root_dict));
						
						tx.success();
					}
				}
			}
		}
		
		// DELETE
		for (int i = start; i < ahoXmlReader.numbersOfTrees(); i++) {
			for(int modeCreate = 2; modeCreate >= 0; modeCreate--){
				for(int modeSearch = 0; modeSearch >= 0 && modeSearch <=modeCreate && modeSearch<2; modeSearch++){
					String DICT = "Dict" + i + modeCreate + modeSearch;
					
					try (Transaction tx = graphDb.beginTx()) {
						// Root still exist
						System.out.println("Checking dictionary root " + DICT);
						assertTrue(graphDb.findNodesByLabelAndProperty(ACProperties.LabelTypes.DICTIONARY, 
								ACProperties.DICTIONARY_NAME, DICT).iterator().hasNext());
						
						tx.success();
					}
					
					// Delete Tree
					assertTrue(ACFactoryEmbedded.deleteDictTree(graphDb, DICT));
					
					try (Transaction tx = graphDb.beginTx()) {
						// Root does not exist anymore
						assertTrue(!graphDb.findNodesByLabelAndProperty(ACProperties.LabelTypes.DICTIONARY, 
								ACProperties.DICTIONARY_NAME, DICT).iterator().hasNext());
						
						tx.success();
					}
				}
			}
		}
		
		for (int i = start; i < ahoXmlReader.numbersOfTrees(); i++) {
			for(int modeCreate = 2; modeCreate >= 0; modeCreate--){
				for(int modeSearch = 0; modeSearch >= 0 && modeSearch <=modeCreate && modeSearch<2; modeSearch++){
					String DICT = "Dict" + i + modeCreate + modeSearch;
					// It can't delete anything
					assertTrue(!ACFactoryEmbedded.deleteDictTree(graphDb, DICT));
				}
			}
		}
		// Is Database empty?
		try (Transaction tx = graphDb.beginTx()) {
			assertEquals(0, countAllNodesInDatabase(graphDb));
			tx.success();
		}
	}
	
	@SuppressWarnings("rawtypes")
	@Test
	public void testAddToPreparedDict() throws JSONException, IOException {
		GraphDatabaseService graphDb = db.startGraphDatabase();
		int start = 0;
		
		// CREATE AND PREPARE
		for (int runs = 0; runs < 1; runs++) {
			for (int i = start; i < ahoXmlReader.numbersOfTrees(); i++) {
				for(int modeCreate = 2; modeCreate >= 0; modeCreate--){
					for(int modeSearch = 0; modeSearch >= 0 && modeSearch <=modeCreate && modeSearch<2; modeSearch++){
						System.out.println("Dict :"+i+" Mode: "+modeCreate + " "+ modeSearch);
						List entries = ahoXmlReader.getListOfEntriesByTree(i, false);
//						Collections.shuffle(entries);
						
						/*** CREATE A DICT TREE ***/
						
						// // Create Root ////
						String DICT = "Dict" + i + modeCreate + modeSearch;
						ACDictionary dict = new ACDictionary(DICT, modeCreate, modeSearch);
						boolean createDict = ACFactoryEmbedded.createDictTree(graphDb, g.toJson(dict));
						
						// TEST METHODE
						assertTrue(createDict);
						assertTrue(!ACFactoryEmbedded.createDictTree(graphDb, g.toJson(dict)));
						
						Node root_dict = null;
						Map<String, String> propertyMap = ahoXmlReader.getPropertiesOfTree(i);
						
						try (Transaction tx = graphDb.beginTx()) {
							// TEST FSM STRUCTURE
							assertTrue(graphDb.findNodesByLabelAndProperty(ACProperties.LabelTypes.DICTIONARY, 
									ACProperties.DICTIONARY_NAME, dict.name()).iterator().hasNext());
							
							// TEST FSM STRUCTURE
							root_dict = graphDb.findNodesByLabelAndProperty(ACProperties.LabelTypes.DICTIONARY, 
									ACProperties.DICTIONARY_NAME, dict.name()).iterator().next();
							
							assertTrue(root_dict != null);
							
							tx.success();
						}
						
						// PREPARE TREE
						boolean prepareDict = ACFactoryEmbedded.prepareDictTreeForSearch(graphDb, g.toJson(dict));
						assertTrue(prepareDict);
						
						try (Transaction tx = graphDb.beginTx()) {
							assertTrue(ACUtil.toBoolean(root_dict.getProperty(ACProperties.PREPARED)));
							tx.success();
						}
						
						for (int x = 0; x < entries.size(); x++) {
							List add = Lists.newArrayList(entries.get(x));
							String json = g.toJson(add);
							assertTrue(ACFactoryEmbedded.addListToPreparedDictTree(graphDb, g.toJson(dict), json));
							assertTrue(!ACFactoryEmbedded.addListToDictTree(graphDb, g.toJson(dict), json));
						}
						
						// TEST FSM STRUCTURE
						
						try (Transaction tx = graphDb.beginTx()) {
							// Anzahl der Fail-Relationen
							assertEquals(ACUtil.toLong(propertyMap.get(ahoXmlReader.ALLNODES)) - 1, 
									numberOfFailRelation(root_dict));
							// Wieviele Knoten hat der Baum "Wurzel-Info"
							assertEquals(ACUtil.toLong(propertyMap.get(ahoXmlReader.ALLNODES)), 
									ACUtil.toLong(root_dict.getProperty(ACProperties.NODES_IN_TREE)));
							// Wieviele Knoten hat der Baum "Per Hand gezählt"
							assertEquals(ACUtil.toLong(propertyMap.get(ahoXmlReader.ALLNODES)), 
									nodesInTree(graphDb, root_dict));
							// Wieviele Incoming Fail-Relationen in Root existieren
							assertEquals(ACUtil.toLong(propertyMap.get(ahoXmlReader.FAILRELINROOTS)), 
									countIncomingFailRelationsInNode(graphDb, root_dict));
							// Wieviele Output Nodes gibt es?
							assertEquals(ACUtil.toLong(propertyMap.get(ahoXmlReader.FINALNODES)), 
									countOutputNodes(graphDb, root_dict));
							// Wieviel Original Nodes gibt es?
							assertEquals(entries.size(), 
									countNodesWithOriginal(graphDb, root_dict));
							
							if(dict.isLocalSearch()){
								// Stimmen die Relationshipsmaps mit den wirklichen überein?
								assertTrue(areRelationsSame(graphDb, root_dict));
							}
							tx.success();
						}
					}	
				}
			}

			// DELETE
			for (int i = start; i < ahoXmlReader.numbersOfTrees(); i++) {
				for(int modeCreate = 2; modeCreate >= 0; modeCreate--){
					for(int modeSearch = 0; modeSearch >= 0 && modeSearch <=modeCreate && modeSearch<2; modeSearch++){
						String DICT = "Dict" + i + modeCreate + modeSearch;
						// Root still exist
						try (Transaction tx = graphDb.beginTx()) {
							assertTrue(graphDb.findNodesByLabelAndProperty(ACProperties.LabelTypes.DICTIONARY, 
									ACProperties.DICTIONARY_NAME, DICT).iterator().hasNext());
							tx.success();
						}
						// Delete Tree
						assertTrue(ACFactoryEmbedded.deleteDictTree(graphDb, DICT));
						
						// Root does not exist anymore
						try (Transaction tx = graphDb.beginTx()) {
							assertTrue(!graphDb.findNodesByLabelAndProperty(ACProperties.LabelTypes.DICTIONARY, 
									ACProperties.DICTIONARY_NAME, DICT).iterator().hasNext());
							tx.success();
						}
					}
				}
			}

			for (int i = start; i < ahoXmlReader.numbersOfTrees(); i++) {
				for(int modeCreate = 2; modeCreate >= 0; modeCreate--){
					for(int modeSearch = 0; modeSearch >= 0 && modeSearch <=modeCreate && modeSearch<2; modeSearch++){
						String DICT = "Dict" + i + modeCreate + modeSearch;
						// It can't delete anything
						assertTrue(!ACFactoryEmbedded.deleteDictTree(graphDb, DICT));
					}
				}
			}
			// Is Database empty?
			try (Transaction tx = graphDb.beginTx()) {
				assertEquals(0, countAllNodesInDatabase(graphDb));
				tx.close();
			}
		}
	}
	
	@AfterClass
	public static void shutdown() {
		db.stopBatchInserter();
		db.stopGraphDatabase();
	}
	
	/////////////////////////////////////////////
	//////// HELP FUNCTIONS
	
	private long countIncomingFailRelationsInNode(GraphDatabaseService graphDb, Node root) {
		long count = 0;

		ExecutionEngine engine = new ExecutionEngine(graphDb);
		ExecutionResult result = engine.execute("START n = node(" + root.getId() + ") MATCH (n)<-[:FAIL]-(m) RETURN count(DISTINCT m)");
		ResourceIterator<Long> iter = result.columnAs("count(DISTINCT m)");
		while (iter.hasNext()) {
			count = count + iter.next();
		}

		return count;
	}

	private long countNodesWithOriginal(GraphDatabaseService graphDb, Node root) {

		long count = 0;

		ExecutionEngine engine = new ExecutionEngine(graphDb);
		ExecutionResult result = engine.execute("START n = node(" + root.getId() + ") MATCH (n)-[*]->(m)	WHERE HAS(m.property_original) RETURN count(DISTINCT m)");
		ResourceIterator<Long> iter = result.columnAs("count(DISTINCT m)");
		while (iter.hasNext()) {
			count = count + iter.next();
		}

		return count;
	}

	private long countAllNodesInDatabase(GraphDatabaseService graphDb) {

		long count = 0;

		ExecutionEngine engine = new ExecutionEngine(graphDb);
		ExecutionResult result = engine.execute("START n = node(*) RETURN count(n)");
		ResourceIterator<Long> iter = result.columnAs("count(n)");
		while (iter.hasNext()) {
			count = count + iter.next();
		}

		return count;
	}

	/**
	 * 
	 * @param root
	 * @return
	 */
	private long countOutputNodes(GraphDatabaseService graphDb, Node root) {
		long count = 0;

		ExecutionEngine engine = new ExecutionEngine(graphDb);
		ExecutionResult result = engine.execute("START n = node(" + root.getId() + ") MATCH n-[*]->(m) WHERE m.property_number_Output>0 RETURN count(DISTINCT m)");
		ResourceIterator<Long> iter = result.columnAs("count(DISTINCT m)");
		while (iter.hasNext()) {
			count = count + iter.next();
		}

		return count;
	}

	/**
	 * Number of Fail Relation
	 * 
	 * @param root
	 * @return
	 */
	private long numberOfFailRelation(Node root) {

		int count = 0;
		Deque<Node> dek = new ArrayDeque<Node>();
		dek.add(root);
		
		while(!dek.isEmpty()){
			Node node = dek.pop();
			Iterator<Relationship> iterOut = node.getRelationships(Direction.OUTGOING).iterator();
			while(iterOut.hasNext()){
				Relationship out = iterOut.next();
				if(!out.isType(ACProperties.EdgeTypes.FAIL)){
					dek.add(out.getEndNode());
				}
			}
			
			Iterator<Relationship> iterIncoming = node.getRelationships(ACProperties.EdgeTypes.FAIL, Direction.INCOMING).iterator();
			while(iterIncoming.hasNext()){
				iterIncoming.next();
				count++;
			}
		}
		
		
		return count;
	}
	
	/**
	 * Count all states in the DictTree
	 * 
	 * @param root
	 * @return
	 */
	private long nodesInTree(GraphDatabaseService graphDb, Node root) {

		long count = 0;

		ExecutionEngine engine = new ExecutionEngine(graphDb);
		ExecutionResult result = engine.execute("start n=node(" + root.getId() + ") MATCH (n)-[*]->(m) Return count(DISTINCT m) ");
		ResourceIterator<Long> iter = result.columnAs("count(DISTINCT m)");
		while (iter.hasNext()) {
			count = count + iter.next();
		}

		return count;
	}

	/**
	 * Count all words in a Tree
	 * 
	 * @param root
	 *            - Root of the DictTree
	 * @return number of Entries in the Dict Tree
	 */
	private long entriesInTree(GraphDatabaseService graphDb, Node root) {

		long count = 0;

		ExecutionEngine engine = new ExecutionEngine(graphDb);
		ExecutionResult result = engine.execute("START n = node(" + root.getId() + ") MATCH (n)-[*]->(m) WHERE HAS(m.property_original) RETURN count(DISTINCT m)");
		ResourceIterator<Long> iter = result.columnAs("count(DISTINCT m)");
		while (iter.hasNext()) {
			count = count + iter.next();
		}

		return count;

	}
	
	private boolean areRelationsSame(GraphDatabaseService graphDb, Node root) throws JSONException {
		
		Deque<Node> deq = new ArrayDeque<>();
		deq.add(root);
		
		while(!deq.isEmpty()){
			
			Node currentNode = deq.pop();
			
			Iterator<Relationship> iter = currentNode.getRelationships(Direction.OUTGOING).iterator();
			JSONObject obProp = new JSONObject(String.valueOf(currentNode.getProperty(ACProperties.RELATIONSHIP)));
			
			while(iter.hasNext()){
				Relationship rel = iter.next();
				String nameRel = rel.getType().name();
				
				// IS FAIL RELATION
				if(nameRel.equals(ACProperties.getFailName())){
					if(currentNode.getId() != root.getId() && !obProp.has(ACProperties.EdgeTypes.FAIL.name())){
						return false;
					}
					// IS NO FAIL RELATION
				} else{
					deq.add(rel.getEndNode());
					if(!obProp.has((String) rel.getProperty(ACProperties.LETTER))){
						return false;
					}
					if(ACUtil.toLong(obProp.get((String) rel.getProperty(ACProperties.LETTER)))!=rel.getEndNode().getId()){
						return false;
					}
				}
			}
		}		
		return true;
	}
	
	private boolean numberNext(GraphDatabaseService graphDb, Node root) throws JSONException {
		
		Deque<Node> deq = new ArrayDeque<>();
		deq.add(root);
		
		while(!deq.isEmpty()){
			
			Node current = deq.pop();
			
			Iterator<Relationship> iter = current.getRelationships(Direction.OUTGOING).iterator();
			
			int countNext = 0;
			
			while(iter.hasNext()){
				Relationship rel = iter.next();
				String nameRel = rel.getType().name();
				
				// We just look at outgoing Relationships
				if(!nameRel.equals(ACProperties.getFailName())){
					deq.add(rel.getEndNode());
				}
				
				countNext++;
			}
			
			if(countNext != ACUtil.toLong(current.getProperty(ACProperties.NUMBER_NEXT))){
				return false;
			}
		}		
		
		return true;
	}
}
