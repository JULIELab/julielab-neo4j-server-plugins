package de.julielab.neo4j.plugins;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.RecursiveMappingRepresentation;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.server.rest.repr.ValueRepresentation;
import org.neo4j.shell.util.json.JSONException;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.xml.sax.SAXException;

import scala.collection.mutable.HashSet;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

// Ignore as long as the AhoCorasick plugin is not updated to Neo4j 2.0.0M6
//@Ignore
public class AhoCorasickTest {

//	private static GraphDatabaseService graphDb;
//	private static AhoCorasick ahoCorasick;
//	private static AhoCorasickXmlTreeReader ahoXmlReader;
//	private static Gson g;
//
//	@BeforeClass
//	public static void initialize() throws SAXException, IOException, ParserConfigurationException {
//		graphDb = TestUtilities.getGraphDB();
//		ahoCorasick = new AhoCorasick();
//
//		g = new Gson();
//
//		ahoXmlReader = new AhoCorasickXmlTreeReader("src/test/resources/trees.xml");
//		ahoXmlReader.getListOfEntriesByTree(0);
//	}
//
//	// Löscht vor jeder Funktion die Datenbank
//	@Before
//	public void cleanForTest() throws IOException {
//		TestUtilities.deleteEverythingInDB(graphDb);
//	}
//
//	@SuppressWarnings({ "rawtypes" })
//	@Test
//	public void testCreateAndDelete() throws Exception {
//
//		BatchInserter batchDb =
//		        BatchInserters.inserter( TestUtilities.GRAPH_DB_DIR_2);
//		BatchInserterIndexProvider indexProvider =
//		        new LuceneBatchInserterIndexProvider( batchDb );
//		BatchInserterIndex personen =
//		        indexProvider.nodeIndex( "personen", MapUtil.stringMap( "type", "exact" ) );
//		
//		Label personLabel = DynamicLabel.label( "Person" );
//		
//		HashMap<String, Object> prop1 = new HashMap<>();
//		prop1.put("alter", 12);
//		
//		HashMap<String, Object> prop2 = new HashMap<>();
//		prop2.put("alter", 12);
//		
//		long mattiasNode = batchDb.createNode( prop1, personLabel );
//		long chrisNode = batchDb.createNode( prop2, personLabel);
//
//		personen.add(chrisNode, prop1);
//		personen.add(mattiasNode, prop2);
//		
//		personen.flush();
//		
//		batchDb.createRelationship(mattiasNode, chrisNode, DynamicRelationshipType.withName("s"), null);
////		mattiasNode.createRelationshipTo( chrisNode, knows );/
//		
//		indexProvider.shutdown();
//		batchDb.shutdown();
//		
//		batchDb =
//		        BatchInserters.inserter( TestUtilities.GRAPH_DB_DIR_2);
//		indexProvider =
//		        new LuceneBatchInserterIndexProvider( batchDb );
//		personen =
//		        indexProvider.nodeIndex( "personen", MapUtil.stringMap( "type", "exact" ) );
//		
//		IndexHits<Long> hits = personen.get("alter", 12);
//		System.out.println(hits.size());
//		
//		indexProvider.shutdown();
//		batchDb.shutdown();
//		
////		long old = mattiasNode.getId();
////		
////		GraphDatabaseService database = new GraphDatabaseFactory().newEmbeddedDatabase( TestUtilities.GRAPH_DB_DIR_2);
////		try{
////			database.createNode();
////		}catch (Exception e) {
////			System.out.println("is so");
////		}
////		
////		Transaction tx = database.beginTx();
////		try{
////			GlobalGraphOperations.at(database).getAllNodes();
////		}finally{
////			tx.success();
////		}
//		
////		batchDb = BatchInserters.batchDatabase( TestUtilities.GRAPH_DB_DIR_2);
////		mattiasNode = batchDb.createNode( personLabel );
////		
////		System.out.println(batchDb.getNodeById(old));
////		batchDb.shutdown();
//
//		
////		
////		// CREATE AND PREPARE
////		for (int i = 4; i < ahoXmlReader.numbersOfTrees(); i++) {
////			List entries = ahoXmlReader.getListOfEntriesByTree(i);
////			String json = g.toJson(entries);
////
////			/*** CREATE A DICT TREE ***/
////
////			// // Create Root ////
////			String DICT = "Dict" + i;
////			boolean createDict = ahoCorasick.createDictTree(graphDb, DICT);
////
////			// TEST METHODE
////			assertTrue(createDict);
////			assertTrue(!ahoCorasick.createDictTree(graphDb, DICT));
////
////			// TEST FSM STRUCTURE
////			try (Transaction tx = graphDb.beginTx()) {
////				assertTrue(graphDb.findNodesByLabelAndProperty(AhoCorasick.LabelTypes.DICTIONARY, ahoCorasick.DICTIONARY_NAME, DICT).iterator().hasNext());
////
////				// // Add Entries ////
////				tx.success();
////			}
////			assertTrue(ahoCorasick.addListToDictTree(graphDb, DICT, json));
////			assertTrue(!ahoCorasick.addListToDictTree(graphDb, "Dict", json));
////			// TEST FSM STRUCTURE
////			Node root_dict;
////			Map<String, String> propertyMap;
////			try (Transaction tx = graphDb.beginTx()) {
////				root_dict = graphDb.findNodesByLabelAndProperty(AhoCorasick.LabelTypes.DICTIONARY, ahoCorasick.DICTIONARY_NAME, DICT).iterator().next();
////			propertyMap = ahoXmlReader.getPropertiesOfTree(i);
////
////			assertTrue(root_dict != null);
////			// Wieviele Knoten hat der Baum "Wurzel-Info"
////			assertEquals(toLong(propertyMap.get(ahoXmlReader.ALLNODES)), toLong(root_dict.getProperty(ahoCorasick.NODES_IN_TREE)));
////			// Wieviel Einträge gibt es "Wurzel-Info"
////			assertEquals(entries.size(), toLong(root_dict.getProperty(ahoCorasick.NUMBER_OF_ENTRIES)));
////			// Wieviel Fail-Relationen gibt es
////			assertEquals(0, numberOfFailRelation(root_dict));
////			// Wieviele Knoten hat der Baum "Per Hand gezählt"
////			assertEquals(toLong(propertyMap.get(ahoXmlReader.ALLNODES)), nodesInTree(root_dict) + 1); // Zähle wurzel nicht mit
////			// Wieviel Einträge gibt es "Per Hand gezählt"
////			assertEquals(entries.size(), entriesInTree(root_dict));
////			// Ist Baum vorbereitet? FALSE
////			assertTrue(!toBoolean(root_dict.getProperty(ahoCorasick.PREPARED)));
////			// Keine Suche kann stattfinden bisher
////			assertTrue(null == ahoCorasick.completeSearch(graphDb, DICT, "test"));
////			tx.success();
////			}
////
////			// // Prepare Tree /////
////			boolean prepareDict = ahoCorasick.prepareDictTreeForSearch(graphDb, DICT);
////
////			// TEST FSM STRUCTURE
////			// Ist das Wörterbuch vorbereitet?
////			assertTrue(prepareDict);
////			assertTrue(toBoolean(root_dict.getProperty(ahoCorasick.PREPARED)));
////			// Anzahl der Fail-Relationen
////			assertEquals(toLong(propertyMap.get(ahoXmlReader.ALLNODES)) - 1, numberOfFailRelation(root_dict));
////			// Darf nichts ergänzen, weil Baum schon prepared ist
////			assertTrue(!ahoCorasick.addListToDictTree(graphDb, DICT, json));
////			// Wieviele Knoten hat der Baum "Wurzel-Info"
////			assertEquals(toLong(propertyMap.get(ahoXmlReader.ALLNODES)), toLong(root_dict.getProperty(ahoCorasick.NODES_IN_TREE)));
////			// Wieviele Knoten hat der Baum "Per Hand gezählt"
////			assertEquals(toLong(propertyMap.get(ahoXmlReader.ALLNODES)), nodesInTree(root_dict));
////			// Wieviele Incoming Fail-Relationen in Root existieren
////			assertEquals(toLong(propertyMap.get(ahoXmlReader.FAILRELINROOTS)), countIncomingFailRelationsInNode(root_dict));
////			// Wieviele Output Nodes gibt es?
////			assertEquals(toLong(propertyMap.get(ahoXmlReader.FINALNODES)), countOutputNodes(root_dict));
////			// Wieviel Original Nodes gibt es?
////			assertEquals(entries.size(), countNodesWithOriginal(root_dict));
////
////			// // Unprepare Tree /////
////			boolean unprepareDict = ahoCorasick.unprepareDictTree(graphDb, DICT);
////
////			assertTrue(unprepareDict);
////			// Wieviele Knoten hat der Baum "Wurzel-Info"
////			assertEquals(toLong(propertyMap.get(ahoXmlReader.ALLNODES)), toLong(root_dict.getProperty(ahoCorasick.NODES_IN_TREE)));
////			// Wieviel Einträge gibt es "Wurzel-Info"
////			assertEquals(entries.size(), toLong(root_dict.getProperty(ahoCorasick.NUMBER_OF_ENTRIES)));
////			// Wieviel Fail-Relationen gibt es
////			assertEquals(0, numberOfFailRelation(root_dict));
////			// Wieviele Knoten hat der Baum "Per Hand gezählt"
////			assertEquals(toLong(propertyMap.get(ahoXmlReader.ALLNODES)), nodesInTree(root_dict) + 1); // Zähle wurzel nicht mit
////			// Wieviel Einträge gibt es "Per Hand gezählt"
////			assertEquals(entries.size(), entriesInTree(root_dict));
////			// Ist Baum vorbereitet? FALSE
////			assertTrue(!toBoolean(root_dict.getProperty(ahoCorasick.PREPARED)));
////			// Keine Suche kann stattfinden bisher
////			assertTrue(null == ahoCorasick.completeSearch(graphDb, DICT, "test"));
////		}
////
////		// DELETE
////		for (int i = 4; i < ahoXmlReader.numbersOfTrees(); i++) {
////			String DICT = "Dict" + i;
////			// Root still exist
////			assertTrue(graphDb.findNodesByLabelAndProperty(AhoCorasick.LabelTypes.DICTIONARY, ahoCorasick.DICTIONARY_NAME, DICT).iterator().hasNext());
////			// Delete Tree
////			assertTrue(ahoCorasick.deleteDictTree(graphDb, DICT));
////			// Root does not exist anymore
////			assertTrue(!graphDb.findNodesByLabelAndProperty(AhoCorasick.LabelTypes.DICTIONARY, ahoCorasick.DICTIONARY_NAME, DICT).iterator().hasNext());
////		}
////
////		for (int i = 4; i < ahoXmlReader.numbersOfTrees(); i++) {
////			String DICT = "Dict" + i;
////			// It can't delete anything
////			assertTrue(!ahoCorasick.deleteDictTree(graphDb, DICT));
////		}
////		// Is Database empty?
////		assertEquals(1, countAllNodesInDatabase());
//	}
//
//	@SuppressWarnings({ "rawtypes", "unchecked" })
//	@Test
//	public void testAttributMethode() throws Exception {
//
//		final int DELETE = 1;
//		final int ADD = 2;
//		final int CHANGE = 3;
//
//		Field test = ValueRepresentation.class.getDeclaredField("value");
//		test.setAccessible(true);
//
//		Field responseContent = ListRepresentation.class.getDeclaredField("content");
//		responseContent.setAccessible(true);
//
//		Random rand = new Random(System.currentTimeMillis());
//
//		// Attribute Random add, change and delete
//		for (int i = 4; i < ahoXmlReader.numbersOfTrees(); i++) {
//			List entries = ahoXmlReader.getListOfEntriesByTree(i);
//			String json = g.toJson(entries);
//
//			/*** CREATE A DICT TREE ***/
//
//			// // Create DicTree ////
//			String DICT = "Dict" + i;
//			boolean createDict = ahoCorasick.createDictTree(graphDb, DICT);
//			assertTrue(createDict);
//			assertTrue(ahoCorasick.addListToDictTree(graphDb, DICT, json));
//
//			boolean prepareDict = ahoCorasick.prepareDictTreeForSearch(graphDb, DICT);
//			assertTrue(prepareDict);
//
//			Node root_dict = graphDb.findNodesByLabelAndProperty(AhoCorasick.LabelTypes.DICTIONARY, ahoCorasick.DICTIONARY_NAME, DICT).iterator().next();
//
//			assertTrue(root_dict != null);
//
//			// Change randomly Attributes
//			for (int j = 0; j < 15; j++) {
//				double random = rand.nextDouble();
//				int modus = 0;
//
//				if (random < 0.333) {
//					modus = DELETE;
//				} else if (random < 0.666) {
//					modus = ADD;
//				} else {
//					modus = CHANGE;
//				}
//
//				Map<String, Object> entry = null;
//				Map<String, String> attribute = null;
//
//				switch (modus) {
//				case DELETE:
//					// GET A RANDOM ENTRY TO CHANGE A ATTRIBUTE
//					while (entry == null) {
//						entry = (Map<String, Object>) entries.get(rand.nextInt(entries.size()));
//						HashMap<String, String> attr = (HashMap<String, String>) entry.get(ahoCorasick.ATTRIBUTES);
//						if (attr.isEmpty()) {
//							entry = null;
//						}
//					}
//
//					// GET ATTRIBUTE LIST AND DELETE ONE ENTRY
//					attribute = (Map<String, String>) entry.get(ahoCorasick.ATTRIBUTES);
//					List deleteList = Lists.newArrayList(attribute.keySet().iterator().next());
//
//					// TEST METHODE
//					assertTrue(ahoCorasick.deleteAttributeOfEntry(graphDb, DICT, String.valueOf(entry.get(ahoCorasick.ENTRY)), g.toJson(deleteList)));
//					attribute.remove(deleteList.get(0));
//
//					break;
//				/********** ADD *****************/
//				case ADD:
//					// ADD TO A RANDOM ENTRY AN ATTRIBUTE
//					while (entry == null) {
//						entry = (Map<String, Object>) entries.get(rand.nextInt(entries.size()));
//					}
//
//					// CREATE RANDOM ATTRIBUTE
//					StringBuilder attCreater = new StringBuilder();
//
//					for (int x = 0; x < (rand.nextInt(10) + 1); x++) {
//						attCreater.append(TestUtilities.randomLetter());
//					}
//
//					String attName = attCreater.toString();
//
//					attCreater = new StringBuilder();
//					for (int x = 0; x < (rand.nextInt(10) + 1); x++) {
//						attCreater.append(TestUtilities.randomLetter());
//					}
//
//					String attValue = attCreater.toString();
//
//					// ADD TO TREE THE ATTRIBUTE
//					JsonObject obAtt = new JsonObject();
//					obAtt.addProperty(attName, attValue);
//					assertTrue(ahoCorasick.addAttributeToEntry(graphDb, DICT, String.valueOf(entry.get(ahoCorasick.ENTRY)), obAtt.toString()));
//
//					// ENTRY ANPASSEN
//					attribute = (Map<String, String>) entry.get(ahoCorasick.ATTRIBUTES);
//					attribute.put(attName, attValue);
//
//					break;
//				/********** CHANGE *****************/
//				default:
//					// CHANGE A RANDOM ENTRY IN AN ATTRIBUTE
//					while (entry == null) {
//						entry = (Map<String, Object>) entries.get(rand.nextInt(entries.size()));
//						HashMap<String, String> attr = (HashMap<String, String>) entry.get(ahoCorasick.ATTRIBUTES);
//						if (attr.isEmpty()) {
//							entry = null;
//						}
//					}
//
//					// CREATE RANDOM ATTRIBUTE
//					StringBuilder attNameCreater = new StringBuilder();
//
//					for (int x = 0; x < (rand.nextInt(10) + 1); x++) {
//						attNameCreater.append(TestUtilities.randomLetter());
//					}
//
//					String attNewValue = attNameCreater.toString();
//
//					// GET ATTRIBUTE LIST AND DELETE ONE ENTRY
//					attribute = (Map<String, String>) entry.get(ahoCorasick.ATTRIBUTES);
//					String attNameChange = attribute.keySet().iterator().next();
//
//					// CHANGE TO TREE THE ATTRIBUTE
//					JsonObject obAttChange = new JsonObject();
//					obAttChange.addProperty(attNameChange, attNewValue);
//					ahoCorasick.changeAttributesOfEntry(graphDb, DICT, String.valueOf(entry.get(ahoCorasick.ENTRY)), obAttChange.toString());
//
//					// ENTRY ANPASSEN
//					attribute = (Map<String, String>) entry.get(ahoCorasick.ATTRIBUTES);
//					attribute.remove(attNameChange);
//					attribute.put(attNameChange, attNewValue);
//
//					break;
//				}
//
//				// CHECK WHETHER ATTRIBUTE IS CORRECT
//				ListRepresentation listChange = (ListRepresentation) ahoCorasick.getAllAttributesOfEntry(graphDb, DICT, String.valueOf(entry.get(ahoCorasick.ENTRY)));
//				Iterator<Representation> iter_change = TestUtilities.getIterableFromListRepresentation(listChange).iterator();
//
//				int countChangeMethode = 0;
//				while (iter_change.hasNext()) {
//					ValueRepresentation vr = (ValueRepresentation) iter_change.next();
//					String nameAtt = String.valueOf(test.get(vr));
//
//					assertTrue(attribute.containsKey(nameAtt));
//					countChangeMethode += 1;
//				}
//
//				int countChangeElement = 0;
//				Iterator<String> iterChangeAtt = attribute.keySet().iterator();
//				while (iterChangeAtt.hasNext()) {
//					iterChangeAtt.next();
//					countChangeElement += 1;
//				}
//
//				assertEquals(countChangeElement, countChangeMethode);
//
//				ListRepresentation result = (ListRepresentation) ahoCorasick.completeSearch(graphDb, DICT, String.valueOf(entry.get(ahoCorasick.ENTRY)));
//
//				assertTrue(result != null);
//				List<Representation> list = TestUtilities.getListFromListRepresentation(result);
//
//				for (int x = 0; x < list.size(); x++) {
//					RecursiveMappingRepresentation map = (RecursiveMappingRepresentation) list.get(x);
//					Map<String, Object> mapAnswer = map.getUnderlyingMap();
//					Map match = (Map) mapAnswer.get(ahoCorasick.MATCH);
//					String matchEntry = String.valueOf(match.get(ahoCorasick.ENTRY));
//					if (matchEntry.equals(String.valueOf(entry.get(ahoCorasick.ENTRY)))) {
//
//						Map<String, String> matchAttr = (Map<String, String>) match.get(ahoCorasick.ATTRIBUTES);
//						Iterator<String> iterAttString = attribute.keySet().iterator();
//						while (iterAttString.hasNext()) {
//							String attCompare = iterAttString.next();
//							assertEquals(String.valueOf(attribute.get(attCompare)), String.valueOf(matchAttr.get(attCompare)));
//						}
//						break;
//					}
//				}
//			}
//		}
//	}
//
//	@SuppressWarnings({ "rawtypes" })
//	@Test
//	public void testAddToPreparedDict() throws JSONException, IOException {
//		// CREATE AND PREPARE
//		for (int runs = 0; runs < 1; runs++) {
//			for (int i = 4; i < ahoXmlReader.numbersOfTrees(); i++) {
//				List entries = ahoXmlReader.getListOfEntriesByTree(i);
//				Collections.shuffle(entries);
//
//				/*** CREATE A DICT TREE ***/
//
//				// // Create Root ////
//				String DICT = "Dict" + i;
//				boolean createDict = ahoCorasick.createDictTree(graphDb, DICT);
//
//				// TEST METHODE
//				assertTrue(createDict);
//				assertTrue(!ahoCorasick.createDictTree(graphDb, DICT));
//
//				// TEST FSM STRUCTURE
//				assertTrue(graphDb.findNodesByLabelAndProperty(AhoCorasick.LabelTypes.DICTIONARY, ahoCorasick.DICTIONARY_NAME, DICT).iterator().hasNext());
//
//				// TEST FSM STRUCTURE
//				Node root_dict = graphDb.findNodesByLabelAndProperty(AhoCorasick.LabelTypes.DICTIONARY, ahoCorasick.DICTIONARY_NAME, DICT).iterator().next();
//				Map<String, String> propertyMap = ahoXmlReader.getPropertiesOfTree(i);
//
//				assertTrue(root_dict != null);
//
//				// PREPARE TREE
//				boolean prepareDict = ahoCorasick.prepareDictTreeForSearch(graphDb, DICT);
//				assertTrue(prepareDict);
//				assertTrue(toBoolean(root_dict.getProperty(ahoCorasick.PREPARED)));
//
//				for (int x = 0; x < entries.size(); x++) {
//					List add = Lists.newArrayList(entries.get(x));
//					String json = g.toJson(add);
//					assertTrue(ahoCorasick.addListToPreparedDictTree(graphDb, DICT, json));
//					assertTrue(!ahoCorasick.addListToDictTree(graphDb, DICT, json));
//				}
//
//				// TEST FSM STRUCTURE
//
//				// Anzahl der Fail-Relationen
//				assertEquals(toLong(propertyMap.get(ahoXmlReader.ALLNODES)) - 1, numberOfFailRelation(root_dict));
//				// Wieviele Knoten hat der Baum "Wurzel-Info"
//				assertEquals(toLong(propertyMap.get(ahoXmlReader.ALLNODES)), toLong(root_dict.getProperty(ahoCorasick.NODES_IN_TREE)));
//				// Wieviele Knoten hat der Baum "Per Hand gezählt"
//				assertEquals(toLong(propertyMap.get(ahoXmlReader.ALLNODES)), nodesInTree(root_dict));
//				// Wieviele Incoming Fail-Relationen in Root existieren
//				assertEquals(toLong(propertyMap.get(ahoXmlReader.FAILRELINROOTS)), countIncomingFailRelationsInNode(root_dict));
//				// Wieviele Output Nodes gibt es?
//				assertEquals(toLong(propertyMap.get(ahoXmlReader.FINALNODES)), countOutputNodes(root_dict));
//				// Wieviel Original Nodes gibt es?
//				assertEquals(entries.size(), countNodesWithOriginal(root_dict));
//			}
//
//			// DELETE
//			for (int i = 4; i < ahoXmlReader.numbersOfTrees(); i++) {
//				String DICT = "Dict" + i;
//				// Root still exist
//				assertTrue(graphDb.findNodesByLabelAndProperty(AhoCorasick.LabelTypes.DICTIONARY, ahoCorasick.DICTIONARY_NAME, DICT).iterator().hasNext());
//				// Delete Tree
//				assertTrue(ahoCorasick.deleteDictTree(graphDb, DICT));
//				// Root does not exist anymore
//				assertTrue(!graphDb.findNodesByLabelAndProperty(AhoCorasick.LabelTypes.DICTIONARY, ahoCorasick.DICTIONARY_NAME, DICT).iterator().hasNext());
//			}
//
//			for (int i = 4; i < ahoXmlReader.numbersOfTrees(); i++) {
//				String DICT = "Dict" + i;
//				// It can't delete anything
//				assertTrue(!ahoCorasick.deleteDictTree(graphDb, DICT));
//			}
//			// Is Database empty?
//			assertEquals(1, countAllNodesInDatabase());
//		}
//	}
//
//	@SuppressWarnings({ "rawtypes", "unchecked" })
//	@Test
//	public void testSearch() throws NoSuchFieldException, SecurityException, JSONException, IOException, IllegalArgumentException, IllegalAccessException {
//
//		Field responseContent = ListRepresentation.class.getDeclaredField("content");
//		responseContent.setAccessible(true);
//
//		// Attribute Random add, change and delete
//		for (int i = 4; i < ahoXmlReader.numbersOfTrees(); i++) {
//			List entries = ahoXmlReader.getListOfEntriesByTree(i);
//			String json = g.toJson(entries);
//
//			/*** CREATE A DICT TREE ***/
//
//			// // Create DicTree ////
//			String DICT = "Dict" + i;
//			boolean createDict = ahoCorasick.createDictTree(graphDb, DICT);
//			assertTrue(createDict);
//			assertTrue(ahoCorasick.addListToDictTree(graphDb, DICT, json));
//
//			boolean prepareDict = ahoCorasick.prepareDictTreeForSearch(graphDb, DICT);
//			assertTrue(prepareDict);
//
//			Node root_dict = graphDb.findNodesByLabelAndProperty(AhoCorasick.LabelTypes.DICTIONARY, ahoCorasick.DICTIONARY_NAME, DICT).iterator().next();
//
//			assertTrue(root_dict != null);
//
//			// FULL SEARCH TEST
//			Map<String, Object> mapFull = ahoXmlReader.queryOfTree(i, ahoXmlReader.FULL_MOD);
//			Iterator<String> fullQueryIter = mapFull.keySet().iterator();
//
//			while (fullQueryIter.hasNext()) {
//				String query = fullQueryIter.next();
//				ListRepresentation result = (ListRepresentation) ahoCorasick.completeSearch(graphDb, DICT, query);
//
//				Map<String, Object> mapMatch = (HashMap<String, Object>) mapFull.get(query);
//
//				assertTrue(result != null);
//				List<Representation> list = TestUtilities.getListFromListRepresentation(result);
//
//				for (int x = 0; x < list.size(); x++) {
//					RecursiveMappingRepresentation map = (RecursiveMappingRepresentation) list.get(x);
//
//					Map<String, Object> mapAnswer = map.getUnderlyingMap();
//					Map match = (Map) mapAnswer.get(ahoCorasick.MATCH);
//					String matchEntry = String.valueOf(match.get(ahoCorasick.ENTRY));
//
//					assertTrue(mapMatch.containsKey(matchEntry));
//
//					Map<String, Object> mapBeginEnd = (Map<String, Object>) mapMatch.get(matchEntry);
//
//					assertEquals(toLong(mapBeginEnd.get(ahoCorasick.BEGIN)), toLong(mapAnswer.get(ahoCorasick.BEGIN)));
//					assertEquals(toLong(mapBeginEnd.get(ahoCorasick.END)), toLong(mapAnswer.get(ahoCorasick.END)));
//
//				}
//
//				Iterator<String> iterCompareTree = mapMatch.keySet().iterator();
//
//				int countFounds = 0;
//				while (iterCompareTree.hasNext()) {
//					iterCompareTree.next();
//					countFounds += 1;
//				}
//
//				assertEquals(countFounds, list.size());
//			}
//
//			// FULL SEARCH WITHOUT OVERLAPPING TEST
//			Map<String, Object> mapWithout = ahoXmlReader.queryOfTree(i, ahoXmlReader.WITHOUT_MOD);
//			Iterator<String> withoutQueryIter = mapFull.keySet().iterator();
//
//			while (withoutQueryIter.hasNext()) {
//				String query = withoutQueryIter.next();
//				ListRepresentation result = ahoCorasick.completeSearchWithoutOverlappingResults(graphDb, DICT, query);
//
//				Map<String, Object> mapMatch = (HashMap<String, Object>) mapWithout.get(query);
//
//				assertTrue(result != null);
//				List<Representation> list = TestUtilities.getListFromListRepresentation(result);
//
//				for (int x = 0; x < list.size(); x++) {
//					RecursiveMappingRepresentation map = (RecursiveMappingRepresentation) list.get(x);
//
//					Map<String, Object> mapAnswer = map.getUnderlyingMap();
//					Map match = (Map) mapAnswer.get(ahoCorasick.MATCH);
//					String matchEntry = String.valueOf(match.get(ahoCorasick.ENTRY));
//
//					assertTrue(mapMatch.containsKey(matchEntry));
//
//					Map<String, Object> mapBeginEnd = (Map<String, Object>) mapMatch.get(matchEntry);
//
//					assertEquals(toLong(mapBeginEnd.get(ahoCorasick.BEGIN)), toLong(mapAnswer.get(ahoCorasick.BEGIN)));
//					assertEquals(toLong(mapBeginEnd.get(ahoCorasick.END)), toLong(mapAnswer.get(ahoCorasick.END)));
//
//				}
//
//				Iterator<String> iterCompareTree = mapMatch.keySet().iterator();
//
//				int countFounds = 0;
//				while (iterCompareTree.hasNext()) {
//					iterCompareTree.next();
//					countFounds += 1;
//				}
//
//				assertEquals(countFounds, list.size());
//			}
//
//		}
//	}
//
//	@SuppressWarnings({ "rawtypes" })
//	@Test
//	public void testBigTestSet() throws IOException, JSONException, SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
//		/***** EINLESEN DER DATEN ****/
//		Charset charset = StandardCharsets.UTF_8;
//		File f = new File("src/test/resources/namelist/authornamesTestSet1000");
//
//		LineIterator it;
//		it = FileUtils.lineIterator(f, charset.name());
//
//		// Einträge komplett
//		HashSet<String> setNames = new HashSet<String>();
//
//		while (it.hasNext()) {
//			setNames.add(it.next());
//		}
//
//		int counter = 0;
//		List<Map<String, Object>> entriesComplete = Lists.newArrayList();
//		scala.collection.Iterator<String> itNames = setNames.iterator();
//
//		while (itNames.hasNext()) {
//
//			counter++;
//
//			Map<String, Object> ent = new HashMap<String, Object>();
//			Map<String, String> att = new HashMap<String, String>();
//
//			att.put("attribute" + counter, "attValue" + counter);
//			ent.put(ahoCorasick.ENTRY, itNames.next());
//			ent.put(ahoCorasick.ATTRIBUTES, att);
//
//			entriesComplete.add(ent);
//		}
//
//		// Liste vorbereiten für das Übertragen
//		Gson g = new Gson();
//		String json = g.toJson(entriesComplete);
//
//		// Graphdatenbank vorbereiten
//		final String dictNameTree = "NamenBaumTest";
//		ahoCorasick.createDictTree(graphDb, dictNameTree);
//
//		ahoCorasick.addListToDictTree(graphDb, dictNameTree, json);
//		ahoCorasick.prepareDictTreeForSearch(graphDb, dictNameTree);
//
//		// Testen von Grundeigenschaften
//		Node root_dict = graphDb.findNodesByLabelAndProperty(AhoCorasick.LabelTypes.DICTIONARY, ahoCorasick.DICTIONARY_NAME, dictNameTree).iterator().next();
//
//		assertEquals(995, toLong(root_dict.getProperty(ahoCorasick.NUMBER_OF_ENTRIES)));
//		assertEquals(true, root_dict.getProperty(ahoCorasick.PREPARED));
//		assertEquals(toLong(root_dict.getProperty(ahoCorasick.NODES_IN_TREE)) - 1, numberOfFailRelation(root_dict));
//
//		// Attribute abfragen und testen
//
//		for (int i = 0; i < entriesComplete.size(); i++) {
//			Map<String, Object> entryOb = entriesComplete.get(i);
//			String entry = String.valueOf(entryOb.get(ahoCorasick.ENTRY));
//			ListRepresentation repListAttri = ahoCorasick.getAllAttributesOfEntry(graphDb, dictNameTree, entry);
//			Iterable<Representation> list_entry1Attr = TestUtilities.getIterableFromListRepresentation(repListAttri);
//			Iterator<Representation> iter_entry = list_entry1Attr.iterator();
//
//			Map mapAttriCompare = (Map) ((Map) entriesComplete.get(i)).get(ahoCorasick.ATTRIBUTES);
//			Field test = ValueRepresentation.class.getDeclaredField("value");
//			test.setAccessible(true);
//
//			long numberOfAtt = 0;
//
//			while (iter_entry.hasNext()) {
//				ValueRepresentation vr = (ValueRepresentation) iter_entry.next();
//				String nameAtt = String.valueOf(test.get(vr));
//				assertTrue(mapAttriCompare.containsKey(nameAtt));
//				numberOfAtt++;
//			}
//
//			assertEquals(mapAttriCompare.size(), numberOfAtt);
//		}
//
//		// Finden
//		Random rand = new Random(System.currentTimeMillis());
//		int matchCounter = 0;
//
//		for (int i = 0; i < entriesComplete.size(); i++) {
//			Map<String, Object> entryOb = entriesComplete.get(i);
//			String entry = String.valueOf(entryOb.get(ahoCorasick.ENTRY));
//
//			int begin = rand.nextInt(10);
//			int end = rand.nextInt(10);
//
//			// Suchbegriff erstellen, mit zufälligen Buchstaben davor und danach
//			StringBuffer search = new StringBuffer();
//
//			for (int j = 0; j < begin; j++) {
//				search.append(TestUtilities.randomLetter());
//			}
//
//			search.append(entry);
//
//			for (int j = 0; j < end; j++) {
//				search.append(TestUtilities.randomLetter());
//			}
//
//			// Anfrage
//			ListRepresentation repListSearchResults = ahoCorasick.completeSearch(graphDb, dictNameTree, search.toString());
//			List<Representation> listSearchResults = TestUtilities.getListFromListRepresentation(repListSearchResults);
//
//			matchCounter = 0;
//
//			for (int j = 0; j < listSearchResults.size(); j++) {
//				RecursiveMappingRepresentation maprep = (RecursiveMappingRepresentation) listSearchResults.get(j);
//				Map<String, Object> test = maprep.getUnderlyingMap();
//				Map match = (Map) test.get(ahoCorasick.MATCH);
//				if (match.get(ahoCorasick.ENTRY).equals(entryOb.get(ahoCorasick.ENTRY))) {
//					matchCounter += 1;
//					if (toLong(test.get(ahoCorasick.BEGIN)) == begin) {
//						matchCounter += 1;
//					}
//				}
//			}
//			assertEquals(2, matchCounter);
//		}
//	}
//
//	@SuppressWarnings({ "rawtypes", "unchecked" })
//	@Test
//	public void testDeleteEntry() throws JSONException, IOException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
//
//		for (int runs = 0; runs < 1; runs++) {
//			for (int i = 4; i < ahoXmlReader.numbersOfTrees(); i++) {
//				List entries = ahoXmlReader.getListOfEntriesByTree(i);
//				Collections.shuffle(entries);
//				String jsonOriginal = g.toJson(entries.subList(0, entries.size() - 1));
//				String jsonFull = g.toJson(entries);
//
//				/*** CREATE A DICT TREE ***/
//
//				// // Create Root ////
//				String DICT_Original = "Dict" + i;
//				String DICT_Full = "Dict" + i + "Full";
//
//				assertTrue(ahoCorasick.createDictTree(graphDb, DICT_Original));
//				assertTrue(ahoCorasick.createDictTree(graphDb, DICT_Full));
//
//				// TEST METHODE
//				assertTrue(!ahoCorasick.createDictTree(graphDb, DICT_Original));
//				assertTrue(!ahoCorasick.createDictTree(graphDb, DICT_Full));
//
//				// // Add Entries ////
//				assertTrue(ahoCorasick.addListToDictTree(graphDb, DICT_Original, jsonOriginal));
//				assertTrue(ahoCorasick.addListToDictTree(graphDb, DICT_Full, jsonFull));
//
//				assertTrue(ahoCorasick.prepareDictTreeForSearch(graphDb, DICT_Full));
//				assertTrue(ahoCorasick.prepareDictTreeForSearch(graphDb, DICT_Original));
//
//				// // Delete one Entry ////
//				ahoCorasick.deleteEntryInDictTree(graphDb, DICT_Full, (String) ((Map<String, Object>) entries.get(entries.size() - 1)).get(ahoCorasick.ENTRY));
//
//				// TEST FSM STRUCTURE
//				Node root_dict_original = graphDb.findNodesByLabelAndProperty(AhoCorasick.LabelTypes.DICTIONARY, ahoCorasick.DICTIONARY_NAME, DICT_Original).iterator().next();
//				Node root_dict_full = graphDb.findNodesByLabelAndProperty(AhoCorasick.LabelTypes.DICTIONARY, ahoCorasick.DICTIONARY_NAME, DICT_Full).iterator().next();
//
//				assertTrue(root_dict_original != null);
//				assertTrue(root_dict_full != null);
//
//				// Same number of nodes in tree
//				assertEquals(nodesInTree(root_dict_original), nodesInTree(root_dict_full));
//				assertEquals(entriesInTree(root_dict_original), entriesInTree(root_dict_full));
//				assertEquals(countIncomingFailRelationsInNode(root_dict_original), countIncomingFailRelationsInNode(root_dict_full));
//				assertEquals(countOutputNodes(root_dict_original), countOutputNodes(root_dict_full));
//				assertEquals(numberOfFailRelation(root_dict_original), numberOfFailRelation(root_dict_full));
//			}
//			TestUtilities.deleteEverythingInDB(graphDb);
//		}
//	}
//
//	@SuppressWarnings({ "rawtypes", "unchecked" })
//	@Test
//	public void testDeleteAndAddEntry() throws JSONException, IOException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
//
//		for (int runs = 0; runs < 1; runs++) {
//			for (int i = 4; i < ahoXmlReader.numbersOfTrees(); i++) {
//				List entries = ahoXmlReader.getListOfEntriesByTree(i);
//				Collections.shuffle(entries);
//				String jsonOriginal = g.toJson(entries.subList(0, entries.size() - 1));
//				String jsonFull = g.toJson(entries);
//
//				/*** CREATE A DICT TREE ***/
//
//				// // Create Root ////
//				String DICT_Original = "Dict" + i;
//				String DICT_Full = "Dict" + i + "Full";
//
//				assertTrue(ahoCorasick.createDictTree(graphDb, DICT_Original));
//				assertTrue(ahoCorasick.createDictTree(graphDb, DICT_Full));
//
//				// TEST METHODE
//				assertTrue(!ahoCorasick.createDictTree(graphDb, DICT_Original));
//				assertTrue(!ahoCorasick.createDictTree(graphDb, DICT_Full));
//
//				// // Add Entries ////
//				assertTrue(ahoCorasick.addListToDictTree(graphDb, DICT_Original, jsonOriginal));
//				assertTrue(ahoCorasick.addListToDictTree(graphDb, DICT_Full, jsonFull));
//
//				assertTrue(ahoCorasick.prepareDictTreeForSearch(graphDb, DICT_Full));
//				assertTrue(ahoCorasick.prepareDictTreeForSearch(graphDb, DICT_Original));
//
//				// // Delete last three Entries ////
//				ahoCorasick.deleteEntryInDictTree(graphDb, DICT_Full, (String) ((Map<String, Object>) entries.get(entries.size() - 1)).get(ahoCorasick.ENTRY));
//				ahoCorasick.deleteEntryInDictTree(graphDb, DICT_Full, (String) ((Map<String, Object>) entries.get(entries.size() - 2)).get(ahoCorasick.ENTRY));
//				ahoCorasick.deleteEntryInDictTree(graphDb, DICT_Full, (String) ((Map<String, Object>) entries.get(entries.size() - 3)).get(ahoCorasick.ENTRY));
//
//				// // ADD two Entries
//				String json = g.toJson(entries.subList(entries.size() - 3, entries.size() - 1));
//				assertTrue(ahoCorasick.addListToPreparedDictTree(graphDb, DICT_Full, json));
//
//				// TEST FSM STRUCTURE
//				Node root_dict_original = graphDb.findNodesByLabelAndProperty(AhoCorasick.LabelTypes.DICTIONARY, ahoCorasick.DICTIONARY_NAME, DICT_Original).iterator().next();
//				Node root_dict_full = graphDb.findNodesByLabelAndProperty(AhoCorasick.LabelTypes.DICTIONARY, ahoCorasick.DICTIONARY_NAME, DICT_Full).iterator().next();
//
//				assertTrue(root_dict_original != null);
//				assertTrue(root_dict_full != null);
//
//				// Same number of nodes in tree
//				assertEquals(nodesInTree(root_dict_original), nodesInTree(root_dict_full));
//				assertEquals(entriesInTree(root_dict_original), entriesInTree(root_dict_full));
//				assertEquals(countIncomingFailRelationsInNode(root_dict_original), countIncomingFailRelationsInNode(root_dict_full));
//				assertEquals(countOutputNodes(root_dict_original), countOutputNodes(root_dict_full));
//				assertEquals(numberOfFailRelation(root_dict_original), numberOfFailRelation(root_dict_full));
//			}
//			TestUtilities.deleteEverythingInDB(graphDb);
//		}
//	}
//
//	@AfterClass
//	public static void shutdown() {
//		graphDb.shutdown();
//	}
//
//	/********* HILFSFUNKTIONEN ************/
//
//	// ///////////////////////////////////
//	// ///////////////// CONVERT FUNCTIONS
//
//	private long toLong(Object ob) {
//		return Long.valueOf(String.valueOf(ob));
//	}
//
//	private boolean toBoolean(Object ob) {
//		return Boolean.valueOf(String.valueOf(ob));
//	}
//
//	// //////////////////////////////////
//	// ///////////////// COUNT FUNCTIONS
//
//	/**
//	 * Count all states in the DictTree
//	 * 
//	 * @param root
//	 * @return
//	 */
//	private long nodesInTree(Node root) {
//
//		long count = 0;
//
//		ExecutionEngine engine = new ExecutionEngine(graphDb);
//		ExecutionResult result = engine.execute("start n=node(" + root.getId() + ") MATCH (n)-[*]->(m) Return count(DISTINCT m) ");
//		ResourceIterator<Long> iter = result.columnAs("count(DISTINCT m)");
//		while (iter.hasNext()) {
//			count = count + iter.next();
//		}
//
//		return count;
//	}
//
//	/**
//	 * Count all words in a Tree
//	 * 
//	 * @param root
//	 *            - Root of the DictTree
//	 * @return number of Entries in the Dict Tree
//	 */
//	private long entriesInTree(Node root) {
//
//		long count = 0;
//
//		ExecutionEngine engine = new ExecutionEngine(graphDb);
//		ExecutionResult result = engine.execute("START n = node(" + root.getId() + ") MATCH (n)-[*]->(m) WHERE HAS(m.property_original) RETURN count(DISTINCT m)");
//		ResourceIterator<Long> iter = result.columnAs("count(DISTINCT m)");
//		while (iter.hasNext()) {
//			count = count + iter.next();
//		}
//
//		return count;
//
//	}
//
//	private long countIncomingFailRelationsInNode(Node root) {
//		long count = 0;
//
//		ExecutionEngine engine = new ExecutionEngine(graphDb);
//		ExecutionResult result = engine.execute("START n = node(" + root.getId() + ") MATCH (n)<-[:FAIL]-(m) RETURN count(DISTINCT m)");
//		ResourceIterator<Long> iter = result.columnAs("count(DISTINCT m)");
//		while (iter.hasNext()) {
//			count = count + iter.next();
//		}
//
//		return count;
//	}
//
//	private long countNodesWithOriginal(Node root) {
//
//		long count = 0;
//
//		ExecutionEngine engine = new ExecutionEngine(graphDb);
//		ExecutionResult result = engine.execute("START n = node(" + root.getId() + ") MATCH (n)-[*]->(m)	WHERE HAS(m.property_original) RETURN count(DISTINCT m)");
//		ResourceIterator<Long> iter = result.columnAs("count(DISTINCT m)");
//		while (iter.hasNext()) {
//			count = count + iter.next();
//		}
//
//		return count;
//	}
//
//	private long countAllNodesInDatabase() {
//
//		long count = 0;
//
//		ExecutionEngine engine = new ExecutionEngine(graphDb);
//		ExecutionResult result = engine.execute("START n = node(*) RETURN count(n)");
//		ResourceIterator<Long> iter = result.columnAs("count(n)");
//		while (iter.hasNext()) {
//			count = count + iter.next();
//		}
//
//		return count;
//	}
//
//	/**
//	 * 
//	 * @param root
//	 * @return
//	 */
//	private long countOutputNodes(Node root) {
//		long count = 0;
//
//		ExecutionEngine engine = new ExecutionEngine(graphDb);
//		ExecutionResult result = engine.execute("START n = node(" + root.getId() + ") MATCH n-[*]->(m) WHERE m.property_number_Output>0 RETURN count(DISTINCT m)");
//		ResourceIterator<Long> iter = result.columnAs("count(DISTINCT m)");
//		while (iter.hasNext()) {
//			count = count + iter.next();
//		}
//
//		return count;
//	}
//
//	/**
//	 * Number of Fail Relation
//	 * 
//	 * @param root
//	 * @return
//	 */
//	private long numberOfFailRelation(Node root) {
//
//		ArrayDeque<Node> list = new ArrayDeque<Node>();
//		list.add(root);
//
//		int count = 0;
//
//		while (!list.isEmpty()) {
//
//			// Besorgen dir eine Knoten aus der Liste und suche nach dem Knoten
//			// im anderen Baum
//			Node node = list.pop();
//
//			Iterator<Relationship> iterRel = node.getRelationships(Direction.OUTGOING).iterator();
//			while (iterRel.hasNext()) {
//				Relationship rel = iterRel.next();
//				RelationshipType typ = rel.getType();
//
//				if (typ.name().equals(AhoCorasick.EdgeTypes.FAIL.name())) {
//					count++;
//				} else {
//					// Fuege den Knoten zur Menge, wenn er nicht Ã¼ber eine
//					// Fail-Relation mit
//					// dem Knoten verknÃ¼pft ist
//					// Ohne diese Abfrage, wÃ¼rde es zu einer Schleife kommen !
//					list.add(rel.getEndNode());
//				}
//			}
//		}
//		return count;
//	}

}
