package de.julielab.neo4j.plugins.ahocorasick;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

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
import org.neo4j.graphdb.RelationshipType;
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
import de.julielab.neo4j.plugins.ahocorasick.property.ACEntry;
import de.julielab.neo4j.plugins.test.TestUtilities;

// works in Eclipse but not with "mvn clean test" on the command line...?!
// Dauert lange (eine gute Minute auf meinem MacBook Air), müsste aber funktionieren (regelmäßig testen!)
@Ignore
public class ACRefactoryTest {

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
	public void testAttributMethode() throws Exception {

		GraphDatabaseService graphDb = db.startGraphDatabase();
		int start = 0;

		final int DELETE = 1;
		final int ADD = 2;
		final int CHANGE = 3;

		Random rand = new Random(System.currentTimeMillis());

		// Attribute Random add, change and delete
		for (int i = start; i < ahoXmlReader.numbersOfTrees(); i++) {
			for (int modeCreate = 2; modeCreate >= 0; modeCreate--) {
				for (int modeSearch = 0; modeSearch >= 0 && modeSearch <= modeCreate && modeSearch < 2; modeSearch++) {
					System.out.println("Dict :" + i + " Mode: " + modeCreate + " " + modeSearch);

					List entries = ahoXmlReader.getListOfEntriesByTree(i, false);
					String json = g.toJson(entries);

					/*** CREATE A DICT TREE ***/

					// // Create DicTree ////
					String DICT = "Dict" + i + modeCreate + modeSearch + "Normal";
					ACDictionary dict = new ACDictionary(DICT, modeCreate, modeSearch);
					boolean createDict = ACFactoryEmbedded.createDictTree(graphDb, dict.toJSONString());
					assertTrue(createDict);
					assertTrue(ACFactoryEmbedded.addListToDictTree(graphDb, dict.toJSONString(), json));

					boolean prepareDict = ACFactoryEmbedded.prepareDictTreeForSearch(graphDb, g.toJson(dict));
					assertTrue(prepareDict);

					Node root_dict = null;
					try (Transaction tx = graphDb.beginTx()) {
						root_dict = graphDb
								.findNodesByLabelAndProperty(ACProperties.LabelTypes.DICTIONARY,
										ACProperties.DICTIONARY_NAME, DICT).iterator().next();

						tx.success();
					}

					assertTrue(root_dict != null);

					// Change randomly Attributes
					for (int j = 0; j < 15; j++) {
						double random = rand.nextDouble();
						int modus = 0;

						if (random < 0.1) {
							modus = DELETE;
						} else if (random < 0.7) {
							modus = ADD;
						} else {
							modus = CHANGE;
						}

						ACEntry entry = null;
						Map<String, Object> attribute;

						switch (modus) {
						case DELETE:
							// GET A RANDOM ENTRY TO CHANGE A ATTRIBUTE
							while (entry == null) {
								entry = (ACEntry) entries.get(rand.nextInt(entries.size()));
								Map<String, Object> attr = entry.getAllAttributes();
								if (attr.isEmpty()) {
									entry = null;
								}
							}

							// GET ATTRIBUTE LIST AND DELETE ONE ENTRY
							attribute = entry.getAllAttributes();
							List deleteList = Lists.newArrayList(attribute.keySet().iterator().next());
							entry.deleteAttribute((String) deleteList.get(0));

							// TEST METHODE
							assertTrue(ACRefactory.editEntry(graphDb, g.toJson(dict), g.toJson(entry)));
							attribute.remove(deleteList.get(0));

							break;
						/********** ADD *****************/
						case ADD:
							// ADD TO A RANDOM ENTRY AN ATTRIBUTE
							while (entry == null) {
								entry = (ACEntry) entries.get(rand.nextInt(entries.size()));
							}

							// CREATE RANDOM ATTRIBUTE
							StringBuilder attCreater = new StringBuilder();

							for (int x = 0; x < (rand.nextInt(10) + 1); x++) {
								attCreater.append(TestUtilities.randomLetter());
							}

							String attName = attCreater.toString();

							attCreater = new StringBuilder();
							for (int x = 0; x < (rand.nextInt(10) + 1); x++) {
								attCreater.append(TestUtilities.randomLetter());
							}

							String attValue = attCreater.toString();

							// ADD TO TREE THE ATTRIBUTE
							entry.addAttribute(attName, attValue);
							assertTrue(ACRefactory.editEntry(graphDb, g.toJson(dict), g.toJson(entry)));

							// ENTRY ANPASSEN
							attribute = entry.getAllAttributes();
							attribute.put(attName, attValue);

							break;
						/********** CHANGE *****************/
						default:
							// CHANGE A RANDOM ENTRY IN AN ATTRIBUTE
							while (entry == null) {
								entry = (ACEntry) entries.get(rand.nextInt(entries.size()));
								Map<String, Object> attr = entry.getAllAttributes();
								if (attr.isEmpty()) {
									entry = null;
								}
							}

							// CREATE RANDOM ATTRIBUTE
							StringBuilder attNameCreater = new StringBuilder();

							for (int x = 0; x < (rand.nextInt(10) + 1); x++) {
								attNameCreater.append(TestUtilities.randomLetter());
							}

							String attNewValue = attNameCreater.toString();

							// GET ATTRIBUTE LIST AND DELETE ONE ENTRY
							String attNameChange = entry.getAllAttributes().keySet().iterator().next();
							entry.addAttribute(attNameChange, attNewValue);

							// CHANGE TO TREE THE ATTRIBUTE
							assertTrue(ACRefactory.editEntry(graphDb, g.toJson(dict), g.toJson(entry)));

							// ENTRY ANPASSEN
							attribute = entry.getAllAttributes();
							attribute.remove(attNameChange);
							attribute.put(attNameChange, attNewValue);

							break;
						}

						// CHECK WHETHER ATTRIBUTE IS CORRECT
						ACEntry entryChange = new ACEntry(new JSONObject(ACSearch.getCompleteEntry(graphDb,
								g.toJson(dict), entry.entryString())));
						Map<String, Object> attMap = entryChange.getAllAttributes();
						Iterator<String> iter = attMap.keySet().iterator();
						while (iter.hasNext()) {
							String key = iter.next();
							assertTrue(attribute.containsKey(key));
							assertTrue(attribute.get(key).equals(attMap.get(key)));
						}
						assertEquals(attribute.size(), attMap.size());
					}

				}
			}
		}
		db.stopGraphDatabase();
	}

	@SuppressWarnings({ "rawtypes" })
	@Test
	public void testDeleteEntry() throws JSONException, IOException, NoSuchFieldException, SecurityException,
			IllegalArgumentException, IllegalAccessException {

		GraphDatabaseService graphDb = db.startGraphDatabase();
		int start = 0;

		for (int runs = 0; runs < 1; runs++) {
			for (int i = start; i < ahoXmlReader.numbersOfTrees(); i++) {
				List entries = ahoXmlReader.getListOfEntriesByTree(i, false);
				Collections.shuffle(entries);
				String jsonOriginal = g.toJson(entries.subList(0, entries.size() - 1));
				String jsonFull = g.toJson(entries);

				/*** CREATE A DICT TREE ***/

				for (int modeCreate = 2; modeCreate >= 0; modeCreate--) {
					for (int modeSearch = 0; modeSearch >= 0 && modeSearch <= modeCreate && modeSearch < 2; modeSearch++) {
						System.out.println("Dict :" + i + " Mode: " + modeCreate + " " + modeSearch);

						// // Create Root ////
						String DICT_Original = "DictDeleeee" + i + modeCreate + modeSearch;
						String DICT_Full = "DictDeleeee" + i + "Full" + modeCreate + modeSearch;

						ACDictionary dictOrg = new ACDictionary(DICT_Original, modeCreate, modeSearch);
						ACDictionary dictFull = new ACDictionary(DICT_Full, modeCreate, modeSearch);

						assertTrue(ACFactoryEmbedded.createDictTree(graphDb, g.toJson(dictOrg)));
						assertTrue(ACFactoryEmbedded.createDictTree(graphDb, g.toJson(dictFull)));

						// TEST METHODE
						assertTrue(!ACFactoryEmbedded.createDictTree(graphDb, g.toJson(dictOrg)));
						assertTrue(!ACFactoryEmbedded.createDictTree(graphDb, g.toJson(dictFull)));

						// // Add Entries ////
						assertTrue(ACFactoryEmbedded.addListToDictTree(graphDb, g.toJson(dictOrg), jsonOriginal));
						assertTrue(ACFactoryEmbedded.addListToDictTree(graphDb, g.toJson(dictFull), jsonFull));

						assertTrue(ACFactoryEmbedded.prepareDictTreeForSearch(graphDb, g.toJson(dictFull)));
						assertTrue(ACFactoryEmbedded.prepareDictTreeForSearch(graphDb, g.toJson(dictOrg)));

						// // Delete one Entry ////
						ACRefactory.deleteEntryInDictTree(graphDb, g.toJson(dictFull),
								((ACEntry) entries.get(entries.size() - 1)).entryString());

						// TEST FSM STRUCTURE
						try (Transaction tx = graphDb.beginTx()) {
							Node root_dict_original = graphDb
									.findNodesByLabelAndProperty(ACProperties.LabelTypes.DICTIONARY,
											ACProperties.DICTIONARY_NAME, DICT_Original).iterator().next();
							Node root_dict_full = graphDb
									.findNodesByLabelAndProperty(ACProperties.LabelTypes.DICTIONARY,
											ACProperties.DICTIONARY_NAME, DICT_Full).iterator().next();

							assertTrue(root_dict_original != null);
							assertTrue(root_dict_full != null);

							// Same number of nodes in tree
							assertEquals(nodesInTree(graphDb, root_dict_original), nodesInTree(graphDb, root_dict_full));
							assertEquals(entriesInTree(graphDb, root_dict_original),
									entriesInTree(graphDb, root_dict_full));
							assertEquals(countIncomingFailRelationsInNode(graphDb, root_dict_original),
									countIncomingFailRelationsInNode(graphDb, root_dict_full));
							assertEquals(countOutputNodes(graphDb, root_dict_original),
									countOutputNodes(graphDb, root_dict_full));
							assertEquals(numberOfFailRelation(root_dict_original), numberOfFailRelation(root_dict_full));

							if (dictFull.isLocalSearch()) {
								assertTrue(areRelationsSame(graphDb, root_dict_full));
							}

							tx.success();
						}
					}
				}
			}
			TestUtilities.deleteEverythingInDB(graphDb);
		}

		db.stopGraphDatabase();
	}

	@SuppressWarnings({ "rawtypes" })
	@Test
	public void testDeleteAndAddEntry() throws JSONException, IOException, NoSuchFieldException, SecurityException,
			IllegalArgumentException, IllegalAccessException {

		GraphDatabaseService graphDb = db.startGraphDatabase();
		int start = 0;

		for (int runs = 0; runs < 1; runs++) {
			for (int i = start; i < ahoXmlReader.numbersOfTrees(); i++) {
				List entries = ahoXmlReader.getListOfEntriesByTree(i, false);
				Collections.shuffle(entries);
				String jsonOriginal = g.toJson(entries.subList(0, entries.size() - 1));
				String jsonFull = g.toJson(entries);

				/*** CREATE A DICT TREE ***/
				for (int modeCreate = 2; modeCreate >= 0; modeCreate--) {
					for (int modeSearch = 0; modeSearch >= 0 && modeSearch <= modeCreate && modeSearch < 2; modeSearch++) {
						System.out.println("Dict :" + i + " Mode: " + modeCreate + " " + modeSearch);

						// // Create Root ////
						String DICT_Original = "DictDel" + i + modeCreate + modeSearch;
						String DICT_Full = "DictDel" + i + "Full" + modeCreate + modeSearch;

						ACDictionary dictOrg = new ACDictionary(DICT_Original, modeCreate, modeSearch);
						ACDictionary dictFull = new ACDictionary(DICT_Full, modeCreate, modeSearch);

						assertTrue(ACFactoryEmbedded.createDictTree(graphDb, g.toJson(dictOrg)));
						assertTrue(ACFactoryEmbedded.createDictTree(graphDb, g.toJson(dictFull)));

						// TEST METHODE
						assertTrue(!ACFactoryEmbedded.createDictTree(graphDb, g.toJson(dictOrg)));
						assertTrue(!ACFactoryEmbedded.createDictTree(graphDb, g.toJson(dictFull)));

						// // Add Entries ////
						assertTrue(ACFactoryEmbedded.addListToDictTree(graphDb, g.toJson(dictOrg), jsonOriginal));
						assertTrue(ACFactoryEmbedded.addListToDictTree(graphDb, g.toJson(dictFull), jsonFull));

						assertTrue(ACFactoryEmbedded.prepareDictTreeForSearch(graphDb, g.toJson(dictFull)));
						assertTrue(ACFactoryEmbedded.prepareDictTreeForSearch(graphDb, g.toJson(dictOrg)));

						// // Delete last three Entries ////
						ACRefactory.deleteEntryInDictTree(graphDb, g.toJson(dictFull),
								((ACEntry) entries.get(entries.size() - 1)).entryString());
						ACRefactory.deleteEntryInDictTree(graphDb, g.toJson(dictFull),
								((ACEntry) entries.get(entries.size() - 2)).entryString());
						ACRefactory.deleteEntryInDictTree(graphDb, g.toJson(dictFull),
								((ACEntry) entries.get(entries.size() - 3)).entryString());

						// TEST CURRENT REL MAP

						try (Transaction tx = graphDb.beginTx()) {
							Node root_dict_full = graphDb
									.findNodesByLabelAndProperty(ACProperties.LabelTypes.DICTIONARY,
											ACProperties.DICTIONARY_NAME, DICT_Full).iterator().next();

							if (dictFull.isLocalSearch()) {
								assertTrue(areRelationsSame(graphDb, root_dict_full));
							}

							tx.success();
						}

						// // ADD two Entries
						String json = g.toJson(entries.subList(entries.size() - 3, entries.size() - 1));
						assertTrue(ACFactoryEmbedded.addListToPreparedDictTree(graphDb, g.toJson(dictFull), json));

						try (Transaction tx = graphDb.beginTx()) {
							// TEST FSM STRUCTURE
							Node root_dict_original = graphDb
									.findNodesByLabelAndProperty(ACProperties.LabelTypes.DICTIONARY,
											ACProperties.DICTIONARY_NAME, DICT_Original).iterator().next();
							Node root_dict_full = graphDb
									.findNodesByLabelAndProperty(ACProperties.LabelTypes.DICTIONARY,
											ACProperties.DICTIONARY_NAME, DICT_Full).iterator().next();

							assertTrue(root_dict_original != null);
							assertTrue(root_dict_full != null);

							// Same number of nodes in tree
							assertEquals(nodesInTree(graphDb, root_dict_original), nodesInTree(graphDb, root_dict_full));
							assertEquals(entriesInTree(graphDb, root_dict_original),
									entriesInTree(graphDb, root_dict_full));
							assertEquals(countIncomingFailRelationsInNode(graphDb, root_dict_original),
									countIncomingFailRelationsInNode(graphDb, root_dict_full));
							assertEquals(countOutputNodes(graphDb, root_dict_original),
									countOutputNodes(graphDb, root_dict_full));
							assertEquals(numberOfFailRelation(root_dict_original), numberOfFailRelation(root_dict_full));

							tx.success();
						}
					}
				}
			}
			TestUtilities.deleteEverythingInDB(graphDb);
		}

		db.stopGraphDatabase();
	}

	@AfterClass
	public static void shutdown() {
		db.stopBatchInserter();
		db.stopGraphDatabase();
	}

	// ///////////////////////////////////////////
	// ////// HELP FUNCTIONS

	private long countIncomingFailRelationsInNode(GraphDatabaseService graphDb, Node root) {
		long count = 0;

		Iterator<Relationship> iter = root.getRelationships(Direction.INCOMING, ACProperties.EdgeTypes.FAIL).iterator();

		while (iter.hasNext()) {
			iter.next();
			count = count++;
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
		ExecutionResult result = engine.execute("START n = node(" + root.getId()
				+ ") MATCH n-[*]->(m) WHERE m.property_number_Output>0 RETURN count(DISTINCT m)");
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

		ArrayDeque<Node> list = new ArrayDeque<Node>();
		list.add(root);

		int count = 0;

		while (!list.isEmpty()) {

			// Besorgen dir eine Knoten aus der Liste und suche nach dem Knoten
			// im anderen Baum
			Node node = list.pop();

			Iterator<Relationship> iterRel = node.getRelationships(Direction.OUTGOING).iterator();
			while (iterRel.hasNext()) {
				Relationship rel = iterRel.next();
				RelationshipType typ = rel.getType();

				if (typ.name().equals(ACProperties.getFailName())) {
					count++;
				} else {
					// Fuege den Knoten zur Menge, wenn er nicht Ã¼ber eine
					// Fail-Relation mit
					// dem Knoten verknÃ¼pft ist
					// Ohne diese Abfrage, wÃ¼rde es zu einer Schleife kommen !
					list.add(rel.getEndNode());
				}
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
		ExecutionResult result = engine.execute("start n=node(" + root.getId()
				+ ") MATCH (n)-[*]->(m) Return count(DISTINCT m) ");
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
		ExecutionResult result = engine.execute("START n = node(" + root.getId()
				+ ") MATCH (n)-[*]->(m) WHERE HAS(m.property_original) RETURN count(DISTINCT m)");
		ResourceIterator<Long> iter = result.columnAs("count(DISTINCT m)");
		while (iter.hasNext()) {
			count = count + iter.next();
		}

		return count;

	}

	private boolean areRelationsSame(GraphDatabaseService graphDb, Node root) throws JSONException {

		Deque<Node> deq = new ArrayDeque<>();
		deq.add(root);

		while (!deq.isEmpty()) {

			Node currentNode = deq.pop();

			Iterator<Relationship> iter = currentNode.getRelationships(Direction.OUTGOING).iterator();
			JSONObject obProp = new JSONObject(String.valueOf(currentNode.getProperty(ACProperties.RELATIONSHIP)));

			while (iter.hasNext()) {
				Relationship rel = iter.next();
				String nameRel = rel.getType().name();

				// IS FAIL RELATION
				if (nameRel.equals(ACProperties.getFailName())) {
					if (currentNode.getId() != root.getId() && !obProp.has(ACProperties.EdgeTypes.FAIL.name())) {
						return false;
					}
					// IS NO FAIL RELATION
				} else {
					deq.add(rel.getEndNode());
					if (!obProp.has((String) rel.getProperty(ACProperties.LETTER))) {
						return false;
					}
					if (ACUtil.toLong(obProp.get((String) rel.getProperty(ACProperties.LETTER))) != rel.getEndNode()
							.getId()) {
						return false;
					}
				}
			}
		}
		return true;
	}

}
