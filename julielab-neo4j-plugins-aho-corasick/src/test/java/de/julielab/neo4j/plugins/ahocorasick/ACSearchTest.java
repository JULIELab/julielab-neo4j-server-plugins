package de.julielab.neo4j.plugins.ahocorasick;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.shell.util.json.JSONException;
import org.neo4j.shell.util.json.JSONObject;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.xml.sax.SAXException;

import com.google.gson.Gson;

import de.julielab.neo4j.plugins.AhoCorasickXmlTreeReader;
import de.julielab.neo4j.plugins.ahocorasick.property.ACDataBase;
import de.julielab.neo4j.plugins.ahocorasick.property.ACDictionary;
import de.julielab.neo4j.plugins.ahocorasick.property.ACMatch;
import de.julielab.neo4j.plugins.ahocorasick.property.ACQuery;
import de.julielab.neo4j.plugins.test.TestUtilities;

/**
 * Takes a long time (nearly 10 minutes on my MacBook Air) but runs. If not, someone did something wrong.
 * @author ohm
 *
 */
// works in Eclipse but not with "mvn clean test" on the command line...?!
@Ignore
public class ACSearchTest {

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
	
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testSearchBatch() throws NoSuchFieldException, SecurityException, JSONException, 
	IOException, IllegalArgumentException, IllegalAccessException {

		Field responseContent = ListRepresentation.class.getDeclaredField("content");
		responseContent.setAccessible(true);
		
		// Attribute Random add, change and delete
		for (int i = 0; i < ahoXmlReader.numbersOfTrees(); i++) {
			for(int modeCreate = 2; modeCreate >= 0; modeCreate--){
				for(int modeSearch = 0; modeSearch >= 0 && modeSearch <=modeCreate && modeSearch<2; modeSearch++){
					System.out.println("Dict :"+i+" Mode: "+modeCreate + " "+ modeSearch);
					
					BatchInserter batchInserter = db.startBatchInserter();
					List entries = ahoXmlReader.getListOfEntriesByTree(i, false);
					
					/*** CREATE A DICT TREE ***/
					
					// // Create DicTree ////
					String DICT = "Dict" + i + modeCreate + modeSearch;
					ACDictionary dict = new ACDictionary(DICT, modeCreate, modeSearch);
					boolean createDict = ACFactoryBatch.createDictTree(batchInserter, dict);
					assertTrue(createDict);
					assertTrue(ACFactoryBatch.addListToDictTree(db, dict, entries));
					
					boolean prepareDict = ACFactoryBatch.prepareDictTreeForSearch(db, dict);
					assertTrue(prepareDict);
					
					db.stopBatchInserter();
					
					// FULL SEARCH TEST - COMPLETE
					Map<String, Object> mapFull = ahoXmlReader.queryOfTree(i, ahoXmlReader.FULL_MOD);
					Iterator<String> fullQueryIter = mapFull.keySet().iterator();
					
					GraphDatabaseService graphDb = db.startGraphDatabase();
					
					while (fullQueryIter.hasNext()) {
						String queryString = fullQueryIter.next();
						
						ACQuery query = new ACQuery(queryString, ACSearch.FULL_WITH_OVERLAPPING);
						String test = ACSearch.search(graphDb, dict.toJSONString(), query.toJSONString());
						query = new ACQuery(new JSONObject(test));

						assertTrue(query.getAllMatches().size()>0);
						Map<String, Object> mapMatch = (HashMap<String, Object>) mapFull.get(queryString);
						List<ACMatch> list = query.getAllMatches();
						
						for (int x = 0; x < list.size(); x++) {
							ACMatch map = list.get(x);
							
							assertTrue(mapMatch.containsKey(map.getEntry().entryString()));
							Map<String, Object> mapBeginEnd = (Map<String, Object>) mapMatch.get(map.getEntry().entryString());
							
							assertEquals(map.getBegin(), 
									ACUtil.toLong(mapBeginEnd.get(ACSearch.BEGIN)));
							assertEquals(map.getEnd(), 
									ACUtil.toLong(mapBeginEnd.get(ACSearch.END)));
							
						}
						
						Iterator<String> iterCompareTree = mapMatch.keySet().iterator();
						
						int countFounds = 0;
						while (iterCompareTree.hasNext()) {
							iterCompareTree.next();
							countFounds += 1;
						}
						
						assertEquals(countFounds, list.size());
					}
					
					// FULL SEARCH WITHOUT OVERLAPPING TEST
					Map<String, Object> mapWithout = ahoXmlReader.queryOfTree(i, ahoXmlReader.WITHOUT_MOD);
					Iterator<String> withoutQueryIter = mapFull.keySet().iterator();
					
					while (withoutQueryIter.hasNext()) {
						String queryString = withoutQueryIter.next();
						
						ACQuery query = new ACQuery(queryString, ACSearch.FULL_WITHOUT_OVERLAPPING);
						String test = ACSearch.search(graphDb, dict.toJSONString(), query.toJSONString());
						query = new ACQuery(new JSONObject(test));
						
						Map<String, Object> mapMatch = (HashMap<String, Object>) mapWithout.get(queryString);
						
						assertTrue(query.getAllMatches().size()>0);
						List<ACMatch> list = query.getAllMatches();
						
						for (int x = 0; x < list.size(); x++) {
							ACMatch map = list.get(x);
							
							assertTrue(mapMatch.containsKey(map.getEntry().entryString()));
							Map<String, Object> mapBeginEnd = (Map<String, Object>) mapMatch.get(map.getEntry().entryString());
							
							assertEquals(map.getBegin(), 
									ACUtil.toLong(mapBeginEnd.get(ACSearch.BEGIN)));
							assertEquals(map.getEnd(), 
									ACUtil.toLong(mapBeginEnd.get(ACSearch.END)));
							
						}
						
						Iterator<String> iterCompareTree = mapMatch.keySet().iterator();
						
						int countFounds = 0;
						while (iterCompareTree.hasNext()) {
							iterCompareTree.next();
							countFounds += 1;
						}
						
						assertEquals(countFounds, list.size());
					}
					db.stopGraphDatabase();
				}
			}
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testSearchEmbedded() throws NoSuchFieldException, SecurityException, JSONException, IOException, IllegalArgumentException, IllegalAccessException {
		
		Field responseContent = ListRepresentation.class.getDeclaredField("content");
		responseContent.setAccessible(true);
		
		
		// Attribute Random add, change and delete
		for (int i = 0; i < ahoXmlReader.numbersOfTrees(); i++) {
			for(int modeCreate = 2; modeCreate >= 0; modeCreate--){
				for(int modeSearch = 0; modeSearch >= 0 && modeSearch <=modeCreate && modeSearch<2; modeSearch++){
					System.out.println("Dict :"+i+" Mode: "+modeCreate + " "+ modeSearch+"Embedded");
				
					GraphDatabaseService graphDb = db.startGraphDatabase();
					List entries = ahoXmlReader.getListOfEntriesByTree(i, false);
					String json = g.toJson(entries);
					
					/*** CREATE A DICT TREE ***/
					
					// // Create DicTree ////
					String DICT = "Dict" + i + modeCreate + modeSearch;
					ACDictionary dict = new ACDictionary(DICT, modeCreate, modeSearch);
					boolean createDict = ACFactoryEmbedded.createDictTree(graphDb, dict.toJSONString());
					assertTrue(createDict);
					assertTrue(ACFactoryEmbedded.addListToDictTree(graphDb, dict.toJSONString(), json));
					
					boolean prepareDict = ACFactoryEmbedded.prepareDictTreeForSearch(graphDb, g.toJson(dict));
					assertTrue(prepareDict);
						
					// FULL SEARCH TEST
					Map<String, Object> mapFull = ahoXmlReader.queryOfTree(i, ahoXmlReader.FULL_MOD);
					Iterator<String> fullQueryIter = mapFull.keySet().iterator();
					
					while (fullQueryIter.hasNext()) {
						String queryString = fullQueryIter.next();
						
						ACQuery query = new ACQuery(queryString, ACSearch.FULL_WITH_OVERLAPPING);
						String test = ACSearch.search(graphDb, dict.toJSONString(), query.toJSONString());
						query = new ACQuery(new JSONObject(test));
						
						Map<String, Object> mapMatch = (HashMap<String, Object>) mapFull.get(queryString);
						
						assertTrue(query.getAllMatches().size()>0);
						List<ACMatch> list = query.getAllMatches();
						
						for (int x = 0; x < list.size(); x++) {
							ACMatch map = list.get(x);
							
							assertTrue(mapMatch.containsKey(map.getEntry().entryString()));
							Map<String, Object> mapBeginEnd = (Map<String, Object>) mapMatch.get(map.getEntry().entryString());
							
							assertEquals(map.getBegin(), 
									ACUtil.toLong(mapBeginEnd.get(ACSearch.BEGIN)));
							assertEquals(map.getEnd(), 
									ACUtil.toLong(mapBeginEnd.get(ACSearch.END)));
							
						}
						
						Iterator<String> iterCompareTree = mapMatch.keySet().iterator();
						
						int countFounds = 0;
						while (iterCompareTree.hasNext()) {
							iterCompareTree.next();
							countFounds += 1;
						}
						
						assertEquals(countFounds, list.size());
					}
					
					// FULL SEARCH WITHOUT OVERLAPPING TEST
					Map<String, Object> mapWithout = ahoXmlReader.queryOfTree(i, ahoXmlReader.WITHOUT_MOD);
					Iterator<String> withoutQueryIter = mapFull.keySet().iterator();
					
					while (withoutQueryIter.hasNext()) {
						String queryString = withoutQueryIter.next();
						ACQuery query = new ACQuery(queryString, ACSearch.FULL_WITHOUT_OVERLAPPING);
						String test = ACSearch.search(graphDb, dict.toJSONString(), query.toJSONString());
						query = new ACQuery(new JSONObject(test));
						
						Map<String, Object> mapMatch = (HashMap<String, Object>) mapWithout.get(queryString);
						
						assertTrue(query.getAllMatches().size()>0);
						List<ACMatch> list = query.getAllMatches();
						
						for (int x = 0; x < list.size(); x++) {
							ACMatch map = list.get(x);
							
							assertTrue(mapMatch.containsKey(map.getEntry().entryString()));
							Map<String, Object> mapBeginEnd = (Map<String, Object>) mapMatch.get(map.getEntry().entryString());
							
							assertEquals(map.getBegin(), 
									ACUtil.toLong(mapBeginEnd.get(ACSearch.BEGIN)));
							assertEquals(map.getEnd(), 
									ACUtil.toLong(mapBeginEnd.get(ACSearch.END)));
							
						}
						
						Iterator<String> iterCompareTree = mapMatch.keySet().iterator();
						
						int countFounds = 0;
						while (iterCompareTree.hasNext()) {
							iterCompareTree.next();
							countFounds += 1;
						}
						
						assertEquals(countFounds, list.size());
					}
					
					db.stopGraphDatabase();
				
				}
			}
		}
	}
	
	@AfterClass
	public static void shutdown() {
		db.stopBatchInserter();
		db.stopGraphDatabase();
	}
	
}
