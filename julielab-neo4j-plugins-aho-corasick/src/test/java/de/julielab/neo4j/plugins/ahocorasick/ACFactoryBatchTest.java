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
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.shell.util.json.JSONException;
import org.neo4j.shell.util.json.JSONObject;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchRelationship;
import org.xml.sax.SAXException;

import de.julielab.neo4j.plugins.AhoCorasickXmlTreeReader;
import de.julielab.neo4j.plugins.ahocorasick.property.ACDataBase;
import de.julielab.neo4j.plugins.ahocorasick.property.ACDictionary;
import de.julielab.neo4j.plugins.ahocorasick.property.ACGlobalMap;
import de.julielab.neo4j.plugins.test.TestUtilities;

// works in Eclipse but not with "mvn clean test" on the command line...?!
@Ignore
public class ACFactoryBatchTest {

	private static AhoCorasickXmlTreeReader ahoXmlReader;
	private static ACDataBase db;

	@BeforeClass
	public static void initialize() throws SAXException, IOException, ParserConfigurationException {
		db = new ACDataBase(TestUtilities.GRAPH_DB_DIR);
		
		ahoXmlReader = new AhoCorasickXmlTreeReader("src/test/resources/trees.xml");
		
		TestUtilities.deleteEverythingInDB(db.startGraphDatabase());
		db.stopGraphDatabase();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testCreateDictTreeLocal() throws JSONException, IOException{
		// CREATE AND PREPARE
		BatchInserter batchInserter = db.startBatchInserter();
		Map<String, String> propertyMap;
		for (int i = 0; i < ahoXmlReader.numbersOfTrees(); i++) {
			for(int modeCreate = 2; modeCreate >= 0; modeCreate--){
				for(int modeSearch = 0; modeSearch >= 0 && modeSearch <=modeCreate && modeSearch<2; modeSearch++){
					System.out.println("Dict :"+i+" Mode: "+modeCreate + " "+ modeSearch);
				
					List entries = ahoXmlReader.getListOfEntriesByTree(i, false);
					
					propertyMap = ahoXmlReader.getPropertiesOfTree(i);
					/*** CREATE A DICT TREE ***/
					
					// // Create Root ////
					String DICT = "Dict" + i + modeCreate + modeSearch;
					ACDictionary dict = new ACDictionary(DICT, modeCreate, modeSearch);
					ACDictionary dictFalse = new ACDictionary("Dict", modeCreate, modeSearch);
					boolean createDict = ACFactoryBatch.createDictTree(batchInserter, dict);
					
					// TEST METHODE
					assertTrue(createDict);
					assertTrue(!ACFactoryBatch.createDictTree(batchInserter, dict));
					
					assertTrue(ACFactoryBatch.addListToDictTree(db, dict, entries));
					assertTrue(!ACFactoryBatch.addListToDictTree(db, dictFalse, entries));
					
					long rootId = ACUtil.getRootID(batchInserter, DICT);
					assertTrue(rootId != -1);
					
					Map<String, Object> mapRootProperties = batchInserter.getNodeProperties(rootId);
					
					// Wieviele Knoten hat der Baum? (Wurzel-Info)
					assertEquals(ACUtil.toLong(propertyMap.get(ahoXmlReader.ALLNODES)), 
							ACUtil.toLong(mapRootProperties.get(ACProperties.NODES_IN_TREE)));
					// Wieviel Einträge gibt es? (Wurzel-Info)
					assertEquals(entries.size(), 
							ACUtil.toLong(mapRootProperties.get(ACProperties.NUMBER_OF_ENTRIES)));
					// Wieviel Einträge gibt es? (Per Hand gezählt)
					assertEquals(entries.size(), entriesInTree(batchInserter, rootId));
					// Ist Baum vorbereitet? FALSE
					assertTrue(!ACUtil.toBoolean(mapRootProperties.get(ACProperties.PREPARED)));
					// Stimmen die Relationshipsmaps mit den wirklichen überein?
					if(dict.isLocalCreate()){
						assertTrue(areRelationsSameLocal(batchInserter, rootId));
					}
					if(dict.isGlobalCreate()){
						assertTrue(areRelationsSameGlobal(batchInserter, rootId, db.getMap(DICT)));
					}
					
					// // Prepare Tree /////
					boolean prepareDict = ACFactoryBatch.prepareDictTreeForSearch(db, dict);
					assertTrue(prepareDict);
					
					mapRootProperties = batchInserter.getNodeProperties(rootId);
					assertTrue(ACUtil.toBoolean(mapRootProperties.get(ACProperties.PREPARED)));
					// Darf nichts ergänzen, weil Baum schon prepared ist
					assertTrue(!ACFactoryBatch.addListToDictTree(db, dict, entries));
					// Wieviele Knoten hat der Baum "Wurzel-Info"
					assertEquals(ACUtil.toLong(propertyMap.get(ahoXmlReader.ALLNODES)), 
							ACUtil.toLong(mapRootProperties.get(ACProperties.NODES_IN_TREE)));
					// Wieviele Knoten hat der Baum "Per Hand gezählt"
					assertEquals(ACUtil.toLong(propertyMap.get(ahoXmlReader.ALLNODES))
							, nodesInTree(batchInserter, rootId));
					// Wieviele Incoming Fail-Relationen in Root existieren
					assertEquals(ACUtil.toLong(propertyMap.get(ahoXmlReader.FAILRELINROOTS)), 
							countIncomingFailRelationsInNode(batchInserter, rootId));
					// Wieviele Output Nodes gibt es?
					assertEquals(ACUtil.toLong(propertyMap.get(ahoXmlReader.FINALNODES)), 
							countOutputNodes(batchInserter, rootId));
					// Wieviel Original Nodes gibt es?
					assertEquals(entries.size(), entriesInTree(batchInserter, rootId));
					// Stimmen die Relationshipsmaps mit den wirklichen überein?
					if(dict.isLocalCreate()){
						assertTrue(areRelationsSameLocal(batchInserter, rootId));
					}
				}
			}
		}
	}
	
	@AfterClass
	public static void shutdown() {
		db.stopBatchInserter();
		db.stopGraphDatabase();
	}
	
	///////////////// HELP FUNCTIONS FOR TESTING
	// TODO: SIMPLFY!!!!!
	
	private long entriesInTree(BatchInserter batchInserter, long rootId) throws JSONException {

		long count = 0;

		Deque<Long> deq = new ArrayDeque<>();
		deq.add(rootId);
		
		while(!deq.isEmpty()){
			
			long currentId = deq.pop();
			Iterator<BatchRelationship> iter = batchInserter.getRelationships(currentId).iterator();
			while(iter.hasNext()){
				BatchRelationship rel = iter.next();
				if(rel.getStartNode() == currentId && !rel.getType().name().equals(ACProperties.getFailName())){
					deq.add(rel.getEndNode());
				}
			}
			
			if(batchInserter.getNodeProperties(currentId).containsKey(ACProperties.ORIGINAL))
				count++;
		}		
		
		return count;
	}
	
	private long countIncomingFailRelationsInNode(BatchInserter batchInserter, long rootId) throws JSONException {
		
		long countFail = 0;
		
		Iterator<BatchRelationship> iter = batchInserter.getRelationships(rootId).iterator();
		while(iter.hasNext()){
			BatchRelationship rel = iter.next();
			if(rel.getEndNode() == rootId){
				countFail++;
			}
		}
		
		return countFail;
	}
	
	private long nodesInTree(BatchInserter batchInserter, long rootId) throws JSONException {
		
		long count = 0;
		
		Deque<Long> deq = new ArrayDeque<>();
		deq.add(rootId);
		
		while(!deq.isEmpty()){
			
			long currentId = deq.pop();
			Iterator<BatchRelationship> iter = batchInserter.getRelationships(currentId).iterator();
			while(iter.hasNext()){
				BatchRelationship rel = iter.next();
				if(rel.getStartNode() == currentId && !rel.getType().name().equals(ACProperties.getFailName())){
					deq.add(rel.getEndNode());
				}
			}
			
			count++;
		}		
		
		return count;
	}
	
	private long countOutputNodes(BatchInserter batchInserter, long rootId) throws JSONException {
		
		long count = 0;
		
		Deque<Long> deq = new ArrayDeque<>();
		deq.add(rootId);
		
		while(!deq.isEmpty()){
			
			long currentId = deq.pop();
			Iterator<BatchRelationship> iter = batchInserter.getRelationships(currentId).iterator();
			while(iter.hasNext()){
				BatchRelationship rel = iter.next();
				if(rel.getStartNode() == currentId && !rel.getType().name().equals(ACProperties.getFailName())){
					deq.add(rel.getEndNode());
				}
			}
			
			if(ACUtil.toLong(batchInserter.getNodeProperties(currentId)
					.get(ACProperties.NUMBER_OUTPUT))>0){
				count++;
			}
		}		
		
		return count;
	}
	
	private boolean areRelationsSameLocal(BatchInserter batchInserter, long rootId) throws JSONException {
		
		Deque<Long> deq = new ArrayDeque<>();
		deq.add(rootId);
		
		while(!deq.isEmpty()){
			
			long currentId = deq.pop();
			
			Iterator<Long> iter = batchInserter.getRelationshipIds(currentId).iterator();
			Map<String, Object> mapNodeProperties = batchInserter.getNodeProperties(currentId);
			JSONObject obProp = new JSONObject(String.valueOf(mapNodeProperties.get(ACProperties.RELATIONSHIP)));
			
			while(iter.hasNext()){
				long rel = iter.next();
				
				BatchRelationship batchRel = batchInserter.getRelationshipById(rel);
				Map<String, Object> mapRel = batchInserter.getRelationshipProperties(batchRel.getId());
				String nameRel = (String) mapRel.get(ACProperties.LETTER);
				
				// We just look at outgoing Relationships
				if(batchRel.getStartNode()==currentId && batchRel.getEndNode() != currentId){
					if(!batchRel.getType().name().equals(ACProperties.getFailName())){
						deq.add(batchRel.getEndNode());
					}else{
						nameRel = batchRel.getType().name();
					}
					
					if(!obProp.has(nameRel)){
						return false;
					}
					if(ACUtil.toLong(obProp.get(nameRel))!=batchRel.getEndNode()){
						return false;
					}
				}
			}
		}		
		
		return true;
	}
	
	private boolean areRelationsSameGlobal(BatchInserter batchInserter, long rootId, ACGlobalMap map) throws JSONException {
		
		Deque<Long> deq = new ArrayDeque<>();
		deq.add(rootId);
		
		while(!deq.isEmpty()){
			
			long currentId = deq.pop();
			
			Iterator<Long> iter = batchInserter.getRelationshipIds(currentId).iterator();
			
			while(iter.hasNext()){
				long rel = iter.next();
				BatchRelationship batchRel = batchInserter.getRelationshipById(rel);
				Map<String, Object> mapRel = batchInserter.getRelationshipProperties(batchRel.getId());
				String nameRel = (String) mapRel.get(ACProperties.LETTER);
				
				// We just look at outgoing Relationships
				if(batchRel.getStartNode()==currentId){
					if(!batchRel.getType().name().equals(ACProperties.getFailName())){
						deq.add(batchRel.getEndNode());
					}
					
					if(!map.hasRel(currentId, nameRel)){
						return false;
					}
					if(ACUtil.toLong(map.getNodeID(currentId, nameRel))!=batchRel.getEndNode()){
						return false;
					}
				}
			}
			
			if(map.numberOfRel(currentId) != ACUtil.toLong(batchInserter
					.getNodeProperties(currentId).get(ACProperties.NUMBER_NEXT))){
				return false;
			}
		}		
		return true;
	}
	
}
