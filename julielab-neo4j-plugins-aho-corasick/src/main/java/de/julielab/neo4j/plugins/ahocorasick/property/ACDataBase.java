package de.julielab.neo4j.plugins.ahocorasick.property;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.shell.util.json.JSONException;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import com.google.gson.Gson;

import de.julielab.neo4j.plugins.ahocorasick.ACFactoryBatch;
import de.julielab.neo4j.plugins.ahocorasick.ACFactoryEmbedded;
import de.julielab.neo4j.plugins.ahocorasick.ACProperties;


public class ACDataBase {
	
	/*PROPERTIES*/
	
	// Path to the GraphDatabase (neo4j)
	private final String path;
	// BatchInserter
	private BatchInserter batch;
	// GraphDatabseService
	private GraphDatabaseService graphDb;
	// Table of all GlobalMaps to help building a DictTree
	private Hashtable<String, ACGlobalMap> globalMaps;
	
	/**
	 * Initial a {@link ACDataBase} with the path to the
	 * neo4j Database.
	 * @param path - Path to the neo4j GraphDatabase
	 */
	public ACDataBase(String path) {
		// Set Path
		this.path = path;
		
		// Communicator with the Database don't exist
		this.batch = null;
		this.graphDb = null;
		
		// Overview of the globalMaps
		this.globalMaps = new Hashtable<>();
	}
	
	/*BATCH INSERTER*/
	
	/**
	 * Starts a {@link BatchInserter} for the Database and load all
	 * {@link ACDictionary} in the given Database. If the {@link BatchInserter} is
	 * always running it returns the current {@link BatchInserter}.
	 * @return
	 */
	public BatchInserter startBatchInserter(){
		if(batch==null){
			// Start Batch Inserter
			new BatchInserters();
			batch = BatchInserters.inserter(path);
		}
		return batch;
	}
	
	
	/**
	 * Stops the current {@link BatchInserter} and resets the overview of
	 * all dictionaries.
	 * @return
	 */
	public boolean stopBatchInserter(){
		if(batch!=null){
			batch.shutdown();
			batch = null;
			return true;
		}
		return false;
	}
	
	/*GRAPH DATABASE SERVICE*/
	
	/**
	 * Starts a new {@link GraphDatabaseService} and loads all dictionaries
	 * in an overview.
	 * @return
	 */
	public GraphDatabaseService startGraphDatabase(){
		if(graphDb==null){
			graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(path);
			return graphDb;
		}
		return graphDb;
	}
	
	/**
	 * Stops the current {@link GraphDatabaseService} and 
	 * resets the overview of all dictionaries.
	 * @return
	 */
	public boolean stopGraphDatabase(){
		if(graphDb!=null){
			graphDb.shutdown();
			graphDb = null;
			return true;
		}
		return false;
	}
	
	/*ORGANIZATION OF THE DICTIONARIES*/
	
	/**
	 * Adds a dictionary of the given parameters to
	 * the GraphDatabase.
	 * @param dict - Dictionary to add
	 * @return
	 * @throws JSONException
	 */
	public boolean addDict(ACDictionary dict) throws JSONException{
		
		boolean succes = false;
		
		if(graphDb != null){
			Gson g = new Gson();
			succes = ACFactoryEmbedded.createDictTree(graphDb, g.toJson(dict));
		}
		
		if(batch != null){
			succes =ACFactoryBatch.createDictTree(batch, dict);
		}
		
		return succes;
	}
	
	/**
	 * Gets the a {@link ACDictionary} of a dictionary in 
	 * the GraphDatabase.
	 * @param name - Name of the dictionary
	 * @return {@link ACDictionary}
	 */
	public ACDictionary getDict(String name){
		if(graphDb!=null){
			IndexManager manager = graphDb.index();
			Index<Node> index = manager.forNodes(ACProperties.INDEX_DIC);
			
			IndexHits<Node> hits = index.get(ACProperties.DICTIONARY_NAME, name);
			Node node = hits.getSingle();
			
			ACDictionary dictEntry = new ACDictionary(
					(String)node.getProperty(ACProperties.DICTIONARY_NAME),
					(int)node.getProperty(ACProperties.MODECREATE),
					(int)node.getProperty(ACProperties.MODESEARCH));
			
			return dictEntry;
		}else if(batch != null){
			// Get all Dictionaries
			// Load all Dictionaries
			BatchInserterIndexProvider indexProvider = new LuceneBatchInserterIndexProvider(batch);
			// Index of typ excact match
			BatchInserterIndex dict = indexProvider.nodeIndex( ACProperties.INDEX_DIC, MapUtil.stringMap( "type", "exact" ) );
			IndexHits<Long> hits = dict.get(ACProperties.DICTIONARY_NAME, name);
			
			Long id = hits.getSingle();
			Map<String, Object> properties = batch.getNodeProperties(id);
			
			ACDictionary dictEntry = 
					new ACDictionary(
							(String)properties.get(ACProperties.DICTIONARY_NAME),
							(int)properties.get(ACProperties.MODECREATE),
							(int)properties.get(ACProperties.MODESEARCH));
			return dictEntry;
		}
		return null;
	}
	
	/**
	 * Deletes a dictionary in the GraphDatabase.
	 * Only working if {@link GraphDatabaseService} is already running.
	 * @param name - Name of the Dictionary
	 * @return
	 * @throws IOException
	 */
	public boolean deleteDict(String name) throws IOException{
		if(graphDb != null){
			boolean succes = ACFactoryEmbedded.deleteDictTree(graphDb, name);
			if(succes){
				return true;
			}
		}
		return false;
	}
	
	/*GlobalMap*/
	
	/**
	 * Gets a {@link ACGlobalMap} for the Dictionary
	 * and creates one if no exist so far
	 * @param name
	 * @return
	 */
	public ACGlobalMap getMap(String name){
		if(!this.globalMaps.containsKey(name)){
			this.addMap(name, new ACGlobalMap());
		}
		return this.globalMaps.get(name);
	}
	
	/**
	 * Adds a {@link ACGlobalMap} for a dictionary
	 * @param name
	 * @param map
	 */
	public void addMap(String name, ACGlobalMap map){
		this.globalMaps.put(name, map);
	}
	
	/**
	 * Delets a {@link ACGlobalMap} for a dictionary
	 * @param name
	 */
	public void deleteMap(String name){
		this.globalMaps.remove(name);
	}

}
