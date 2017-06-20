package de.julielab.neo4j.plugins.ahocorasick.property;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

/**
 * @author SebOh
 */

public class ACGlobalMap {

	// Global Map for the dictionary
	private Map<Long, Map<String, Long>> globalMap;

	/**
	 * Creates a brand new Global Map Object without any information about the 
	 * dictionary.
	 */
	public ACGlobalMap() {
		this.globalMap = new HashMap<Long, Map<String, Long>>();
	}
	
	
	/**
	 * Adds a new node to the GlobalMap
	 * @param id - ID of the Node
	 */
	public void addNode(long id){
		Map<String, Long> to = new HashMap<>();
		if(!globalMap.containsKey(id)){
			globalMap.put(id, to);
		}
	}
	
	/**
	 * Adds a relationship between to nodes
	 * @param idStart - ID of the start node
	 * @param key - Name of the Relationship
	 * @param idEnd - ID of the end node
	 * @return <code>true</code> if success, <code>false</code> if not
	 */
	public boolean addRel(long idStart, String key, long idEnd){
		if(!globalMap.containsKey(idStart)){
			addNode(idStart);
		}
		
		Long value = globalMap.get(idStart).put(key, idEnd);
		
		if(value != null){
			return true;
		}
		return false;
	}
	
	/**
	 * Deletes a relationship of the map 
	 * @param id - ID of the start node of the Relation
	 * @param key - Name of the Relationship
	 * @return <code>true</code> if success, <code>false</code> if not
	 */
	public boolean deleteRel(long id, String key){
		return globalMap.get(id).remove(key) != null;
	}
	
	
	public boolean hasRel(long idStart, String key){
		if(globalMap.containsKey(idStart)){
			return globalMap.get(idStart).containsKey(key);
		}
		return false;
	}
	
	public int numberOfRel(long idStart){
		if(globalMap.containsKey(idStart)){
			return globalMap.get(idStart).size();
		}
		return 0;
	}
	
	public Iterator<String> iteratorNextNodes(long idStart){
		if(globalMap.containsKey(idStart)){
			return globalMap.get(idStart).keySet().iterator();
		}
		HashSet<String> empty = new HashSet<>();
		return empty.iterator();
	}
	
	public long getNodeID(long idStart, String key){
		Map<String, Long> map = globalMap.get(idStart);
		if(map!=null){
			Long id = map.get(key);
			if(id!=null){
				return id;
			}
			// No Relation Found
			return -1;
		}
		// Node is not in Global Map
		return -2;
	}
	
	public Map<Long, Map<String, Long>> getRelMap(){
		return this.globalMap;
	}
	
}
