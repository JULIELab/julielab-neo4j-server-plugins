package de.julielab.neo4j.plugins.ahocorasick;

import java.util.Iterator;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.Name;
import org.neo4j.server.plugins.Parameter;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;
import org.neo4j.shell.util.json.JSONException;
import org.neo4j.shell.util.json.JSONObject;

import com.google.gson.Gson;

import de.julielab.neo4j.plugins.ahocorasick.property.ACDictionary;
import de.julielab.neo4j.plugins.ahocorasick.property.ACEntry;
import de.julielab.neo4j.plugins.ahocorasick.property.ACMatch;
import de.julielab.neo4j.plugins.ahocorasick.property.ACQuery;

public class ACSearch extends ServerPlugin{

	
	// CONSTANT FOR THE MODE OF THE SEARCHING PROCESS
	public final static int FULL_WITH_OVERLAPPING = 0;
	public final static int FULL_WITHOUT_OVERLAPPING = 1;
	public final static int PROGRESSIV = 2;
	
	// CONSTANTS FOR MATCHING
	public final static String MATCH = "match";
	public final static String BEGIN = "begin";
	public final static String END = "end";
	public final static String NODE = "node";
	
	/**
	 * Returns a list of attribute names
	 * 
	 * @param graphDb
	 *            - Graphdatabase
	 * @param name
	 *            - Name of the DictTree
	 * @param entry
	 *            - Name of the entry for the attributes
	 * @return list with attribute names
	 * @throws SecurityException
	 * @throws NoSuchFieldException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws JSONException
	 */
	@SuppressWarnings("unchecked")
	@Name("get_entry")
	@Description("Get ACEntry as Json String.")
	@PluginTarget(GraphDatabaseService.class)
	public static String getCompleteEntry(@Source GraphDatabaseService graphDb, 
			@Description("ACDictionary as JSONString") @Parameter(name = "dict") String dictJSON,
			@Description("Entry to Change") @Parameter(name = "entry") String entry) 
					throws SecurityException, NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException, JSONException {
		
		Transaction tx = graphDb.beginTx();
		try{
			ACDictionary dict = new ACDictionary(new JSONObject(dictJSON));
			// Find root for DictTree
			Node root = ACUtil.getRootNode(graphDb, dict.name());
			
			// Check if root exists, If not return null
			if (root != null) {
				// Find node with entry
				Node node = ACUtil.getExactNode(dict, graphDb, root, entry);
				// Check of a output in node exist
				if (ACUtil.toLong(node.getProperty(ACProperties.NUMBER_OUTPUT)) > 0) {
					// Get all Attributenames for the first output of the node
					ACEntry acEntry = new ACEntry(entry);

					JSONObject jsonOb = new JSONObject(String.valueOf(node.getProperty(ACProperties.OUTPUT)));
					JSONObject attributes = new JSONObject(jsonOb.getString(entry));
					Iterator<String> iter = attributes.keys();
					
					while (iter.hasNext()) {
						String key = iter.next();
						acEntry.addAttribute(key, attributes.getString(key));
					}
					
					return new Gson().toJson(acEntry);
				}
			}
			
			// Returns null, if no root exists
			return null;
		}finally{
			tx.success();
		}
	}
	
	/**
	 * Implementation of the algorithm 1 in the paper of ahoCorasick See <a href="http://dl.acm.org/citation.cfm?doid=360825.360855">article</a>
	 * 
	 * @param graphDb
	 *            - GraphDatabase
	 * @param name
	 *            - Name of the DictTree
	 * @param query
	 *            - the Query for searching
	 * @param mode
	 *            - PROGRESSIV, FULL_WITH_OVERLAPPING, FULL_WITHOUT_OVERLAPPING
	 * @return List of maps which contains start, end, node and match
	 * @throws JSONException
	 */
	@Name("search")
	@Description("Get all matches of the query with the dictionary but without overlapping.")
	@PluginTarget(GraphDatabaseService.class)
	@SuppressWarnings("unchecked")
	public static String search(@Source GraphDatabaseService graphDb,
			@Description("ACDictionary as JSONString") @Parameter(name = "dict") String dictJSON,
			@Description("Queries to look for substrings") @Parameter(name = "queries") String queryJSON) throws JSONException {

		Transaction tx = graphDb.beginTx();
		
		ACDictionary dict = new ACDictionary(new JSONObject(dictJSON));
		ACQuery queryOb = new ACQuery(new JSONObject(queryJSON));
		String[] query = queryOb.getQuery();
		
		// Getting root of the DictTree
		Node root = ACUtil.getRootNode(graphDb, dict.name());
		
		if (ACUtil.isDictTreePrepared(root)) {
			try{
				
				
				Node currentState = root;
				// If Null return null
				if (currentState == null) {
					return null;
				}
				
				// If Tree is not prepared for searching return null
				if (!ACUtil.isDictTreePrepared(currentState)) {
					return null;
				}
				
				String letter;
				
				// Constants for the non overlapping search mode
				long beginLast = 0;
				long endLast = 0;
				ACMatch matchOld = null;
				
				// Search letter by letter in the DictTree
				for (int i = 0; i < query.length; i++) {
					// Getting next letter in the query
					letter = query[i];
					
					Node toNode = null;
					// As long their is no relation with name of the letter get failNode
					while ((toNode = ACUtil.getNextNodeSearch(dict, graphDb, currentState, letter)) == null) {
						if(dict.isLocalSearch()){
							JSONObject obBetween = new JSONObject(String.valueOf(currentState.getProperty(ACProperties.RELATIONSHIP)));
							currentState = graphDb.getNodeById(obBetween.getLong(ACProperties.EdgeTypes.FAIL.toString()));
						}else{
							currentState  = currentState.getSingleRelationship(ACProperties.EdgeTypes.FAIL, Direction.OUTGOING).getEndNode();
						}
					}
					currentState = toNode;
					
					int numberFounds = (Integer) currentState
							.getProperty(ACProperties.NUMBER_OUTPUT);
					
					// If current state has a output then get this output
					if (numberFounds > 0) {
						
						// Constants important for Overlapping and Progessiv search
						long beginLongest = 0;
						long endLongest = 0;
						ACMatch matchLongest = null;
						
						// Get each output of the node
						JSONObject jsonObOut = new JSONObject(String.valueOf(currentState
								.getProperty(ACProperties.OUTPUT)));
						
						Iterator<String> iterOut = jsonObOut.keys();
						while (iterOut.hasNext()) {
							String key = iterOut.next();
							Object attributesEl = jsonObOut.get(key);
							
							if (attributesEl.getClass().equals(JSONObject.class)) {
								JSONObject attributesJsonOb = (JSONObject) attributesEl;
								
								ACEntry entry = new ACEntry(key);
								
								Iterator<String> iterAtt = attributesJsonOb.keys();
								while (iterAtt.hasNext()) {
									String keyAt = iterAtt.next();
									entry.addAttribute(keyAt, attributesJsonOb.getString(keyAt).replaceAll("\"", ""));
								}
								
								// Determine begin and end of the match
								long begin = i + 1 - key.length();
								long end = i + 1;
								
								// Create Match Map for the match
								ACMatch match = new ACMatch(entry, begin, end);
								
								switch (queryOb.getMode()) {
								// WITH Overlapping
								case FULL_WITH_OVERLAPPING:
									queryOb.addMatch(match);
									break;
									
								default:
									// Look which is the longest match in the node
									if ((end - begin) > (endLongest - beginLongest)) {
										beginLongest = begin;
										endLongest = end;
										matchLongest = match;
									}
								}
							}
						}
						
						switch (queryOb.getMode()) {
						case FULL_WITHOUT_OVERLAPPING:
							// Is the current match overlapping to a match before
							// and if so which match is longer? The longest match
							// is here important
							if (endLast <= beginLongest) {
								beginLast = beginLongest;
								endLast = endLongest;
								
								if (matchOld != null)
									queryOb.addMatch(matchOld);
								
								matchOld = matchLongest;
								
								break;
							}
							
							if (beginLongest <= endLast) {
								if ((endLast - beginLast) < (endLongest - beginLongest)) {
									beginLast = beginLongest;
									endLast = endLongest;
									matchOld = matchLongest;
								}
							}
							break;
						case PROGRESSIV:
							// Return first longest match
							queryOb.addMatch(matchLongest);
							return queryOb.toJSONString();
						default:
							break;
						}
					}
				}
				
				// Adds the last match
				if (queryOb.getMode() == FULL_WITHOUT_OVERLAPPING) {
					queryOb.addMatch(matchOld);
				}
				
				return queryOb.toJSONString();
			}finally{
				tx.success();
			}
		}
		return "";
	}
	
}
