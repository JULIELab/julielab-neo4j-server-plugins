package de.julielab.neo4j.plugins.ahocorasick.property;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.shell.util.json.JSONArray;
import org.neo4j.shell.util.json.JSONException;
import org.neo4j.shell.util.json.JSONObject;
import org.neo4j.shell.util.json.JSONString;

import com.google.gson.Gson;

public class ACQuery {

	/*PROPERTIES*/
	private String[] query;
	private List<ACMatch> matches;
	private int mode;
	
	/*KONSTANTE*/
	private final String QUERY = "query";
	private final String MATCHES = "matches";
	private final String MODE = "mode";
	
	// CONSTANT FOR THE MODE OF THE SEARCHING PROCESS
	public final static int FULL_WITH_OVERLAPPING = 0;
	public final static int FULL_WITHOUT_OVERLAPPING = 1;
	public final static int PROGRESSIV = 2;
	
	/**
	 * Tokenized version of the query
	 * @param query
	 */
	public ACQuery(String[] query, int mode) {
		this.query = query;
		this.mode = mode;
		this.matches = new ArrayList<>();
	}
	
	/**
	 * Default query will be split into letters
	 * @param query
	 */
	public ACQuery(String query, int mode){
		this(Arrays.copyOfRange(query.split(""), 1, query.length()+1), mode);
	}
	
	/**
	 * Initial after conversation
	 * @param ob
	 * @throws JSONException
	 */
	public ACQuery(JSONObject ob) throws JSONException{
		
		JSONArray array = (JSONArray) ob.get(QUERY);
		this.query = new String[array.length()];
		for(int i = 0; i<query.length; i++){
			this.query[i] = array.getString(i);
		}
		
		this.mode = ob.getInt(MODE);
		
		array = (JSONArray) ob.get(MATCHES);
		this.matches = new ArrayList<ACMatch>();
		for(int i = 0; i<array.length(); i++){
			this.matches.add(new ACMatch((JSONObject) array.get(i)));
		}
	}
	
	/*ADD-METHODEN*/
	
	public void addMatch(ACMatch match){
		matches.add(match);
	}
	
	/*GET-METHODEN*/
	
	/**
	 * Returns the query
	 * @return
	 */
	public String[] getQuery(){
		return this.query;
	}
	
	/**
	 * Return the mode for searching
	 * @return
	 */
	public int getMode(){
		return this.mode;
	}
	
	/**
	 * Return a {@link List} of all {@link ACMatch} for the query
	 * @return
	 */
	public List<ACMatch> getAllMatches(){
		return this.matches;
	}
	
	/*CONVERTATION*/
	
	/**
	 * Converts the Object to a {@link JSONString}
	 * @return
	 */
	public String toJSONString(){
		Gson g = new Gson();
		return g.toJson(this);
	}
	
}
