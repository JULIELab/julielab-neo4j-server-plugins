package de.julielab.neo4j.plugins.ahocorasick.property;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.shell.util.json.JSONArray;
import org.neo4j.shell.util.json.JSONException;
import org.neo4j.shell.util.json.JSONObject;

import de.julielab.neo4j.plugins.ahocorasick.ACProperties;

public class ACEntry {

	private final String entry;
	private Map<String, Object> attributes;
	private String[] tokens;
	
	public ACEntry(String entry) {
		this.entry = entry;
		this.attributes = new HashMap<>();
		this.tokens = entry.split("");
		this.tokens = Arrays.copyOfRange(tokens, 1, tokens.length);
	}
	
	public ACEntry(String entry, String[] tokens) {
		this.entry = entry;
		this.attributes = new HashMap<>();
		this.tokens = tokens;
	}
	
	@SuppressWarnings("unchecked")
	public ACEntry(JSONObject entry) throws JSONException{
		this.entry = entry.getString(ACProperties.ENTRY);
		this.attributes = ((JSONObject) entry.get(ACProperties.ATTRIBUTES)).toMap();
		
		JSONArray array = (JSONArray) entry.get("tokens");
		this.tokens = new String[array.length()];
		for(int i = 0; i<tokens.length; i++){
			this.tokens[i] = array.getString(i);
		}
	}
	
	//////// ENTRY METHODS
	
	public String entryString(){
		return entry;
	}
	
	
	public String[] getTokens(){
		return tokens;
	}
	
	//////// ATTRIBUTE METHODS
	
	public void addAttribute(String key, String value){
		attributes.put(key, value);
	}
	
	public boolean deleteAttribute(String key){
		attributes.remove(key);
		return !hasAttribute(key);
		
	}
	
	public boolean hasAttribute(String key){
		return attributes.containsKey(attributes);
	}
	
	public Object getAttribute(String key){
		return attributes.get(key);
	}
	
	public Map<String, Object> getAllAttributes(){
		return this.attributes;
	}
	
	
	///////// CONVERTER
	
//	public JSONObject toJSONObject() throws JSONException{
//		JSONObject ob = new JSONObject();
//		ob.putOnce(ACProperties.ENTRY, entry);
//		ob.putOnce(ACProperties.ATTRIBUTES, attributes);
//		ob.put(A, value)
//		return ob;
//	}
//	
//	public String toJSONObjectString() throws JSONException{
//		return toJSONObject().toString();
//	}
}
