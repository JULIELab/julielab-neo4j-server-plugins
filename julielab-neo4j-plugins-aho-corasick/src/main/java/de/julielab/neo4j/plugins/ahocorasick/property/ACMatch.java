package de.julielab.neo4j.plugins.ahocorasick.property;

import org.neo4j.shell.util.json.JSONException;
import org.neo4j.shell.util.json.JSONObject;

public class ACMatch {
	
	private ACEntry entry;
	private long start;
	private long end;
	
	public ACMatch(ACEntry entry, long start, long end) {
		this.entry = entry;
		this.start = start;
		this.end = end;
	}
	
	public ACMatch(JSONObject ob) throws JSONException{
		this.entry = new ACEntry(ob.getJSONObject("entry"));
		this.start = ob.getLong("start");
		this.end = ob.getLong("end");
	}
	
	public ACEntry getEntry(){
		return this.entry;
	}
	
	public long getBegin(){
		return this.start;
	}
	
	public long getEnd(){
		return this.end;
	}

}
