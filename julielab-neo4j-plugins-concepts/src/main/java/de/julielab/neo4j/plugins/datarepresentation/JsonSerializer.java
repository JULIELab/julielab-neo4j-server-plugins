package de.julielab.neo4j.plugins.datarepresentation;

import com.google.gson.Gson;

public class JsonSerializer {
	private transient static Gson gson = new Gson();
	
	public static synchronized String toJson(Object serializable) {
		return gson.toJson(serializable);
	}
}
