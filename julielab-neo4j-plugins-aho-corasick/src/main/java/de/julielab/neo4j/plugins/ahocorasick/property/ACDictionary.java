package de.julielab.neo4j.plugins.ahocorasick.property;

import org.neo4j.shell.util.json.JSONException;
import org.neo4j.shell.util.json.JSONObject;
import org.neo4j.shell.util.json.JSONString;

import com.google.gson.Gson;

import de.julielab.neo4j.plugins.ahocorasick.ACFactoryBatch;
import de.julielab.neo4j.plugins.ahocorasick.ACFactoryEmbedded;
import de.julielab.neo4j.plugins.ahocorasick.ACSearch;

public class ACDictionary {

	/*PROPERTIES*/
	// Dictionary Name
	private String name;
	// Mode of Style of the Dictionary 
	private int modeSearch;
	private int modeCreate;
	
	/*STRING FOR JSON OBJECT*/
	private final String NAME = "name";
	private final String MODESEARCH = "modeSearch";
	private final String MODECREATE = "modeCreate";
	
	/*DEFAUL/LOCAL - MODE - SEARCH*/
	public final static int DEFAULT_MODE_SEARCH = 0;
	public final static int LOCAL_MODE_SEARCH = 1;
	
	/*DEFAUL/LOCAL - MODE - SEARCH*/
	public final static int DEFAULT_MODE_CREATE = 0;
	public final static int LOCAL_MODE_CREATE = 1;
	public final static int GLOBAL_MODE_CREATE = 2;
	

	/**
	 * Initial a {@link ACDictionary} for the communication
	 * with the {@link ACFactoryBatch}, {@link ACFactoryEmbedded} and 
	 * {@link ACSearch}.
	 * @param name - Name of the Dictionary
	 * @param mode - Mode in which it's working
	 */
	public ACDictionary(String name, int modeCreate, int modeSearch) {
		this.name = name;
		
		// Just in Case
		if(modeCreate<modeSearch){
			modeSearch = modeCreate;
		}
		
		this.modeCreate = modeCreate;
		this.modeSearch = modeSearch;
	}
	
	/**
	 * Cast given information back
	 * @param ob - {@link JSONObject}
	 * @throws JSONException
	 */
	public ACDictionary(JSONObject ob) throws JSONException {
		this.name = ob.getString(NAME);
		this.modeCreate = ob.getInt(MODECREATE);
		this.modeSearch = ob.getInt(MODESEARCH);
	}
	
	/*GET-METHODS*/
	
	/**
	 * Name of the Dictionary
	 * @return
	 */
	public String name(){
		return this.name;
	}
	
	/**
	 * Answer to the question: Is Dictionary searching in local mode?
	 * @return <code>true</code>/<code>false</code>
	 */
	public boolean isLocalSearch(){
		return modeSearch == LOCAL_MODE_SEARCH;
	}
	
	/**
	 * Get the mode of creation
	 * @return
	 */
	public int getCreateMode(){
		return this.modeCreate;
	}
	
	public boolean isLocalCreate(){
		return modeCreate == LOCAL_MODE_CREATE;
	}
	
	public boolean isGlobalCreate(){
		return modeCreate == GLOBAL_MODE_CREATE;
	}
	
	/*CONVERTER*/
	
	/**
	 * Converts the current object to 
	 * a {@link JSONString}.
	 * @return {@link String}
	 */
	public String toJSONString(){
		Gson g = new Gson();
		return g.toJson(this);
	}
}
