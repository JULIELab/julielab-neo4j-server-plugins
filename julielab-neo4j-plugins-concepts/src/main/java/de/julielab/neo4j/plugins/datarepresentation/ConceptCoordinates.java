package de.julielab.neo4j.plugins.datarepresentation;

import org.neo4j.shell.util.json.JSONException;
import org.neo4j.shell.util.json.JSONObject;

import de.julielab.neo4j.plugins.auxiliaries.JSON;
import de.julielab.neo4j.plugins.constants.semedico.CoordinateConstants;
import de.julielab.neo4j.plugins.constants.semedico.TermConstants;

public class ConceptCoordinates {

	public enum CoordinateType {
		SRC, OSRC
	}

	public String sourceId;
	public String source;
	public String originalId;
	public String originalSource;
	public boolean uniqueSourceId;

	public ConceptCoordinates(String sourceId, String source, String originalId, String originalSource,
			boolean uniqueSourceId) {
		super();
		this.sourceId = sourceId;
		this.source = source;
		this.originalId = originalId;
		this.originalSource = originalSource;
		this.uniqueSourceId = uniqueSourceId;
	}

	public ConceptCoordinates(String sourceId, String source, String originalId, String originalSource) {
		this(sourceId, source, originalId, originalSource, false);
	}

	public ConceptCoordinates(String id, String source, CoordinateType src) throws JSONException {
		switch (src) {
		case OSRC:
			originalId = id;
			originalSource = source;
			break;
		case SRC:
			sourceId = id;
			this.source = source;
			break;
		}
	}

	public ConceptCoordinates(JSONObject jsonObject) throws JSONException {
		this(jsonObject, true);
	}
	
	public ConceptCoordinates(JSONObject jsonObject, boolean checkConsistency) throws JSONException {
		sourceId = JSON.getString(jsonObject, CoordinateConstants.SOURCE_ID);
		source = JSON.getString(jsonObject, CoordinateConstants.SOURCE);
		originalId = JSON.getString(jsonObject, CoordinateConstants.ORIGINAL_ID);
		originalSource = JSON.getString(jsonObject, CoordinateConstants.ORIGINAL_SOURCE);
		uniqueSourceId = JSON.getBoolean(jsonObject, CoordinateConstants.UNIQUE_SOURCE_ID);

		if (checkConsistency) {
			if (sourceId == null && source == null && originalId == null && originalSource == null)
				throw new IllegalArgumentException(
						"The passed concept coordinates JSON object did not specify any IDs or sources");

			if (originalId == null ^ originalSource == null)
				throw new IllegalArgumentException(
						"Coordinates JSON specifies originalId / original source of (" + originalId + ", "
								+ originalSource + ") but when one is not null, the other must be given, too.");
		}
	}

}
