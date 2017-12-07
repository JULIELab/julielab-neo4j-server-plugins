package de.julielab.neo4j.plugins.datarepresentation;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

import de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants;

public class ImportFacetTermRelationship {

	/**
	 * @deprecated used {@link ConceptCoordinates}
	 */
	@JsonProperty(ConceptConstants.RS_TARGET_SRC_ID)
	public String targetSrcId;
	/**
	 * @deprecated used {@link ConceptCoordinates}
	 */
	@JsonProperty(ConceptConstants.RS_TARGET_SRC)
	public String targetSource;
	/**
	 * @deprecated used {@link ConceptCoordinates}
	 */
	@JsonProperty(ConceptConstants.RS_TARGET_ORG_ID)
	public String targetOrgSrcId;
	/**
	 * @deprecated used {@link ConceptCoordinates}
	 */
	@JsonProperty(ConceptConstants.RS_TARGET_ORG_SRC)
	public String targetOrgSource;
	@JsonProperty(ConceptConstants.RS_TYPE)
	public String type;
	@JsonProperty(ConceptConstants.RS_PROPS)
	public Map<String, Object> properties;

	public ImportFacetTermRelationship(String targetSrcId, String targetSource, String type,
			ConceptConstants.IdType idType) {
		switch (idType) {
		case SOURCE:
			this.targetSrcId = targetSrcId;
			this.targetSource = targetSource;
			break;
		case ORIGINAL_SOURCE:
			this.targetOrgSrcId = targetSrcId;
			this.targetOrgSource = targetSource;
		}
		this.type = type;
	}

	public void addProperty(String name, String value) {
		if (null == properties)
			properties = new HashMap<>();
		properties.put(name, value);
	}
}
