package de.julielab.neo4j.plugins.datarepresentation;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.annotations.SerializedName;

import de.julielab.neo4j.plugins.constants.semedico.TermConstants;

public class ImportFacetTermRelationship {

	@SerializedName(TermConstants.RS_TARGET_SRC_ID)
	public String targetSrcId;
	@SerializedName(TermConstants.RS_TARGET_SRC)
	public String targetSource;
	@SerializedName(TermConstants.RS_TARGET_ORG_ID)
	public String targetOrgSrcId;
	@SerializedName(TermConstants.RS_TARGET_ORG_SRC)
	public String targetOrgSource;
	@SerializedName(TermConstants.RS_TYPE)
	public String type;
	@SerializedName(TermConstants.RS_PROPS)
	public Map<String, Object> properties;

	public ImportFacetTermRelationship(String targetSrcId, String targetSource, String type,
			TermConstants.IdType idType) {
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
