package de.julielab.neo4j.plugins.datarepresentation;

import static de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants.FACET_GROUP;
import static de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants.PROP_SOURCE_TYPE;
import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_NAME;

import java.util.List;

import com.google.gson.annotations.SerializedName;

import de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants;

public class ImportFacet {
	public ImportFacet(String name, String shortName, String sourceType, ImportFacetGroup facetGroup) {
		this.name = name;
		this.shortName = shortName;
		this.sourceType = sourceType;
		this.facetGroup = facetGroup;
	}

	public ImportFacet(String id) {
		this.id = id;
	}

	/**
	 * The database-internal ID which is automatically created. This field should
	 * only be set to refer to an already created facet node.
	 */
	@SerializedName(FacetConstants.PROP_ID)
	public String id;
	@SerializedName(PROP_NAME)
	public String name;
	@SerializedName(FacetConstants.PROP_SHORT_NAME)
	public String shortName;
	@SerializedName(PROP_SOURCE_TYPE)
	public String sourceType;
	@SerializedName(FACET_GROUP)
	public ImportFacetGroup facetGroup;
	@SerializedName(FacetConstants.NO_FACET)
	public boolean noFacet;
	@SerializedName(FacetConstants.AGGREGATION_LABELS)
	public List<String> aggregationLabels;
	@SerializedName(FacetConstants.PROP_AGGREGATION_FIELDS)
	public List<String> aggregationFields;
	/**
	 * A JSON object encoding arbitrary properties which are then applied directly
	 * to the resulting facet node in the Neo4j database.
	 */
	@SerializedName(FacetConstants.PROP_PROPERTIES)
	public String facetProperties;

}
