package de.julielab.neo4j.plugins.datarepresentation;

import static de.julielab.neo4j.plugins.constants.semedico.FacetConstants.FACET_GROUP;
import static de.julielab.neo4j.plugins.constants.semedico.FacetConstants.PROP_CSS_ID;
import static de.julielab.neo4j.plugins.constants.semedico.FacetConstants.PROP_FILTER_FIELD_NAMES;
import static de.julielab.neo4j.plugins.constants.semedico.FacetConstants.PROP_POSITION;
import static de.julielab.neo4j.plugins.constants.semedico.FacetConstants.PROP_SEARCH_FIELD_NAMES;
import static de.julielab.neo4j.plugins.constants.semedico.FacetConstants.PROP_SOURCE_TYPE;
import static de.julielab.neo4j.plugins.constants.semedico.NodeConstants.PROP_GENERAL_LABELS;
import static de.julielab.neo4j.plugins.constants.semedico.NodeConstants.PROP_NAME;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.annotations.SerializedName;

import de.julielab.neo4j.plugins.constants.semedico.FacetConstants;

public class ImportFacet {
	public ImportFacet(String name, String cssId, String sourceType, List<String> searchFieldNames,
			List<String> filterFieldNames, int position, List<String> generalLabels, ImportFacetGroup facetGroup) {
		this.name = name;
		this.cssId = cssId;
		this.sourceType = sourceType;
		this.searchFieldNames = searchFieldNames;
		this.filterFieldNames = filterFieldNames;
		this.position = position;
		this.generalLabels = generalLabels;
		this.facetGroup = facetGroup;
	}

	public ImportFacet(String id) {
		this.id = id;
	}

	@SerializedName(PROP_NAME)
	public String name;
	@SerializedName(FacetConstants.PROP_SHORT_NAME)
	public String shortName;
	@SerializedName(PROP_CSS_ID)
	public String cssId;
	@SerializedName(PROP_SOURCE_TYPE)
	public String sourceType;
	@SerializedName(PROP_SEARCH_FIELD_NAMES)
	public List<String> searchFieldNames;
	@SerializedName(PROP_FILTER_FIELD_NAMES)
	public List<String> filterFieldNames;
	@SerializedName(PROP_POSITION)
	int position;
	@SerializedName(PROP_GENERAL_LABELS)
	public List<String> generalLabels;
	@SerializedName(FACET_GROUP)
	public ImportFacetGroup facetGroup;
	@SerializedName(FacetConstants.PROP_SOURCE_NAME)
	public String sourceName;
	@SerializedName(FacetConstants.PROP_UNIQUE_LABELS)
	public List<String> uniqueLabels;
	@SerializedName(FacetConstants.PROP_ID)
	public String id;
	@SerializedName(FacetConstants.NO_FACET)
	public boolean noFacet;
	@SerializedName(FacetConstants.AGGREGATION_LABELS)
	public List<String> aggregationLabels;
	@SerializedName(FacetConstants.PROP_AGGREGATION_FIELDS)
	public ArrayList<String> aggregationFields;
	/**
	 * A JSON object encoding arbitrary properties which are then applied directy to the resulting facet node in the
	 * Neo4j database.
	 */
	@SerializedName(FacetConstants.PROP_PROPERTIES)
	public String facetProperties;
	@SerializedName(FacetConstants.PROP_INDUCING_TERM)
	public String incucingTerm;

	//
	// volatile static Gson gson = new Gson();
	//
	// public String toJson() {
	// return gson.toJson(this);
	// }
	public void addUniqueLabel(String label) {
		if (uniqueLabels == null)
			uniqueLabels = new ArrayList<>();
		uniqueLabels.add(label);
	}

	public void addGeneralLabel(String label) {
		if (generalLabels == null)
			generalLabels = new ArrayList<>();
		generalLabels.add(label);
	}

}
