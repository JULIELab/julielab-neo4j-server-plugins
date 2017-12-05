package de.julielab.neo4j.plugins.datarepresentation;

import java.util.List;

import com.google.gson.annotations.SerializedName;

import de.julielab.neo4j.plugins.datarepresentation.constants.FacetGroupConstants;

public class ImportFacetGroup {
	public ImportFacetGroup(String name, int position, List<String> generalLabels) {
		this.name = name;
		this.position = position;
		this.generalLabels = generalLabels;
	}

	public ImportFacetGroup(String name) {
		this.name = name;
	}

	@SerializedName(FacetGroupConstants.PROP_NAME)
	public String name;
	@SerializedName(FacetGroupConstants.PROP_POSITION)
	public int position;
	@SerializedName(FacetGroupConstants.PROP_LABELS)
	public List<String> generalLabels;
	@SerializedName(FacetGroupConstants.PROP_TYPE)
	public String type;

}
