package de.julielab.neo4j.plugins.datarepresentation;

import com.fasterxml.jackson.annotation.JsonProperty;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetGroupConstants;

import java.util.List;

public class ImportFacetGroup {
	public ImportFacetGroup(String name, int position, List<String> generalLabels) {
		this.name = name;
		this.position = position;
		this.labels = generalLabels;
	}

	public ImportFacetGroup(String name) {
		this.name = name;
	}

	@JsonProperty(FacetGroupConstants.PROP_NAME)
	public String name;
	@JsonProperty(FacetGroupConstants.PROP_POSITION)
	public int position;
	@JsonProperty(FacetGroupConstants.PROP_LABELS)
	public List<String> labels;
	@JsonProperty(FacetGroupConstants.PROP_TYPE)
	public String type;

}
