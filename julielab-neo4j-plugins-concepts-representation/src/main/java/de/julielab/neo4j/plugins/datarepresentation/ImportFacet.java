package de.julielab.neo4j.plugins.datarepresentation;

import com.fasterxml.jackson.annotation.JsonProperty;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants;

import java.util.ArrayList;
import java.util.Collection;

import static de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants.FACET_GROUP;
import static de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants.PROP_SOURCE_TYPE;
import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_LABELS;
import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_NAME;

public class ImportFacet {
	/**
	 * @see FacetConstants#PROP_CUSTOM_ID
	 */
	@JsonProperty(FacetConstants.PROP_CUSTOM_ID)
	protected String customId;

	@JsonProperty(FACET_GROUP)
	protected ImportFacetGroup facetGroup;

	/**
	 * The database-internal ID which is automatically created. This field should
	 * only be set to refer to an already created facet node.
	 */
	@JsonProperty(FacetConstants.PROP_ID)
	protected String id;

	@JsonProperty(PROP_LABELS)
	protected Collection<String> labels;
	@JsonProperty(PROP_NAME)
	protected String name;
	/**
	 * @see FacetConstants#NO_FACET
	 */
	@JsonProperty(FacetConstants.NO_FACET)
	protected boolean noFacet;
	@JsonProperty(FacetConstants.PROP_SHORT_NAME)
	protected String shortName;
	@JsonProperty(PROP_SOURCE_TYPE)
	protected String sourceType;
	/**
	 * Constructor eligible for creating new facets in the database.
	 * 
	 * @param facetGroup
	 *            The group of facets to join this facet to.
	 * @param customId
	 *            A custom ID that can be freely chosen. The only restriction is
	 *            that it has to be unique within the database.
	 * @param name
	 *            A human-readable name for the facet. Does not need to be unique.
	 * @param shortName
	 *            A short name, optional.
	 * @param sourceType
	 *            The type of the facet contents (hierarchical concepts, flat
	 *            concepts or arbitrary strings)
	 */
	public ImportFacet(ImportFacetGroup facetGroup, String customId, String name, String shortName, String sourceType) {
		this(facetGroup, customId, name, shortName, sourceType, null, false);
	}
	/**
	 * Constructor eligible for creating new facets in the database.
	 * 
	 * @param facetGroup
	 *            The group of facets to join this facet to.
	 * @param customId
	 *            A custom ID that can be freely chosen. The only restriction is
	 *            that it has to be unique within the database.
	 * @param name
	 *            A human-readable name for the facet. Does not need to be unique.
	 * @param shortName
	 *            A short name, optional.
	 * @param sourceType
	 *            The type of the facet contents (hierarchical concepts, flat
	 *            concepts or arbitrary strings)
	 * @param noFacet
	 *            Whether or not this facet should go to the default facet group or
	 *            into the separate "no-facet" section of the database. Used for
	 *            concepts that should stay hidden most of the time but are used
	 *            nontheless.
	 */
	public ImportFacet(ImportFacetGroup facetGroup, String customId, String name, String shortName, String sourceType,
			Collection<String> labels, boolean noFacet) {
		this.facetGroup = facetGroup;
		this.name = name;
		this.customId = customId;
		this.shortName = shortName;
		this.sourceType = sourceType;
		this.labels = labels;
		this.noFacet = noFacet;
	}
	/**
	 * Constructor used to refer to an existing facet with the given database ID
	 * (NOT the custom ID).
	 * 
	 * @param id
	 *            The automatically created facet ID. Is returned from the database
	 *            upon facet creation.
	 */
	public ImportFacet(String id) {
		this.id = id;
	}

	/**
	 * Add a label for the resulting Neo4j node.
	 * 
	 * @param label
	 *            An arbitrary label.
	 */
	public void addLabel(String label) {
		if (labels == null)
			labels = new ArrayList<>();
		labels.add(label);
	}

	public String getCustomId() {
		return customId;
	}

	public ImportFacetGroup getFacetGroup() {
		return facetGroup;
	}

	public String getId() {
		return id;
	}

	public Collection<String> getLabels() {
		return labels;
	}

	public String getName() {
		return name;
	}

	public String getShortName() {
		return shortName;
	}

	public String getSourceType() {
		return sourceType;
	}

	public boolean isNoFacet() {
		return noFacet;
	}

	public void setCustomId(String customId) {
		this.customId = customId;
	}

	public void setFacetGroup(ImportFacetGroup facetGroup) {
		this.facetGroup = facetGroup;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setLabels(Collection<String> labels) {
		this.labels = labels;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setNoFacet(boolean noFacet) {
		this.noFacet = noFacet;
	}

	public void setShortName(String shortName) {
		this.shortName = shortName;
	}

	public void setSourceType(String sourceType) {
		this.sourceType = sourceType;
	}
}
