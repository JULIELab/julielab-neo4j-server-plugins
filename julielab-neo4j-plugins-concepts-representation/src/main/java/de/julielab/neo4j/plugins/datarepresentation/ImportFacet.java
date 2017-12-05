package de.julielab.neo4j.plugins.datarepresentation;

import static de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants.FACET_GROUP;
import static de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants.PROP_SOURCE_TYPE;
import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_LABELS;
import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_NAME;

import java.util.ArrayList;
import java.util.Collection;

import com.google.gson.annotations.SerializedName;

import de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants;

public class ImportFacet {
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
	public ImportFacet(ImportFacetGroup facetGroup, String customId, String name, String shortName, String sourceType, Collection<String> labels,
			 boolean noFacet) {
		this.facetGroup = facetGroup;
		this.name = name;
		this.customId = customId;
		this.shortName = shortName;
		this.sourceType = sourceType;
		this.labels = labels;
		this.noFacet = noFacet;
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
	 */
	public ImportFacet(ImportFacetGroup facetGroup, String customId, String name, String shortName, String sourceType) {
		this(facetGroup, customId, name, shortName, sourceType, null, false);
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
	 * The database-internal ID which is automatically created. This field should
	 * only be set to refer to an already created facet node.
	 */
	@SerializedName(FacetConstants.PROP_ID)
	public String id;
	/**
	 * @see FacetConstants#PROP_CUSTOM_ID
	 */
	@SerializedName(FacetConstants.PROP_CUSTOM_ID)
	public String customId;
	@SerializedName(PROP_NAME)
	public String name;
	@SerializedName(FacetConstants.PROP_SHORT_NAME)
	public String shortName;
	@SerializedName(PROP_SOURCE_TYPE)
	public String sourceType;
	@SerializedName(FACET_GROUP)
	public ImportFacetGroup facetGroup;
	/**
	 * @see FacetConstants#NO_FACET
	 */
	@SerializedName(FacetConstants.NO_FACET)
	public boolean noFacet;
	@SerializedName(PROP_LABELS)
	public Collection<String> labels;

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
}
