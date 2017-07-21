package de.julielab.neo4j.plugins.datarepresentation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.gson.annotations.SerializedName;

import de.julielab.neo4j.plugins.constants.semedico.TermConstants;

public class ImportTerm {

	public ImportTerm(String preferredName, ConceptCoordinates coordinates) {
		prefName = preferredName;
		this.coordinates = coordinates;
	}

	public ImportTerm(String preferredName, String description, ConceptCoordinates coordinates) {
		this(preferredName, coordinates);
		this.descriptions = Arrays.asList(description);
	}

	public ImportTerm(String preferredName, List<String> synonyms, ConceptCoordinates coordinates) {
		this(preferredName, coordinates);
		this.synonyms = synonyms;
	}
	
	public ImportTerm(String preferredName, List<String> synonyms, String description, ConceptCoordinates coordinates) {
		this(preferredName, synonyms, coordinates);
		this.descriptions = Arrays.asList(description);
	}

	public ImportTerm(String preferredName, List<String> synonyms, String description, ConceptCoordinates coordinates,
			ConceptCoordinates parentCoordinates) {
		this(preferredName, synonyms, description, coordinates);
		this.parentCoordinates = Arrays.asList(parentCoordinates);
	}

	public ImportTerm(String preferredName, ConceptCoordinates coordinates, ConceptCoordinates parentCoordinates) {
		this(preferredName, coordinates);
		this.parentCoordinates = Arrays.asList(parentCoordinates);
	}

	public ImportTerm(String preferredName, ConceptCoordinates coordinates,
			List<ConceptCoordinates> parentCoordinates) {
		this(preferredName, coordinates);
		this.parentCoordinates = parentCoordinates;
	}

	/**
	 * Constructor for aggregate terms.
	 * 
	 * @param elementCoords
	 *            Coordinates of the elements to be aggregated.
	 * @param copyProperties
	 *            The properties that should be copied from elements to the
	 *            aggregates.
	 */
	public ImportTerm(List<TermCoordinates> elementCoords, List<String> copyProperties) {
		this.elementCoordinates = elementCoords;
		this.copyProperties = copyProperties;
		this.aggregate = true;
	}

	public ImportTerm(String sourceId) {
		this.sourceId = sourceId;
	}

	@SerializedName(TermConstants.PROP_PREF_NAME)
	public String prefName;
	@SerializedName(TermConstants.PROP_DESCRIPTIONS)
	public List<String> descriptions;
	@SerializedName(TermConstants.PROP_SYNONYMS)
	public List<String> synonyms;
	@SerializedName(TermConstants.PROP_WRITING_VARIANTS)
	public List<String> writingVariants;
	@SerializedName(TermConstants.PROP_COORDINATES)
	public ConceptCoordinates coordinates;
	@SerializedName(TermConstants.PROP_SRC_IDS)
	@Deprecated
	public String sourceId;
	@SerializedName(TermConstants.PROP_UNIQUE_SRC_ID)
	@Deprecated
	public boolean uniqueSourceId;
	@SerializedName(TermConstants.PROP_SOURCES)
	/**
	 * An identifier of the source this term came from, e.g. "MESH".
	 */
	@Deprecated
	public String source;
	@SerializedName(TermConstants.PROP_ORG_ID)
	@Deprecated
	public String originalId;
	@SerializedName(TermConstants.PROP_ORG_SRC)
	/**
	 * An identifier of the original source this term came from, e.g. "MESH".
	 */
	@Deprecated
	public String originalSource;
	@SerializedName(TermConstants.PARENT_SRC_IDS)
	/**
	 * @deprecated use parent coordinates instead.
	 */
	@Deprecated
	public List<String> parentSrcIds;
	@SerializedName(TermConstants.PARENT_SOURCES)
	public List<String> parentSources;
	@SerializedName(TermConstants.PARENT_COORDINATES)
	public List<ConceptCoordinates> parentCoordinates;
	@SerializedName(TermConstants.RELATIONSHIPS)
	public List<ImportFacetTermRelationship> relationships;
	@SerializedName(TermConstants.PROP_GENERAL_LABELS)
	public List<String> generalLabels;
	@SerializedName(TermConstants.PROP_DISPLAY_NAME)
	public String displayName;
	@SerializedName(TermConstants.PROP_QUALIFIERS)
	public List<String> qualifiers;

	// ------------- for aggregates -----------------
	@SerializedName(TermConstants.AGGREGATE)
	public Boolean aggregate;
	@SerializedName(TermConstants.ELEMENT_SRC_IDS)
	@Deprecated
	public List<String> elementSrcIds;
	@SerializedName(TermConstants.ELEMENT_SOURCES)
	@Deprecated
	public List<String> elementSources;
	@SerializedName(TermConstants.ELEMENT_COORDINATES)
	public List<TermCoordinates> elementCoordinates;
	@SerializedName(TermConstants.PROP_COPY_PROPERTIES)
	public List<String> copyProperties;
	@SerializedName(TermConstants.AGGREGATE_SOURCES)
	@Deprecated
	public List<String> aggregateSources;
	@SerializedName(TermConstants.AGGREGATE_INCLUDE_IN_HIERARCHY)
	public Boolean aggregateIncludeInHierarchy;

	public void addRelationship(ImportFacetTermRelationship relationship) {
		if (null == relationships)
			relationships = new ArrayList<>();
		relationships.add(relationship);
	}

	@Override
	public String toString() {
		return "ImportTerm [prefName=" + prefName + ", sourceId=" + sourceId + "]";
	}

	public void addGeneralLabel(String... labels) {
		if (null == generalLabels)
			generalLabels = new ArrayList<>();
		try {
			for (int i = 0; i < labels.length; i++) {
				String label = labels[i];
				generalLabels.add(label);
			}
		} catch (java.lang.UnsupportedOperationException e) {
			generalLabels = new ArrayList<>(generalLabels);
			addGeneralLabel(labels);
		}
	}

	public void addElementSourceId(String sourceId) {
		if (null == elementSrcIds)
			elementSrcIds = new ArrayList<>();
		elementSrcIds.add(sourceId);
	}

	public void addCopyProperty(String property) {
		if (null == copyProperties)
			copyProperties = new ArrayList<>();
		copyProperties.add(property);
	}

	public void addQualifier(String speciesQualifier) {
		if (null == qualifiers)
			qualifiers = new ArrayList<>();
		qualifiers.add(speciesQualifier);
	}

	public void removeGeneralLabel(String... labels) {
		if (null == generalLabels || generalLabels.isEmpty())
			return;
		Set<String> removeLabels = new HashSet<>(labels.length);
		for (int i = 0; i < labels.length; i++) {
			String label = labels[i];
			removeLabels.add(label);
		}
		Iterator<String> existingLabels = generalLabels.iterator();
		while (existingLabels.hasNext()) {
			String existingLabel = (String) existingLabels.next();
			if (removeLabels.contains(existingLabel))
				existingLabels.remove();
		}

	}

	public void addParentSrcId(String parentSrcId) {
		if (null == parentSrcIds)
			parentSrcIds = new ArrayList<>();
		parentSrcIds.add(parentSrcId);
	}

}
