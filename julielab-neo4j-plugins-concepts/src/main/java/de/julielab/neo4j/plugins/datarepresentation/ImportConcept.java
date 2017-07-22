package de.julielab.neo4j.plugins.datarepresentation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.gson.annotations.SerializedName;

import de.julielab.neo4j.plugins.constants.semedico.ConceptConstants;

public class ImportConcept {

	public ImportConcept(String preferredName, ConceptCoordinates coordinates) {
		prefName = preferredName;
		this.coordinates = coordinates;
	}

	public ImportConcept(String preferredName, String description, ConceptCoordinates coordinates) {
		this(preferredName, coordinates);
		this.descriptions = Arrays.asList(description);
	}

	public ImportConcept(String preferredName, List<String> synonyms, ConceptCoordinates coordinates) {
		this(preferredName, coordinates);
		this.synonyms = synonyms;
	}

	public ImportConcept(String preferredName, List<String> synonyms, String description,
			ConceptCoordinates coordinates) {
		this(preferredName, synonyms, coordinates);
		this.descriptions = Arrays.asList(description);
	}
	
	public ImportConcept(String preferredName, List<String> synonyms, String description,
			ConceptCoordinates coordinates, ConceptCoordinates parentCoordinates) {
		this(preferredName, synonyms, Arrays.asList(description), coordinates, Arrays.asList(parentCoordinates));
	}

	public ImportConcept(String preferredName, List<String> synonyms, String description,
			ConceptCoordinates coordinates, List<ConceptCoordinates> parentCoordinates) {
		this(preferredName, synonyms, Arrays.asList(description), coordinates, parentCoordinates);
	}

	public ImportConcept(String preferredName, List<String> synonyms, List<String> descriptions,
			ConceptCoordinates coordinates) {
		this(preferredName, synonyms, coordinates);
		this.descriptions = descriptions;
	}

	public ImportConcept(String preferredName, List<String> synonyms, List<String> descriptions,
			ConceptCoordinates coordinates, ConceptCoordinates parentCoordinates) {
		this(preferredName, synonyms, descriptions, coordinates);
		this.parentCoordinates = Arrays.asList(parentCoordinates);
	}

	public ImportConcept(String preferredName, List<String> synonyms, List<String> descriptions,
			ConceptCoordinates coordinates, List<ConceptCoordinates> parentCoordinates) {
		this(preferredName, synonyms, descriptions, coordinates);
		this.parentCoordinates = parentCoordinates;
	}

	public ImportConcept(String preferredName, ConceptCoordinates coordinates, ConceptCoordinates parentCoordinates) {
		this(preferredName, coordinates);
		this.parentCoordinates = Arrays.asList(parentCoordinates);
	}

	public ImportConcept(String preferredName, ConceptCoordinates coordinates,
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
	public ImportConcept(List<TermCoordinates> elementCoords, List<String> copyProperties) {
		this.elementCoordinates = elementCoords;
		this.copyProperties = copyProperties;
		this.aggregate = true;
	}

	public ImportConcept(ConceptCoordinates conceptCoordinates) {
		coordinates = conceptCoordinates;
	}

	public ImportConcept() {
	}

	@SerializedName(ConceptConstants.PROP_PREF_NAME)
	public String prefName;
	@SerializedName(ConceptConstants.PROP_DESCRIPTIONS)
	public List<String> descriptions;
	@SerializedName(ConceptConstants.PROP_SYNONYMS)
	public List<String> synonyms;
	@SerializedName(ConceptConstants.PROP_WRITING_VARIANTS)
	public List<String> writingVariants;
	@SerializedName(ConceptConstants.PROP_COORDINATES)
	public ConceptCoordinates coordinates;
	@SerializedName(ConceptConstants.PARENT_COORDINATES)
	public List<ConceptCoordinates> parentCoordinates;
	@SerializedName(ConceptConstants.RELATIONSHIPS)
	public List<ImportFacetTermRelationship> relationships;
	@SerializedName(ConceptConstants.PROP_GENERAL_LABELS)
	public List<String> generalLabels;
	@SerializedName(ConceptConstants.PROP_DISPLAY_NAME)
	public String displayName;
	@SerializedName(ConceptConstants.PROP_QUALIFIERS)
	public List<String> qualifiers;

	// ------------- for aggregates -----------------
	@SerializedName(ConceptConstants.AGGREGATE)
	public Boolean aggregate;
	@SerializedName(ConceptConstants.ELEMENT_SRC_IDS)
	@Deprecated
	public List<String> elementSrcIds;
	@SerializedName(ConceptConstants.ELEMENT_SOURCES)
	@Deprecated
	public List<String> elementSources;
	@SerializedName(ConceptConstants.ELEMENT_COORDINATES)
	public List<TermCoordinates> elementCoordinates;
	@SerializedName(ConceptConstants.PROP_COPY_PROPERTIES)
	public List<String> copyProperties;
	@SerializedName(ConceptConstants.AGGREGATE_SOURCES)
	@Deprecated
	public List<String> aggregateSources;
	@SerializedName(ConceptConstants.AGGREGATE_INCLUDE_IN_HIERARCHY)
	public Boolean aggregateIncludeInHierarchy;

	public void addRelationship(ImportFacetTermRelationship relationship) {
		if (null == relationships)
			relationships = new ArrayList<>();
		relationships.add(relationship);
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

	public void addParent(ConceptCoordinates parentCoordinates) {
		if (this.parentCoordinates == null)
			this.parentCoordinates = new ArrayList<>();
		this.parentCoordinates.add(parentCoordinates);
	}
	
	public boolean hasParents() {
		return parentCoordinates != null && !parentCoordinates.isEmpty();
	}
	
}
