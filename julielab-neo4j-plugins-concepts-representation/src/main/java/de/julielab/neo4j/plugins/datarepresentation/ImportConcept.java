package de.julielab.neo4j.plugins.datarepresentation;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants;

import java.util.*;

public class ImportConcept {

    @JsonProperty(ConceptConstants.PROP_PREF_NAME)
    public String prefName;
    @JsonProperty(ConceptConstants.PROP_DESCRIPTIONS)
    public List<String> descriptions = Collections.emptyList();
    @JsonProperty(ConceptConstants.PROP_SYNONYMS)
    public List<String> synonyms = Collections.emptyList();
    @JsonProperty(ConceptConstants.PROP_WRITING_VARIANTS)
    public List<String> writingVariants = Collections.emptyList();
    @JsonProperty(ConceptConstants.COORDINATES)
    public ConceptCoordinates coordinates;
    @JsonProperty(ConceptConstants.PARENT_COORDINATES)
    public List<ConceptCoordinates> parentCoordinates = Collections.emptyList();
    @JsonProperty(ConceptConstants.RELATIONSHIPS)
    public List<ImportConceptRelationship> relationships = Collections.emptyList();
    @JsonProperty(ConceptConstants.PROP_LABELS)
    public List<String> generalLabels = Collections.emptyList();
    @JsonProperty(ConceptConstants.PROP_DISPLAY_NAME)
    public String displayName;
    @JsonProperty(ConceptConstants.PROP_QUALIFIERS)
    public List<String> qualifiers = Collections.emptyList();
    // ------------- for aggregates -----------------
    @JsonProperty(ConceptConstants.AGGREGATE)
    public boolean aggregate;
    @JsonProperty(ConceptConstants.ELEMENT_COORDINATES)
    public List<ConceptCoordinates> elementCoordinates = Collections.emptyList();
    @JsonProperty(ConceptConstants.PROP_COPY_PROPERTIES)
    public List<String> copyProperties = Collections.emptyList();
    @JsonProperty(ConceptConstants.AGGREGATE_INCLUDE_IN_HIERARCHY)
    public boolean aggregateIncludeInHierarchy;

    /**
     * This map may contain specific properties required during concept creation. It is not meant to be imported
     * into the database.
     */
    @JsonIgnore
    public Map<String, Object> auxProperties;

    public ImportConcept(String preferredName, ConceptCoordinates coordinates) {
        prefName = preferredName;
        this.coordinates = coordinates;
    }

    public ImportConcept(String preferredName, String description, ConceptCoordinates coordinates) {
        this(preferredName, coordinates);
        if (description != null)
            this.descriptions = Arrays.asList(description);
    }

    public ImportConcept(String preferredName, List<String> synonyms, ConceptCoordinates coordinates) {
        this(preferredName, coordinates);
        this.synonyms = synonyms;
    }

    public ImportConcept(String preferredName, List<String> synonyms, String description,
                         ConceptCoordinates coordinates) {
        this(preferredName, synonyms, coordinates);
        if (description != null)
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
        if (parentCoordinates == null)
            throw new IllegalArgumentException("The passed parent coordinates are null which is invalid (pass an empty list instead).");
        if (parentCoordinates != null)
            this.parentCoordinates = Arrays.asList(parentCoordinates);
    }

    public ImportConcept(String preferredName, List<String> synonyms, List<String> descriptions,
                         ConceptCoordinates coordinates, List<ConceptCoordinates> parentCoordinates) {
        this(preferredName, synonyms, descriptions, coordinates);
        if (parentCoordinates == null)
            throw new IllegalArgumentException("The passed parent coordinates are null which is invalid (pass an empty list instead).");
        if (parentCoordinates != null)
            this.parentCoordinates = parentCoordinates;
    }

    public ImportConcept(String preferredName, ConceptCoordinates coordinates, ConceptCoordinates parentCoordinates) {
        this(preferredName, coordinates);
        if (parentCoordinates == null)
            throw new IllegalArgumentException("The passed parent coordinates are null which is invalid (pass an empty list instead).");
        if (parentCoordinates != null)
            this.parentCoordinates = Arrays.asList(parentCoordinates);
    }

    public ImportConcept(String preferredName, ConceptCoordinates coordinates,
                         List<ConceptCoordinates> parentCoordinates) {
        this(preferredName, coordinates);
        if (parentCoordinates == null)
            throw new IllegalArgumentException("The passed parent coordinates are null which is invalid (pass an empty list instead).");
        if (parentCoordinates != null)
            this.parentCoordinates = parentCoordinates;
    }

    /**
     * Constructor for aggregate terms.
     *
     * @param elementCoords  Coordinates of the elements to be aggregated.
     * @param copyProperties The properties that should be copied from elements to the
     *                       aggregates.
     */
    public ImportConcept(List<ConceptCoordinates> elementCoords, List<String> copyProperties) {
        this.elementCoordinates = elementCoords;
        this.copyProperties = copyProperties;
        this.aggregate = true;
    }

    public ImportConcept(ConceptCoordinates conceptCoordinates) {
        coordinates = conceptCoordinates;
    }

    public ImportConcept() {
    }

    public void addRelationship(ImportConceptRelationship relationship) {
        if (relationships.isEmpty())
            relationships = new ArrayList<>();
        relationships.add(relationship);
    }

    public void addGeneralLabel(String... labels) {
        if (generalLabels.isEmpty())
            generalLabels = new ArrayList<>();
        try {
            for (String label : labels) {
                generalLabels.add(label);
            }
        } catch (java.lang.UnsupportedOperationException e) {
            generalLabels = new ArrayList<>(generalLabels);
            addGeneralLabel(labels);
        }
    }

    public void addCopyProperty(String property) {
        if (copyProperties.isEmpty())
            copyProperties = new ArrayList<>();
        copyProperties.add(property);
    }

    public void addQualifier(String speciesQualifier) {
        if (qualifiers.isEmpty())
            qualifiers = new ArrayList<>();
        qualifiers.add(speciesQualifier);
    }

    public void removeGeneralLabel(String... labels) {
        if (generalLabels.isEmpty())
            return;
        Set<String> removeLabels = new HashSet<>(labels.length);
        for (String label : labels) {
            removeLabels.add(label);
        }
        generalLabels.removeIf(removeLabels::contains);

    }

    public void addParent(ConceptCoordinates parentCoordinates) {
        if (this.parentCoordinates.isEmpty())
            this.parentCoordinates = new ArrayList<>();
        this.parentCoordinates.add(parentCoordinates);
    }

    public void addParentIfNotExists(ConceptCoordinates parentCoordinates) {
        if (this.parentCoordinates.isEmpty() || !this.parentCoordinates.contains(parentCoordinates))
            addParent(parentCoordinates);
    }

    public boolean hasParents() {
        return !parentCoordinates.isEmpty();
    }


}
