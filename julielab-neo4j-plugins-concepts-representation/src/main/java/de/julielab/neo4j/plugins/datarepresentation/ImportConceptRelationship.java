package de.julielab.neo4j.plugins.datarepresentation;

import com.fasterxml.jackson.annotation.JsonProperty;
import de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants;

import java.util.HashMap;
import java.util.Map;

/**
 * A simple relationship class that is used in {@link ImportConcept#relationships}. Since the relationships are a
 * property of a concept, only the target concept of the relation has to be specified, as well as a string
 * identifying the type of the relationship.
 */
public class ImportConceptRelationship {

    @JsonProperty(ConceptConstants.RS_TARGET_COORDINATES)
    public ConceptCoordinates targetCoordinates;
    @JsonProperty(ConceptConstants.RS_TYPE)
    public String type;
    @JsonProperty(ConceptConstants.RS_PROPS)
    public Map<String, Object> properties;

    public ImportConceptRelationship(ConceptCoordinates targetCoordinates, String relationType) {
        this.targetCoordinates = targetCoordinates;
        this.type = relationType;
    }

    public void addProperty(String name, String value) {
        if (null == properties)
            properties = new HashMap<>();
        properties.put(name, value);
    }
}
