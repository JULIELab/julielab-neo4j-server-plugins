package de.julielab.neo4j.plugins.concepts;

import de.julielab.neo4j.plugins.auxiliaries.semedico.CoordinatesSet;
import de.julielab.neo4j.plugins.datarepresentation.ConceptCoordinates;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

import java.util.HashSet;
import java.util.Set;

public class InsertionReport {
    /**
     * A temporary storage to keep track over relationships created during a single
     * concept insertion batch. It is used for deconceptination whether existing
     * relationships between two concepts must be checked or not. This is required
     * for the case that a concept is inserted multiple times in a single insertion
     * batch. Then, the "concept already existing" method does not work anymore.
     */
    public final Set<String> createdRelationshipsCache = new HashSet<>();
    /**
     * The concept nodes that already existed before they should have been inserted
     * again (duplicate detection). This is used to deconceptine whether a check
     * about already existing relationships between two nodes is necessary. If at
     * least one of two concepts between which a relationships should be created did
     * not exist before, no check is necessary: A concept that did not exist could
     * not have had any relationships.
     */
    public final Set<Node> existingConcepts = new HashSet<>();
    /**
     * The source IDs of concepts that have been omitted from the data for -
     * hopefully - good reasons. The first (and perhaps only) use case were
     * aggregates which had a single elements and were thus omitted but should also
     * be included into the concept hierarchy and have other concepts referring to
     * them as a parent. This set serves as a lookup in this case so we know there
     * is not an error.
     */
    public final Set<String> omittedConcepts = new HashSet<>();
    /**
     * The coordinates of all concepts that are being imported. This information is
     * used by the relationship creation method to know if a parent is included in
     * the imported data or not.
     */
    public final CoordinatesSet importedCoordinates = new CoordinatesSet();
    public int numRelationships = 0;
    public int numConcepts = 0;

    public void addCreatedRelationship(Node source, Node target, RelationshipType type) {
        createdRelationshipsCache.add(getRelationshipIdentifier(source, target, type));
    }

    public void addExistingConcept(Node concept) {
        existingConcepts.add(concept);
    }

    private String getRelationshipIdentifier(Node source, Node target, RelationshipType type) {
        return source.getId() + type.name() + target.getId();
    }

    public boolean relationshipAlreadyWasCreated(Node source, Node target, RelationshipType type) {
        return createdRelationshipsCache.contains(getRelationshipIdentifier(source, target, type));
    }

    public void addImportedCoordinates(ConceptCoordinates coordinates) {
        importedCoordinates.add(coordinates);
    }
}