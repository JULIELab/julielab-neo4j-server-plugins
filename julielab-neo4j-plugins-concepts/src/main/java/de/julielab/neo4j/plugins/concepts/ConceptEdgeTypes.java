package de.julielab.neo4j.plugins.concepts;

import org.neo4j.graphdb.RelationshipType;

public enum ConceptEdgeTypes implements RelationshipType {
    /**
     * Relationship type for connecting aggregate classes with their element
     * concepts.
     */
    HAS_ELEMENT, HAS_ROOT_CONCEPT,
    /**
     * Relationship type to express that two concepts seem to be identical regarding
     * preferred label and synonyms.
     */
    HAS_SAME_NAMES, IS_BROADER_THAN,
    /**
     * A concept mapping that expresses some similarity between to concepts, e.g.
     * 'equal' or 'related'. The actual type of relatedness should be added as a
     * property to the relationship.
     */
    IS_MAPPED_TO,
    /**
     * Concept writing variants and their frequencies are stored in a special kind
     * of node, connected to the respective concept with this relationship type.
     */
    HAS_VARIANTS, HAS_ACRONYMS
}