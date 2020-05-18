package de.julielab.neo4j.plugins.concepts;

import org.neo4j.graphdb.Label;

public enum ConceptLabel implements Label {
    /**
     * Label to indicate a node is not an actual concept but an aggregate concept.
     * Such concepts have {@link ConceptEdgeTypes#HAS_ELEMENT} relationships to concepts,
     * deconceptining the set of concepts the aggregate represents.
     */
    AGGREGATE,
    /**
     * A particular type of {@link #AGGREGATE} node.
     */
    AGGREGATE_EQUAL_NAMES,
    /**
     * Label for nodes that are referenced by at least one other concept in imported
     * data, but are not included in the imported data themselves. Such concepts
     * know their source ID (given by the reference of another concept) and will be
     * made un-HOLLOW as soon at a concept with this source ID occurs in imported
     * data.
     */
    HOLLOW, CONCEPT
}