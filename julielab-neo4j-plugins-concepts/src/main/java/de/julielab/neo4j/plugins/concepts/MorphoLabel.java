package de.julielab.neo4j.plugins.concepts;

import org.neo4j.graphdb.Label;

/**
 * Labels for nodes representing lexico-morphological variations of concepts.
 */
public enum MorphoLabel implements Label {
    WRITING_VARIANTS, ACRONYMS, WRITING_VARIANT, ACRONYM
}
