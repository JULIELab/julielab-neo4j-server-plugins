package de.julielab.neo4j.plugins.constants.semedico;

/**
 * Property constants for relationships that represent semantic relations extracted from text.
 */
public class SemanticRelationConstants {
    /**
     * The total amount of relations between the end nodes of the relationship. This should be the sum of the
     * numbers stored in the {@link #PROP_COUNTS} array property.
     */
    public static final String PROP_TOTAL_COUNT = "totalCount";
    /**
     * A sorted array of document IDs from which relations were extracted. Parallel to {@link #PROP_COUNTS} where
     * the number of occurrences of this relation is stored.
     */
    public static final String PROP_DOC_IDS = "doc_ids";
    /**
     * An array with integer counts parallel to {@link #PROP_DOC_IDS}. The ith array position denotes the number
     * of occurrences of the relation represented by this respective Neo4j relationship in the document identified
     * by the ith element of {@link #PROP_DOC_IDS}.
     */
    public static final String PROP_COUNTS = "counts";
    /**
     * The names of databases listing this relation.
     */
    public static final String PROP_DB_NAMES = "db_names";
    /**
     * The methods by which the relationship in a database was determined.
     */
    public static final String PROP_METHODS = "db_methods";
}
