package de.julielab.neo4j.plugins.datarepresentation.constants;

public class ConceptConstants extends NodeConstants {


    /**
     * The name by which a term should be references preferably. This will be
     * the standard name in the literature or the most conventionalized name or
     * similar.
     */
    public static final String PROP_PREF_NAME = "preferredName";
    /**
     * The name which should be displayed in Semedico, e.g. for context
     * disambiguation. This is used for genes, for example, where the organism
     * is appended to the preferred name in order to distinguish between genes
     * with the same symbol in different organisms.
     */
    public static final String PROP_DISPLAY_NAME = "displayName";
    /**
     * Name other than the preferred name that mean the same thing.
     */
    public static final String PROP_SYNONYMS = "synonyms";
    /**
     * Writing variants of the preferred name or any of its synonyms that are
     * not any of these names themselves but writing variants (with or without
     * dashes, slashes, white spaces etc.).
     */
    public static final String PROP_WRITING_VARIANTS = "writingVariants";
    public static final String PROP_DESCRIPTIONS = "descriptions";
    public static final String PROP_FACETS = "facets";
    public static final String PROP_ORG_ID = "originalId";
    public static final String PROP_ORG_SRC = "originalSource";
    public static final String PROP_SRC_IDS = "sourceIds";
    public static final String PROP_SOURCES = "sources";
    public static final String PROP_UNIQUE_SRC_ID = "uniqueSourceId";
    /**
     * Property that stores for database terms, which facets define children for
     * them. This is necessary because one facet can just be a 'view' of another
     * facet, thus removing some of the original terms for a narrower focus.
     * Then it could be that in one facet a particular term has less children
     * that in another - e.g. the original - facet.
     */
    public static final String PROP_CHILDREN_IN_FACETS = "childrenInFacets";

    /**
     * Field in the input data format.  This information is used to identify the correct parent node.
     */
    public static final String PARENT_COORDINATES = "parentCoordinates";

    public static final String INDEX_NAME = "termIndex";

    public static final String SET_INDEX_NAME = "termSetIndex";
    /**
     * Field in the input data format.
     */
    public static final String RELATIONSHIPS = "relationships";
    /**
     * Field in the input data format for explicit term relations. Coordinates of
     * the concept that is the target of a relationship.
     */
    public static final String RS_TARGET_COORDINATES = "targetCoordinates";
    /**
     * Field in the input data format for explicit term relations. Specifies the
     * type of the relationship.
     */
    public static final String RS_TYPE = "type";
    /**
     * Field in the input data format for explicit term relations. Specifies a
     * number of properties to add to the relationship.
     */
    public static final String RS_PROPS = "properties";
    /**
     * Field in the input data format. Indicates whether a term is actually an
     * aggregate term, representing a whole set of real terms. This could be
     * done for terms with equal names to create a single representative with
     * all necessary information to come back to the original terms, for
     * instance.
     */
    public static final String AGGREGATE = "aggregate";

    /**
     * Applies to aggregate terms which are explicitly imported. If set to
     * <tt>true</tt>, they may be nodes embedded into a hierarchy and thus be
     * 'broader than' other terms and be root terms of facets.
     */
    public static final String AGGREGATE_INCLUDE_IN_HIERARCHY = "aggregateIncludeInHierarchy";
    /**
     * Field in the input data format of aggregate terms. Specifies the term
     * source coordinates - source ID and source - of element terms.
     */
    public static final String ELEMENT_COORDINATES = "elementCoordinates";
    /**
     * Property of aggregate terms. Specifies an array of property names to be
     * copied from element terms.
     */
    public static final String PROP_COPY_PROPERTIES = "copyProperties";
    /**
     * Property for aggregates. The value of the property exposes information
     * about the exact set of mapping types allowed to create the aggregate.
     */
    public static final String PROP_MAPPING_TYPE = ConceptRelationConstants.PROP_MAPPING_TYPE;

    /**
     * A map which holds additional properties to be added to the Neo4j nodes.
     */
    public static final String ADDITIONAL_PROPERTIES = "additional_properties";

    public static final String COORDINATES = "coordinates";

    public static final String ADDITIONAL_COORDINATES = "additional_coordinates";

    /**
     * Whether a ImportConcept can be connected to its ImportFacet via a HAS_ROOT_CONCEPT relationship. Useful to switch off when importing concepts with one facet that would rather belong to another one but that is not imported at all or later.
     */
    public static final String ELIGIBLE_FOR_FACET_ROOT = "eligible_for_facet_root";
}
