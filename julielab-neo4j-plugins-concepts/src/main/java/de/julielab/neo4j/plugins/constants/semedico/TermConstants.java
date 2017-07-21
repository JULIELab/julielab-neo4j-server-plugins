package de.julielab.neo4j.plugins.constants.semedico;

import de.julielab.neo4j.plugins.datarepresentation.ConceptCoordinates;

public class TermConstants extends NodeConstants {

	/**
	 * @deprecated should be merged with ConceptCoordinates enum
	 * @author faessler
	 *
	 */
	@Deprecated
	public enum IdType {
		ORIGINAL_SOURCE, SOURCE
	}

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
	 * Field in the input data format. The parent source identifier is only used
	 * for the import of terms. The parent source ID indicates which term is to
	 * be connected to which other term in the graph. Since this information is
	 * explicitly represented by relationships in Neo4j, there is no such node
	 * property in the graph.
	 * @deprecated Use {@link #PARENT_COORDINATES} instead
	 */
	@Deprecated
	public static final String PARENT_SRC_IDS = "parentSrcIds";

	/**
	 * Field in the input data format. Specifies for each - or for none - of the
	 * parent source IDs given with {@link #PARENT_SRC_IDS} the respective
	 * source. This information is used to identify the correct parent node.
	 * @deprecated Use {@link #PARENT_COORDINATES} instead
	 */
	@Deprecated
	public static final String PARENT_SOURCES = "parentSources";

	/**
	 * Field in the input data format. Specifies for each - or for none - of the
	 * parent source IDs given with {@link #PARENT_SRC_IDS} the respective
	 * source. This information is used to identify the correct parent node.
	 */
	public static final String PARENT_COORDINATES = "parentCoordinates";
	
	public static final String INDEX_NAME = "termIndex";

	public static final String SET_INDEX_NAME = "termSetIndex";
	/**
	 * Field in the input data format.
	 */
	public static final String RELATIONSHIPS = "relationships";
	/**
	 * Field in the input data format for explicit term relations. Source ID of
	 * the term that is the target of a relationship.
	 */
	public static final String RS_TARGET_SRC_ID = "targetSrcId";
	/**
	 * Field in the input data format for explicit term relations. The source of
	 * the target term the relation points to.
	 */
	public static final String RS_TARGET_SRC = "targetSource";
	/**
	 * Field in the input data format for explicit term relations. Original
	 * source ID of the term that is the target of a relationship.
	 */
	public static final String RS_TARGET_ORG_ID = "targetOriginalId";
	/**
	 * Field in the input data format for explicit term relations. The original
	 * source of the target term the relation points to.
	 */
	public static final String RS_TARGET_ORG_SRC = "targetOriginalSource";
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
	 * Field in the input data format. Indicates for aggregate terms (i.e.
	 * import data objects where the property {@link #AGGREGATE} is set to
	 * <tt>true</tt>) which term sources (NOT original source) are eligible as
	 * elements for this aggregate.
	 */
	@Deprecated
	public static final String AGGREGATE_SOURCES = "aggregateSources";
	/**
	 * Applies to aggregate terms which are explicitly imported. If set to
	 * <tt>true</tt>, they may be nodes embedded into a hierarchy and thus be
	 * 'broader than' other terms and be root terms of facets.
	 */
	public static final String AGGREGATE_INCLUDE_IN_HIERARCHY = "aggregateIncludeInHierarchy";
	/**
	 * Field in the input data format of aggregate terms. Specifies the source
	 * ID of the elements aggregated by this aggregate node.
	 * @deprecated use {@link #ELEMENT_COORDINATES} instead
	 */
	@Deprecated
	public static final String ELEMENT_SRC_IDS = "elementSrcIds";
	/**
	 * Field in the input data format of aggregate terms. Specifies for each -
	 * or none - of the elements their respective source. Null values are
	 * allowed but for each source ID there must be one source or none at all.
	 * @deprecated use {@link #ELEMENT_COORDINATES} instead
	 */
	@Deprecated
	public static final String ELEMENT_SOURCES = "elementSources";
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
	 * Property disposing for which <tt>specificType</tt> - e.g.
	 * <tt>phosphorylation</tt> or <tt>regulation</tt> - the respective term is
	 * an event term. This property is exactly the <tt>specificType</tt> that is
	 * stored in the UIMA <tt>EventMention</tt>, created by <tt>JReX</tt> for
	 * example.
	 */
	@Deprecated
	public static final String PROP_SPECIFIC_EVENT_TYPE = "specificEventType";
	/**
	 * Property determining how many arguments the event, this term is
	 * representing, can take. This is an array property, allowing multiple
	 * values since, for example, an event can be unary and binary, depending on
	 * the textual context.
	 */
	@Deprecated
	public static final String PROP_EVENT_VALENCE = "eventValence";

	/**
	 * Property for aggregates. The value of the property exposes information
	 * about the exact set of mapping types allowed to create the aggregate.
	 */
	public static final String PROP_MAPPING_TYPE = TermRelationConstants.PROP_MAPPING_TYPE;
	
	/**
	 * Property of a term coordinates object. Denotes the ID coordinate.
	 * @deprecated Use default properties on {@link ConceptCoordinates} instead
	 */
	@Deprecated
	public static final String COORD_ID = "id";
	/**
	 * Property of a term coordinates object. Denotes the source coordinate.
	 * @deprecated Use default properties on {@link ConceptCoordinates} instead
	 */
	@Deprecated
	public static final String COORD_SOURCE = "source";
	// TODO should not be named "PROP_" since it is not a node property
	public static final String PROP_COORDINATES = "coordinates";

}
