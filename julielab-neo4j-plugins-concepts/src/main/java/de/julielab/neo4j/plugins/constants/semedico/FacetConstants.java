package de.julielab.neo4j.plugins.constants.semedico;

public class FacetConstants extends NodeConstants {
	public static final String PROP_CSS_ID = "cssId";
	public static final String PROP_POSITION = "position";
	/**
	 * <p>
	 * The name of the Lucene field which is the source of the term IDs for a facet when e.g. computing facet counts for
	 * the Semedico frontend.
	 * </p>
	 * <p>
	 * This property cannot be set from the outside when creating a facet and will be ignored if delivered within the
	 * information for facet creation. Instead, when a facet is created, the automatically generated facet ID (assembled
	 * by {@link NodeIDPrefixConstants#FACET} and a ascending number for each new facet) will be appended to
	 * {@link #FACET_FIELD_PREFIX} and stored to this property.
	 * </p>
	 * <p>
	 * For faceting to actually work for a Semedico document search, a field of the resulting name must exist in the
	 * index which will eventually be queried for facet values in Semedico. The creation - and naming - of these fields
	 * is configured in the LuCas mapping file in the Semedico UIMA application.
	 * </p>
	 */
	public static final String PROP_SOURCE_NAME = "sourceName";
	/**
	 * The type of content of the Lucene fields indentified by {@link #PROP_SOURCE_NAME}. The source type tells whether
	 * there are IDs of hierarchical terms to find in a facet's source field, or mere strings like dates or author names
	 * (as long as these are not captured as terms, anyway). Values should be one of the constants below.
	 * 
	 * @see #SRC_TYPE_HIERARCHICAL
	 * @see #SRC_TYPE_FLAT
	 * @see #SRC_TYPE_STRINGS
	 */
	public static final String PROP_SOURCE_TYPE = "sourceType";
	/**
	 * The names of Lucene fields which should be searched when looking for terms included in a particular facet.
	 */
	public static final String PROP_SEARCH_FIELD_NAMES = "searchFieldNames";
	/**
	 * The names of Lucene fields on which filtering should be performed when a term of a particular facet is used for
	 * Lucene filtering.
	 */
	public static final String PROP_FILTER_FIELD_NAMES = "filterFieldNames";
	/**
	 * The number of roots a facet has. This is precomputed to speed up decisions within Semedico depending on the
	 * number of roots of a facet. The information is used to decide whether to load all facet roots for faceting or
	 * just to switch the facet to flat counts because there are too many roots (e.g. Genes and Proteins have 450k
	 * roots).
	 */
	public static final String PROP_NUM_ROOT_TERMS = "numRoots";

	/**
	 * Boolean property to indicate whether the facet is active - should be loaded and shown in the system - or not. If
	 * the property is missing on facet, the convention is that the facet is active.
	 */
	public static final String PROP_ACTIVE = "active";

	public static final String NAME_FACET_GROUPS = "facetGroups";

	public static final String NAME_NO_FACET_GROUPS = "noFacet-Groups";

	// public static final String INDEX_ID = "facetIdIndex";

	public static final String INDEX_GENERAL_LABELS = "facetGeneralLabelsIndex";
	/**
	 * Currently, the type is "translated" by the Semedico Facet service into from which field the facet should read and
	 * to which facet group it belongs, among other things. All this information is no stored in the graph structure or
	 * should be an explicit property value (the index field name, for example).
	 */
	@Deprecated
	public static final String PROP_TYPE = "type";

	/**
	 * <p>
	 * Used when inserting facets to specify the facet group they are to be connected to. This is the key under which
	 * the facet group information can be found in the map defining a facet.
	 * </p>
	 * <pre>
	 * Map&lt;String, Object&gt; facetGroupMap = new HashMap&lt;&gt;();
	 * facetGroupMap.put(FacetGroupConstants.PROP_NAME, "myfacetgroup");
	 * 
	 * Map&lt;String, Object&gt; facetMap = new HashMap&lt;&gt;();
	 * facetMap.put(FacetConstants.PROP_NAME, "myfacet");
	 * facetMap.put(.,.);
	 * facetMap.put({@link #FACET_GROUP}, facetGroupMap);
	 * </pre>
	 */
	public static final String FACET_GROUP = "facetGroup";

	public static final String SRC_TYPE_HIERARCHICAL = "hierarchical";
	public static final String SRC_TYPE_FLAT = "flat";
	public static final String SRC_TYPE_STRINGS = "strings";
	public static final String SRC_TYPE_FACET_AGGREGATION = "facetAggregation";

	public static final String FACET_FIELD_PREFIX = "facetTerms";

	/**
	 * Import option. If this field is set to <tt>true</tt> for the facet definition used in a term import, that facet
	 * will not be connected to the normal facet groups but to the no-facet section.
	 */
	public static final String NO_FACET = "noFacet";
	/**
	 * For facets that are just an aggregation of other facets, this property defines a list of facet labels identifying
	 * those facets to be used for the aggregation.
	 */
	public static final String AGGREGATION_LABELS = "aggregationLabels";
	/**
	 * For facets that are just an aggregation of other facets, this property defines a list of index fields to directly
	 * draw terms from.
	 */
	public static final String PROP_AGGREGATION_FIELDS = "aggregationFields";
	/**
	 * For facets that are induced by a term, i.e. that have some kind of equivalent term. For example, the
	 * "phosphorylation" event facet has the "phosphorylation" event term as an inducing term. This information is
	 * sometimes necessary to create connections from term to facet.
	 */
	public static final String PROP_INDUCING_TERM = "inducingTerm";
	/**
	 * An optional short name for display at places that lack more space.
	 */
	public static final String PROP_SHORT_NAME = "shortName";

}
