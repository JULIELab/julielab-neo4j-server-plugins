package de.julielab.neo4j.plugins.datarepresentation.constants;

public class FacetConstants extends NodeConstants {
	/**
	 * Taxonomically arranged concepts.
	 */
	public static final String SRC_TYPE_HIERARCHICAL = "hierarchical";
	/**
	 * Concepts without taxonomical relations between them.
	 */
	public static final String SRC_TYPE_FLAT = "flat";
	/**
	 * No pre-defined concepts with a name and synonyms but just strings. This kind
	 * of facet typically has no concepts in the database but is filled by the using
	 * application at runtime. An example would be author names of a bibliographic
	 * system that does not group authors into concepts but treats each name as a
	 * separate entity.
	 */
	public static final String SRC_TYPE_STRINGS = "strings";

	/**
	 * The custom ID may be specified during the import of facets. It serves as an
	 * alternative identifier for facets besides the automatically generated facet
	 * ID. This custom ID must be unique per facet.
	 */
	public static final String PROP_CUSTOM_ID = "customId";
	/**
	 * The type of content of the facet identified by {@link #PROP_SOURCE_NAME}. The
	 * source type tells whether there are IDs of hierarchical terms to find in a
	 * facet, or mere strings like dates or author names (as long as these are not
	 * captured as concepts). Values should be one of the constants below.
	 * 
	 * @see #SRC_TYPE_HIERARCHICAL
	 * @see #SRC_TYPE_FLAT
	 * @see #SRC_TYPE_STRINGS
	 */
	public static final String PROP_SOURCE_TYPE = "sourceType";

	public static final String NAME_FACET_GROUPS = "facetGroups";
	
	public static final String NAME_NO_FACET_GROUPS = "noFacet-Groups";

	/**
	 * <p>
	 * Used when inserting facets to specify the facet group they are to be
	 * connected to. This is the key under which the facet group information can be
	 * found in the map defining a facet.
	 * </p>
	 * 
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

	public static final String FACET_FIELD_PREFIX = "facetTerms";

	/**
	 * Import option. If this field is set to <tt>true</tt> for the facet definition
	 * used in a term import, that facet will not be connected to the normal facet
	 * groups but to the no-facet section.
	 */
	public static final String NO_FACET = "noFacet";
	/**
	 * An optional short name for display at places that lack more space.
	 */
	public static final String PROP_SHORT_NAME = "shortName";
	
	/**
	 * The number of roots a facet has. This is precomputed to speed up decisions within Semedico depending on the
	 * number of roots of a facet. The information is used to decide whether to load all facet roots for faceting or
	 * just to switch the facet to flat counts because there are too many roots (e.g. Genes and Proteins have 450k
	 * roots).
	 */
	public static final String PROP_NUM_ROOT_TERMS = "numRoots";
	
}
