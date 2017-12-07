package de.julielab.neo4j.plugins.datarepresentation.constants;

import org.neo4j.graphdb.Label;

public class NodeConstants extends de.julielab.neo4j.plugins.constants.NodeConstants {

	public enum Labels implements Label {
		UNIQUE, ROOT
	}

	public static final String PROP_NAME = "name";
	public static final String PROP_ID = "id";
	public static final String PROP_VALUE = "value";
	/**
	 * This node property may be used to add list of Neo4j labels to a node.
	 * Examples are labels like 'show this facet group for the search interface' or
	 * 'hide this facet on startup'.
	 * 
	 */
	public static final String PROP_LABELS = "labels";

	/**
	 * The value of this property is a list of boolean properties where each
	 * property may exist at most once in the database. It can be used to identify
	 * special facets. For instance, the facet listing all first authors may be
	 * allocated the 'firstAuthors' label.
	 * 
	 * @deprecated better actually give those nodes a unique Neo4j label
	 */
	public static final String PROP_UNIQUE_LABELS = "uniqueLabels";

	/**
	 * An array of qualifier strings for a node. For example, genes can be qualified
	 * by the species they belong to, facets could be qualified by their source
	 * (however, we currently don't do it this way, this is just meant as an
	 * example).
	 */
	public static final String PROP_QUALIFIERS = "qualifiers";

	/**
	 * An index for "anchor" nodes.
	 */
	public static final String INDEX_ROOT_NODES = "rootNodeIndex";
}
