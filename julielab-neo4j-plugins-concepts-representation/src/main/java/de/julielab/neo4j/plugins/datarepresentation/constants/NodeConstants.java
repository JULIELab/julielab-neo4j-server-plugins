package de.julielab.neo4j.plugins.datarepresentation.constants;

import org.neo4j.graphdb.Label;

public class NodeConstants extends de.julielab.neo4j.plugins.constants.NodeConstants {

	public static enum Labels implements Label {
		UNIQUE, ROOT
	}

	public static final String PROP_NAME = "name";
	public static final String PROP_ID = "id";
	public static final String PROP_VALUE = "value";
	/**
	 * This node property may be used to add list of labels - in a more general sense - to a node. Examples are labels
	 * like 'show this facet group for the search interface' or 'hide this facet on startup'.
	 */
	public static final String PROP_GENERAL_LABELS = "generalLabels";

	/**
	 * The value of this property is a list of boolean properties where each property may exist at most once in the
	 * database. It can be used to identify special facets. For instance, the facet listing all first authors may be
	 * allocated the 'firstAuthors' label.
	 */
	public static final String PROP_UNIQUE_LABELS = "uniqueLabels";

	/**
	 * An array of qualifier strings for a node. For example, genes can be qualified by the species they belong to,
	 * facets could be qualified by their source (however, we currently don't do it this way, this is just meant as an example).
	 */
	public static final String PROP_QUALIFIERS = "qualifiers";

	/**
	 * An index for "anchor" nodes.
	 */
	public static final String INDEX_ROOT_NODES = "rootNodeIndex";
	/**
	 * This property is supposed to contain a String representing a JSON map with arbitrary key-value pairs. This way, a
	 * custom and flexible set of additional properties can be defined for a node that can easily be queried by the
	 * application. For example, we use it in Semedico for facets that have more information with them than other facets
	 * due to the natur of their source. I.e. when the source is BioPortal, facets do not only have a name but
	 * additionally an acronym, an IRI and a VirtualID. It would make no sense to leverage these properties to default
	 * facet properties because they are specific to BioPortal ontologies.
	 */
	public static final String PROP_PROPERTIES = "properties";
}
