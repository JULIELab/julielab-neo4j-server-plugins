package de.julielab.neo4j.plugins.datarepresentation.constants;

public class FacetGroupConstants extends NodeConstants {
	public static final String PROP_POSITION = "position";
	/**
	 * (Optional?) Property to canonically specify which type this facet group is, e.g. the BioPortal facet group. This
	 * is used to be independent of the actual name.
	 */
	public static final String PROP_TYPE = "type";
}
