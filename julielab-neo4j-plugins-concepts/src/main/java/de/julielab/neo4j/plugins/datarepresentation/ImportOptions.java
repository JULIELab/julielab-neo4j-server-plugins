package de.julielab.neo4j.plugins.datarepresentation;

import java.util.Collections;
import java.util.List;

public class ImportOptions {
	/**
	 * A command to specify terms to append to the "no facet" facet group.
	 */
	public AddToNonFacetGroupCommand noFacetCmd;
	/**
	 * Parents to "cut away" when seen, e.g. owl:Thing for ontology classes.
	 */
	public List<String> cutParents;
	/**
	 * When set to <tt>true</tt>, node parents that cannot be found will be
	 * created as nodes with the "hollow" label. Otherwise, the node defining
	 * the parent is added as a root term to its facet.
	 */
	public boolean createHollowParents;
	/**
	 * When set to <tt>true</tt>, aggregate elements that cannot be found will
	 * be created as nodes with the "hollow" label when explicitly importing
	 * aggregates. Otherwise, the respective aggregate element will not be added
	 * to the aggregate.
	 */
	public boolean createHollowAggregateElements;
	/**
	 * If set to <tt>true</tt>, only merging is performed, i.e. no new terms are
	 * created. Imported terms will be looked up by ID (original ID and original
	 * source or source ID). If found, the existing property values will be
	 * matched with those specified by the imported term. If not found, nothing
	 * will be changed.
	 */
	public boolean merge;
	
	public ImportOptions() {
		createHollowParents = false;
		cutParents = Collections.emptyList();
	}

	public ImportOptions(boolean createHollowParents) {
		this();
		this.createHollowParents = createHollowParents;
	}
}
