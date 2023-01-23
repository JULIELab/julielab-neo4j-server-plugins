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
	 * When set to <tt>true</tt>, parents that are not included in the insertion
	 * data will be dropped and the concept specifying the missing parent will
	 * be made a facet root. Otherwise, node parents that cannot be found will
	 * be created as nodes with the "hollow" label and be added as a facet root
	 * until the missing parent is imported.
	 */
	public boolean doNotCreateHollowParents;
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
	/**
	 * Normally, the preferred name is only set once. In case of correction or necessary update, this option
	 * allows to set the preferred name on already existing nodes. Only non-blank names are accepted.
	 */
	public boolean overridePreferredName;

	public ImportOptions() {
		doNotCreateHollowParents = false;
		cutParents = Collections.emptyList();
	}

	public ImportOptions(boolean doNotCreateHollowParents) {
		this();
		this.doNotCreateHollowParents = doNotCreateHollowParents;
	}

	@Override
	public String toString() {
		return "ImportOptions{" +
				"noFacetCmd=" + noFacetCmd +
				", cutParents=" + cutParents +
				", doNotCreateHollowParents=" + doNotCreateHollowParents +
				", createHollowAggregateElements=" + createHollowAggregateElements +
				", merge=" + merge +
				'}';
	}
}
