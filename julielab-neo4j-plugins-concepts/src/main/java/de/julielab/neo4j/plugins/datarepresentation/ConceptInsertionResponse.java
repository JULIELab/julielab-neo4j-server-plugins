package de.julielab.neo4j.plugins.datarepresentation;

import de.julielab.neo4j.plugins.concepts.ConceptManager;

/**
 * A convenience class to parse the HTTP response from the
 * {@link ConceptManager#INSERT_CONCEPTS} plugin endpoint in another application. This
 * class is not actually used in the plugin code.
 * 
 * @author faessler
 *
 */
public class ConceptInsertionResponse {
	public long numCreatedTerms;
	public long numCreatedRelationships;
	public String facetId;
	public long time;
}
