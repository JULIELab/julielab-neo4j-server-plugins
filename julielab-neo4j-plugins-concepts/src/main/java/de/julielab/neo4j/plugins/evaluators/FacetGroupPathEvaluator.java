package de.julielab.neo4j.plugins.evaluators;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;

import de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants;

/**
 * An <tt>Evaluator</tt> which includes the facet groups node as well as the
 * facet group with the name passed to the constructor.
 * 
 * @author faessler
 * 
 */
public class FacetGroupPathEvaluator implements Evaluator {

	private final String facetGroupName;

	public FacetGroupPathEvaluator(String facetGroupName) {
		this.facetGroupName = facetGroupName;
	}

	@Override
	public Evaluation evaluate(Path path) {
		Node endNode = path.endNode();
		String name = (String) endNode.getProperty(
				ConceptConstants.PROP_NAME, null);
		if (facetGroupName.equals(name)) {
			return Evaluation.INCLUDE_AND_PRUNE;
		} else if (FacetConstants.NAME_FACET_GROUPS.equals(name)) {
			return Evaluation.INCLUDE_AND_CONTINUE;
		}
		return Evaluation.EXCLUDE_AND_CONTINUE;
	}
}