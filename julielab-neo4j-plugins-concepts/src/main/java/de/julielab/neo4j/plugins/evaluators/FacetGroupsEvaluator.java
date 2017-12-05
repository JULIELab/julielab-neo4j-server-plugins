package de.julielab.neo4j.plugins.evaluators;

import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_NAME;
import static de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants.NAME_FACET_GROUPS;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;

/**
 * Evaluates to {@link Evaluation#INCLUDE_AND_CONTINUE} for the facet groups
 * node, i.e. the predecessor of the individual facet group nodes.
 * 
 * @author faessler
 * 
 */
public class FacetGroupsEvaluator implements Evaluator {

	@Override
	public Evaluation evaluate(Path path) {
		Node endNode = path.endNode();
		if (endNode.hasProperty(PROP_NAME)
				&& endNode.getProperty(PROP_NAME).equals(NAME_FACET_GROUPS))
			return Evaluation.INCLUDE_AND_CONTINUE;
		return Evaluation.EXCLUDE_AND_CONTINUE;
	}
}
