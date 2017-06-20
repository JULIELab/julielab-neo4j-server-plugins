package de.julielab.neo4j.plugins.evaluators;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;

public class NodeLabelEvaluator implements Evaluator {

	private Label label;
	private boolean acceptIfLabelNotPresent;

	/**
	 * 
	 * @param label The Neo4j node label to look for.
	 * @param acceptIfLabelNotPresent
	 *            Reverses the semantics of the evaluator if set to
	 *            <tt>true</tt>, i.e. accepts only nodes which do not have the
	 *            given label.
	 */
	public NodeLabelEvaluator(Label label, boolean acceptIfLabelNotPresent) {
		this.label = label;
		this.acceptIfLabelNotPresent = acceptIfLabelNotPresent;
	}

	public NodeLabelEvaluator(Label label) {
		this(label, false);
	}

	@Override
	public Evaluation evaluate(Path path) {
		Node endNode = path.endNode();
		// default condition for inclusion of node: the node has the sought label
		boolean condition = endNode.hasLabel(label);
		// however, if we want to accept only when the label is not present, reverse the condition
		if (acceptIfLabelNotPresent)
			condition = !condition;
		Evaluation eval = condition ? Evaluation.INCLUDE_AND_CONTINUE : Evaluation.EXCLUDE_AND_CONTINUE;
		return eval;
	}

}
