package de.julielab.neo4j.plugins.evaluators;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;

public class HasRelationShipEvaluator implements Evaluator {

	private Direction direction;
	private boolean shouldHave;
	private RelationshipType[] types;

	public HasRelationShipEvaluator(Direction direction, RelationshipType[] types, boolean shouldHave) {
		this.direction = direction;
		this.types = types;
		this.shouldHave = shouldHave;
	}
	
	@Override
	public Evaluation evaluate(Path path) {
		Node endNode = path.endNode();
		boolean condition = endNode.hasRelationship(direction, types);
		if (!shouldHave)
			condition = !condition;
		Evaluation eval = condition ? Evaluation.INCLUDE_AND_CONTINUE : Evaluation.EXCLUDE_AND_CONTINUE;
		return eval;
	}

}
