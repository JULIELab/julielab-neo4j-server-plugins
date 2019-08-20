package de.julielab.neo4j.plugins.evaluators;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;

import java.util.ArrayList;
import java.util.List;

/**
 * An Evaluator that returns {@link Evaluation#INCLUDE_AND_CONTINUE} if the evaluated node has the given property key
 * and the value is or contains (in case the value is an array) the given property value.
 * 
 * @author faessler
 * 
 */
public class PropertyEvaluator implements Evaluator {

	private Object propertyValue;
	private boolean allProperties;
	private boolean allValues;
	private List<String> propertyKeyList;

	public PropertyEvaluator(String propertyKey, Object propertyValue) {
		this.propertyValue = propertyValue;
		this.allProperties = propertyKey.equals("*");
		this.allValues = propertyValue.equals("*");
		this.propertyKeyList = new ArrayList<>(1);
		this.propertyKeyList.add(propertyKey);
	}

	@Override
	public Evaluation evaluate(Path path) {
		Evaluation eval = Evaluation.EXCLUDE_AND_CONTINUE;
		if (allProperties && allValues)
			return Evaluation.INCLUDE_AND_CONTINUE;
		Node node = path.endNode();
		Iterable<String> propertyKeys = allProperties ? node.getPropertyKeys() : propertyKeyList;
		for (String propertyKey : propertyKeys) {
			if (node.hasProperty(propertyKey)) {
				if (allValues)
					return Evaluation.INCLUDE_AND_CONTINUE;
				Object value = node.getProperty(propertyKey);
				if (value.getClass().isArray()) {
					Object[] valueArray = (Object[]) value;
					for (Object o : valueArray) {
						if (o.equals(propertyValue)) {
							eval = Evaluation.INCLUDE_AND_CONTINUE;
						}
					}
				} else if (value.equals(propertyValue)) {
					eval = Evaluation.INCLUDE_AND_CONTINUE;
				}
			}
		}
		return eval;
	}

}
