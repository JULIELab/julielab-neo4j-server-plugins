package de.julielab.neo4j.plugins.auxiliaries;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.server.rest.repr.Representation;

import de.julielab.neo4j.plugins.constants.NodeConstants;

public class NodeRepresentation extends RecursiveMappingRepresentation {
	public NodeRepresentation(Node node) {
		super(Representation.MAP);
		Map<String, Object> nodeMap = convertNodeToMap(node);
		setUnderlyingMap(nodeMap);
	}

	private Map<String, Object> convertNodeToMap(Node node) {
		HashMap<String, Object> nodeMap = new HashMap<String, Object>();
		for (String key : node.getPropertyKeys()) {
			nodeMap.put(key, node.getProperty(key));
		}
		List<String> labels = new ArrayList<>();
		for (Label label : node.getLabels()) {
			labels.add(label.name());
		}
		nodeMap.put(NodeConstants.KEY_LABELS, labels.toArray(new String[labels.size()]));
		return nodeMap;
	}
}
