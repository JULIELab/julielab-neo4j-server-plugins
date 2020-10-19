package de.julielab.neo4j.plugins.auxiliaries.semedico;

import de.julielab.neo4j.plugins.datarepresentation.ConceptCoordinates;
import org.neo4j.graphdb.Node;

import java.util.HashMap;
import java.util.Map;

public class CoordinatesMap {
	private final CoordinatesSet keys = new CoordinatesSet();
	private final Map<ConceptCoordinates, Node> nodes = new HashMap<>();

	public void put(ConceptCoordinates key, Node node) {
		if (key == null || node == null)
			throw new IllegalArgumentException("The coordinate key and the node value must both be non-null.");
		boolean existed = keys.contains(key);
		if (!existed) {
			keys.add(key);
			nodes.put(key, node);
		}
    }

	public Node get(ConceptCoordinates key) {
		// Here is the trick: we use a CoordinatesSet to find the key. The key
		// is not required to be equal to the returned coordinates but just
		// compatible in terms of IDs and sources.
		ConceptCoordinates coordinates = keys.get(key);
		return nodes.get(coordinates);
	}

	public boolean containsKey(ConceptCoordinates coordinates) {
		return keys.contains(coordinates);
	}

	public boolean isEmpty() {
		return keys.isEmpty();
	}
}
