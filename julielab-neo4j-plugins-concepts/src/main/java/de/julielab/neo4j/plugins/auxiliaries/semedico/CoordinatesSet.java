package de.julielab.neo4j.plugins.auxiliaries.semedico;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import de.julielab.neo4j.plugins.datarepresentation.ConceptCoordinates;

public class CoordinatesSet implements Iterable<ConceptCoordinates> {
	private Map<String, ConceptCoordinates> coordsByOriginalId = new HashMap<>();
	private Map<String, ConceptCoordinates> coordsBySourceId = new HashMap<>();

	public boolean add(ConceptCoordinates coordinates) {
		ConceptCoordinates c = coordinates.clone();
		ConceptCoordinates c2 = get(c);

		if (c2 != null) {
			// we already know the coordinates, or at least compatible ones; add new information if we got some
			if (c2.originalId == null && c.originalId != null) {
				c2.originalId = c.originalId;
				c2.originalSource = c.originalSource;
			}
			if (c2.sourceId == null && c.sourceId != null) {
				c2.sourceId = c.sourceId;
				c2.source = c.source;
			}
			return true;
		} else {
			// We didn't have compatible coordinates before. Add them.
			if (c.originalId != null)
				coordsByOriginalId.put(c.originalId, c);
			if (coordinates.sourceId != null)
				coordsBySourceId.put(c.sourceId, c);
			return false;
		}

	}

	public ConceptCoordinates get(ConceptCoordinates coordinates) {
		ConceptCoordinates c = coordsByOriginalId.get(coordinates.originalId);
		if (c != null && c.originalSource.equals(coordinates.originalSource))
			return c;
		// still here, so the original ID wasn't a match
		c = coordsBySourceId.get(coordinates.sourceId);
		if (c != null && c.sourceId.equals(coordinates.sourceId)) {
			if (c.source.equals(coordinates.source) || (c.uniqueSourceId && coordinates.uniqueSourceId))
				return c;
		}
		return null;
	}

	public boolean contains(ConceptCoordinates coordinates) {
		return contains(coordinates, false);
	}
	
	public boolean contains(ConceptCoordinates coordinates, boolean merge) {
		boolean contains = get(coordinates) != null;
		if (contains && merge)
			return add(coordinates);
		return contains;
	}

	@Override
	public Iterator<ConceptCoordinates> iterator() {
		Set<ConceptCoordinates> s = new HashSet<>();
		s.addAll(coordsByOriginalId.values());
		s.addAll(coordsBySourceId.values());
		return s.iterator();
	}

	public boolean isEmpty() {
		return coordsByOriginalId.isEmpty() && coordsBySourceId.isEmpty();
	}
}
