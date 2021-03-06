package de.julielab.neo4j.plugins.datarepresentation;

public class ConceptCoordinates implements Cloneable {

	public String sourceId;
	public String source;
	public String originalId;
	public String originalSource;
	public boolean uniqueSourceId;

	public ConceptCoordinates() {
	}



	public ConceptCoordinates(String sourceId, String source, String originalId, String originalSource,
			boolean uniqueSourceId) {
		super();
		this.sourceId = sourceId;
		this.source = source;
		this.originalId = originalId;
		this.originalSource = originalSource;
		this.uniqueSourceId = uniqueSourceId;
	}

	public ConceptCoordinates(String sourceId, String source, String originalId, String originalSource) {
		this(sourceId, source, originalId, originalSource, false);
	}

	public ConceptCoordinates(String sourceId, String source, boolean uniqueSourceId) {
		this(sourceId, source, null, null, uniqueSourceId);
	}

	public ConceptCoordinates(String id, String source, CoordinateType src) {
		switch (src) {
		case OSRC:
			originalId = id;
			originalSource = source;
			break;
		case SRC:
			sourceId = id;
			this.source = source;
			break;
		}
	}


	public ConceptCoordinates(ConceptCoordinates coordinates) {
		this.originalId = coordinates.originalId;
		this.originalSource = coordinates.originalSource;
		this.sourceId = coordinates.sourceId;
		this.source = coordinates.source;
		this.uniqueSourceId = coordinates.uniqueSourceId;
	}

	@Override
	public String toString() {
		return "org(" + originalId + ", " + originalSource + ") / src(" + sourceId + ", " + source + ")";
	}

	@Override
	public ConceptCoordinates clone() {
		return new ConceptCoordinates(this);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((originalId == null) ? 0 : originalId.hashCode());
		result = prime * result + ((originalSource == null) ? 0 : originalSource.hashCode());
		result = prime * result + ((source == null) ? 0 : source.hashCode());
		result = prime * result + ((sourceId == null) ? 0 : sourceId.hashCode());
		result = prime * result + (uniqueSourceId ? 1231 : 1237);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ConceptCoordinates other = (ConceptCoordinates) obj;
		if (originalId == null) {
			if (other.originalId != null)
				return false;
		} else if (!originalId.equals(other.originalId))
			return false;
		if (originalSource == null) {
			if (other.originalSource != null)
				return false;
		} else if (!originalSource.equals(other.originalSource))
			return false;
		if (source == null) {
			if (other.source != null)
				return false;
		} else if (!source.equals(other.source))
			return false;
		if (sourceId == null) {
			if (other.sourceId != null)
				return false;
		} else if (!sourceId.equals(other.sourceId))
			return false;
		if (uniqueSourceId != other.uniqueSourceId)
			return false;
		return true;
	}


}
