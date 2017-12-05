package de.julielab.neo4j.plugins.datarepresentation;

/**
 * 
 * @author faessler
 * @deprecated Use {@link ConceptCoordinates} instead
 */
@Deprecated
public class TermCoordinates {
	public TermCoordinates(String id, String source) {
		this.id = id;
		this.source = source;
	}

	public String id;
	public String source;

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((source == null) ? 0 : source.hashCode());
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
		TermCoordinates other = (TermCoordinates) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (source == null) {
			if (other.source != null)
				return false;
		} else if (!source.equals(other.source))
			return false;
		return true;
	}

}