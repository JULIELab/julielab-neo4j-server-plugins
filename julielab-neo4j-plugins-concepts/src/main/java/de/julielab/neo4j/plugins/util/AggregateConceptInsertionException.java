package de.julielab.neo4j.plugins.util;

public class AggregateConceptInsertionException extends ConceptInsertionException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1512417820020469807L;

	public AggregateConceptInsertionException() {
		super();
	}

	public AggregateConceptInsertionException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public AggregateConceptInsertionException(String message, Throwable cause) {
		super(message, cause);
	}

	public AggregateConceptInsertionException(String message) {
		super(message);
	}

	public AggregateConceptInsertionException(Throwable cause) {
		super(cause);
	}

}
