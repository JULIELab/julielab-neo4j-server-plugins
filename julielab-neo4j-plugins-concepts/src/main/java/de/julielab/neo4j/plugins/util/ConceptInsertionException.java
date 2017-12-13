package de.julielab.neo4j.plugins.util;

public class ConceptInsertionException extends JulielabNeo4jPluginException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2403465901366145637L;

	public ConceptInsertionException() {
		super();
	}

	public ConceptInsertionException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ConceptInsertionException(String message, Throwable cause) {
		super(message, cause);
	}

	public ConceptInsertionException(String message) {
		super(message);
	}

	public ConceptInsertionException(Throwable cause) {
		super(cause);
	}

}
