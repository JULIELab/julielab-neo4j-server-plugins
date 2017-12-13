package de.julielab.neo4j.plugins.util;

public class JulielabNeo4jPluginException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1625479321561503953L;

	public JulielabNeo4jPluginException() {
		super();
	}

	public JulielabNeo4jPluginException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public JulielabNeo4jPluginException(String message, Throwable cause) {
		super(message, cause);
	}

	public JulielabNeo4jPluginException(String message) {
		super(message);
	}

	public JulielabNeo4jPluginException(Throwable cause) {
		super(cause);
	}

}
