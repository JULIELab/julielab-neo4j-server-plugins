package de.julielab.neo4j.plugins.datarepresentation;

public class CoordinatesInvalidException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4971811446383883117L;

	public CoordinatesInvalidException() {
		super();
	}

	public CoordinatesInvalidException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public CoordinatesInvalidException(String message, Throwable cause) {
		super(message, cause);
	}

	public CoordinatesInvalidException(String message) {
		super(message);
	}

	public CoordinatesInvalidException(Throwable cause) {
		super(cause);
	}

}
