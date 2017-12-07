package de.julielab.neo4j.plugins.datarepresentation.util;

public class UncheckedJsonProcessingException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6777268071136098162L;
	public UncheckedJsonProcessingException() {
		super();
	}

	public UncheckedJsonProcessingException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public UncheckedJsonProcessingException(String message, Throwable cause) {
		super(message, cause);
	}

	public UncheckedJsonProcessingException(String message) {
		super(message);
	}

	public UncheckedJsonProcessingException(Throwable cause) {
		super(cause);
	}

}
