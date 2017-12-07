package de.julielab.neo4j.plugins.datarepresentation.util;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

public class ConceptsJsonSerializer {
	private ConceptsJsonSerializer() {
	}

	private static ObjectMapper mapper = new ObjectMapper().registerModule(new Jdk8Module());
	static {
		mapper.setSerializationInclusion(Include.NON_NULL);
		mapper.setSerializationInclusion(Include.NON_EMPTY);
	}

	public static synchronized String toJson(Object serializable) {
		try {
			return mapper.writeValueAsString(serializable);
		} catch (JsonProcessingException e) {
			throw new UncheckedJsonProcessingException(e);
		}
	}
}
