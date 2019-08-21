package de.julielab.neo4j.plugins;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;

public class JacksonTest {
    public static void main(String args[]) throws JsonProcessingException {
        ImportConcepts testTerms = new ImportConcepts(null);
//        testTerms = getTestTerms(1);
//        testTerms.getConcepts().get(0).coordinates.originalId = "orgId";
//        testTerms.getConcepts().get(0).coordinates.originalSource = "src1";
//        testTerms.getConcepts().get(0).coordinates.source = "src1";
        final ObjectMapper om = new ObjectMapper();
        System.out.println(om.writeValueAsString(testTerms));
    }
}
