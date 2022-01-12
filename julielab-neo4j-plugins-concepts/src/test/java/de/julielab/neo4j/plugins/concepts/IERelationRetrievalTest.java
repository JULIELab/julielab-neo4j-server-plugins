package de.julielab.neo4j.plugins.concepts;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.julielab.neo4j.plugins.Indexes;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;
import de.julielab.neo4j.plugins.datarepresentation.RelationRetrievalRequest;
import de.julielab.neo4j.plugins.datarepresentation.constants.ImportIERelations;
import de.julielab.neo4j.plugins.datarepresentation.util.ConceptsJsonSerializer;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.harness.junit.rule.Neo4jRule;
import org.neo4j.test.server.HTTP;

import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
public class IERelationRetrievalTest {
    @ClassRule
    public static Neo4jRule neo4j = new Neo4jRule()
            .withUnmanagedExtension("/concepts", ConceptManager.class).withFixture(graphDatabaseService -> {
                new Indexes(null).createIndexes(graphDatabaseService);
                return null;
            });
    @BeforeClass
    public static void setup() throws IOException {
        ObjectMapper om = new ObjectMapper();
        ImportConcepts importConcepts = om.readValue(Path.of("src", "test", "resources", "interactionRetrievalTestConcepts.json").toFile(), ImportConcepts.class);
        String uriConceptInsertion = neo4j.httpURI().resolve("concepts/concept_manager/" + ConceptManager.INSERT_CONCEPTS).toString();
        HTTP.Response responseConceptInsertion = HTTP.POST(uriConceptInsertion, ConceptsJsonSerializer.toJsonTree(importConcepts));

        ImportIERelations importIERelations = om.readValue(Path.of("src", "test", "resources", "interactionRetrievalTestRelations.json").toFile(), ImportIERelations.class);
        String uriRelationInsertion = neo4j.httpURI().resolve("concepts/concept_manager/" + ConceptManager.INSERT_IE_RELATIONS).toString();
        HTTP.Response responseRelationInsertion = HTTP.POST(uriRelationInsertion, ConceptsJsonSerializer.toJsonTree(importIERelations));
    }

    @Test
    public void retrieveRelationsOfOneNode() throws JsonProcessingException {
        ObjectMapper om = new ObjectMapper();
        String uriRelationRetrieval = neo4j.httpURI().resolve("concepts/concept_manager/" + ConceptManager.RETRIEVE_IE_RELATIONS).toString();
        HTTP.Response response = HTTP.POST(uriRelationRetrieval, om.readValue("{\"a_list\":{\"id_property\":\"sourceId0\",\"id_source\":\"NCBI Gene\",\"ids\":[\"2475\"]}}", RelationRetrievalRequest.class));
        assertThat(response.status()).isEqualTo(200);
        String content = response.rawContent();
        assertThat(content).isEqualTo("[{\"arg1Name\":\"MTOR\",\"arg2Name\":\"SCYL3\",\"arg1Id\":\"2475\",\"arg2Id\":\"57147\",\"count\":2}]");
    }
}