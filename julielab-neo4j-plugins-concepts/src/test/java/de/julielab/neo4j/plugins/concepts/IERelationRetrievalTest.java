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
import java.util.List;
import java.util.Map;

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
        HTTP.POST(uriConceptInsertion, ConceptsJsonSerializer.toJsonTree(importConcepts));

        ImportIERelations importIERelations = om.readValue(Path.of("src", "test", "resources", "interactionRetrievalTestRelations.json").toFile(), ImportIERelations.class);
        String uriRelationInsertion = neo4j.httpURI().resolve("concepts/concept_manager/" + ConceptManager.INSERT_IE_RELATIONS).toString();
        HTTP.POST(uriRelationInsertion, ConceptsJsonSerializer.toJsonTree(importIERelations));
    }

    @Test
    public void retrieveRelationsOfOneNode() throws JsonProcessingException {
        ObjectMapper om = new ObjectMapper();
        String uriRelationRetrieval = neo4j.httpURI().resolve("concepts/concept_manager/" + ConceptManager.RETRIEVE_IE_RELATIONS).toString();
        HTTP.Response response = HTTP.POST(uriRelationRetrieval, om.readValue("{\"a_list\":{\"id_property\":\"sourceIds\",\"id_source\":\"NCBI Gene\",\"ids\":[\"2475\"]},\"relationTypes\":[\"regulation\"]}", RelationRetrievalRequest.class));
        assertThat(response.status()).isEqualTo(200);
        List<Map<String, Object>> result = response.content();
        assertThat(result.get(0)).containsAllEntriesOf(Map.of("arg1Name", "MTOR", "arg2Name", "SCYL3", "arg1Id", "2475", "arg2Id", "57147", "count", 2));
    }
}