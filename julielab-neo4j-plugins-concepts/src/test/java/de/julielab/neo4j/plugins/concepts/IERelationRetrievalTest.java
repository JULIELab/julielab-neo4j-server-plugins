package de.julielab.neo4j.plugins.concepts;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.julielab.neo4j.plugins.Indexes;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;
import de.julielab.neo4j.plugins.datarepresentation.util.ConceptsJsonSerializer;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.harness.junit.rule.Neo4jRule;
import org.neo4j.test.server.HTTP;

import java.io.IOException;
import java.nio.file.Path;

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
        ImportConcepts importConcepts = om.readValue(Path.of("src", "test", "resources", "interactionRetrievalTestData.json").toFile(), ImportConcepts.class);
        String uri = neo4j.httpURI().resolve("concepts/concept_manager/" + ConceptManager.INSERT_CONCEPTS).toString();
        HTTP.Response response = HTTP.POST(uri, ConceptsJsonSerializer.toJsonTree(importConcepts));
        System.out.println(response);
    }

    @Test
    public void test() {

    }
}