package de.julielab.neo4j.plugins;

import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.harness.junit.rule.Neo4jRule;
import org.neo4j.test.server.HTTP;

import static org.assertj.core.api.Assertions.assertThat;
public class ConceptManagerIntegrationTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withUnmanagedExtension("/app", ConceptManager.class).withFixture(graphDatabaseService -> {
                new Indexes(null).createIndexes(graphDatabaseService);
                return null;
            });

    @Test
    public void testInsertConcepts() throws Exception {
        ImportConcepts testTerms = ConceptManagerTest.getTestTerms(10);
        HTTP.Response response = HTTP.POST(neo4j.httpURI().resolve("app/concept_manager/" + ConceptManager.INSERT_CONCEPTS).toString(), testTerms);
        //{"numCreatedConcepts":10,"numCreatedRelationships":10,"facetId":"fid0","time":507}
        assertThat(response.get("numCreatedConcepts").asInt()).isEqualTo(10);
        assertThat(response.get("numCreatedRelationships").asInt()).isEqualTo(10);
        assertThat(response.get("facetId").asText()).isEqualTo("fid0");
    }

}
