package de.julielab.neo4j.plugins;

import de.julielab.neo4j.plugins.concepts.ConceptManager;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;
import de.julielab.neo4j.plugins.datarepresentation.ImportMapping;
import de.julielab.neo4j.plugins.datarepresentation.util.ConceptsJsonSerializer;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.harness.junit.rule.Neo4jRule;
import org.neo4j.test.server.HTTP;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ConceptManagerIntegrationTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withUnmanagedExtension("/concepts", ConceptManager.class).withFixture(graphDatabaseService -> {
                new Indexes(null).createIndexes(graphDatabaseService);
                return null;
            });

    @Test
    public void testInsertConcepts() throws Exception {
        ImportConcepts importConcepts = ConceptManagerTest.getTestConcepts(10);
        String uri = neo4j.httpURI().resolve("concepts/concept_manager/" + ConceptManager.INSERT_CONCEPTS).toString();
        HTTP.Response response = HTTP.POST(uri, ConceptsJsonSerializer.toJsonTree(importConcepts));
        //{"numCreatedConcepts":10,"numCreatedRelationships":10,"facetId":"fid0","time":507}
        assertThat(response.get("numCreatedConcepts").asInt()).isEqualTo(10);
        assertThat(response.get("numCreatedRelationships").asInt()).isEqualTo(10);
        assertThat(response.get("facetId").asText()).isEqualTo("fid0");
    }

    @Test
    public void insertMappings() {
        // Insert two sets of terms to create mappings between them
        ImportConcepts importConcepts = ConceptManagerTest.getTestConcepts(5);
        HTTP.Response response = HTTP.POST(neo4j.httpURI().resolve("concepts/concept_manager/" + ConceptManager.INSERT_CONCEPTS).toString(), ConceptsJsonSerializer.toJsonTree(importConcepts));
        assertThat(response.status()).isEqualTo(200);
        importConcepts = ConceptManagerTest.getTestConcepts(5, 5);
        importConcepts.getFacet().setName("facet2");
        response = HTTP.POST(neo4j.httpURI().resolve("concepts/concept_manager/" + ConceptManager.INSERT_CONCEPTS).toString(), ConceptsJsonSerializer.toJsonTree(importConcepts));
        assertThat(response.status()).isEqualTo(200);

        List<ImportMapping> importMappings = List.of(new ImportMapping("CONCEPT0", "CONCEPT7", "LOOM"), new ImportMapping("CONCEPT8", "CONCEPT4", "LOOM"));
        response = HTTP.POST(neo4j.httpURI().resolve("concepts/concept_manager/" + ConceptManager.INSERT_MAPPINGS).toString(), importMappings);
        assertThat(response.rawContent()).isEqualTo("2");
    }

}
