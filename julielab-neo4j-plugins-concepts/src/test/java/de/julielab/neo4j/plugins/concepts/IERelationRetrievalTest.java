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

// The tests are nicely written when we use raw maps, disable the warnings
@SuppressWarnings({"rawtypes", "unchecked"})
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
    public void retrieveRelationsOfOneNodeOneRelType() throws JsonProcessingException {
        // Retrieve the regulation events of a single gene, MTOR.
        ObjectMapper om = new ObjectMapper();
        String uriRelationRetrieval = neo4j.httpURI().resolve("concepts/concept_manager/" + ConceptManager.RETRIEVE_IE_RELATIONS).toString();
        HTTP.Response response = HTTP.POST(uriRelationRetrieval, om.readValue("{\"a_list\":{\"id_property\":\"sourceIds\",\"ids\":[\"2475\"]},\"relationTypes\":[\"regulation\"]}", RelationRetrievalRequest.class));
        assertThat(response.status()).isEqualTo(200);
        List<Map<String, Object>> result = response.content();
        assertThat(result.get(0)).containsAllEntriesOf(Map.of("arg1Name", "MTOR", "arg2Name", "SCYL3", "arg1Id", "genegroup2475", "arg2Id", "genegroup57147", "count", 2));
    }

    @Test
    public void retrieveRelationsOfOneNodeOneRelType2() throws JsonProcessingException {
        // Retrieve the regulation events of a single gene, LOC117183042.
        // This case is interesting because that gene is not part of an orthology aggregate.
        ObjectMapper om = new ObjectMapper();
        String uriRelationRetrieval = neo4j.httpURI().resolve("concepts/concept_manager/" + ConceptManager.RETRIEVE_IE_RELATIONS).toString();
        HTTP.Response response = HTTP.POST(uriRelationRetrieval, om.readValue("{\"a_list\":{\"id_property\":\"sourceIds\",\"ids\":[\"117183042\"]},\"relationTypes\":[\"phosphorylation\"]}", RelationRetrievalRequest.class));
        assertThat(response.status()).isEqualTo(200);
        List<Map<String, Object>> result = response.content();
        assertThat(result.get(0)).containsAllEntriesOf(Map.of("arg1Name", "LOC117183042", "arg2Name", "SCYL3", "arg1Id", "117183042", "arg2Id", "genegroup57147", "count", 1));
    }

    @Test
    public void retrieveRelationsOfOneNodeMultipleRelTypes() throws JsonProcessingException {
        // Retrieve all events of a single gene, MTOR. There are regulation with SCYL3, phosphorylation from LRRC51
        // and binding with LOC117183042
        ObjectMapper om = new ObjectMapper();
        String uriRelationRetrieval = neo4j.httpURI().resolve("concepts/concept_manager/" + ConceptManager.RETRIEVE_IE_RELATIONS).toString();
        HTTP.Response response = HTTP.POST(uriRelationRetrieval, om.readValue("{\"a_list\":{\"id_property\":\"sourceIds\",\"ids\":[\"2475\"]},\"relationTypes\":[\"regulation\",\"phosphorylation\",\"binding\"]}", RelationRetrievalRequest.class));
        assertThat(response.status()).isEqualTo(200);
        List<Map<String, Object>> result = response.content();
        assertThat(result).hasSize(3);
        // MTOR -> SCYL3: 2x regulation, 1x binding
        // MTOR <- LOC117183042: binding
        // MTOR <- LRRC51: 3x phosphorylation
        Map[] expectedResults = {Map.of("arg1Name", "MTOR", "arg2Name", "SCYL3", "arg1Id", "genegroup2475", "arg2Id", "genegroup57147", "count", 3),
                Map.of("arg1Name", "MTOR", "arg2Name", "LOC117183042", "arg1Id", "genegroup2475", "arg2Id", "117183042", "count", 1),
                Map.of("arg1Name", "MTOR", "arg2Name", "LRRC51", "arg1Id", "genegroup2475", "arg2Id", "toporthology1", "count", 4)};
        assertThat(result).contains(expectedResults);

        // Do the same thing but directly start at the homology aggregate. The result should be the same.
        response = HTTP.POST(uriRelationRetrieval, om.readValue("{\"a_list\":{\"id_property\":\"sourceIds\",\"id_source\":\"GeneOrthology\",\"ids\":[\"genegroup2475\"]},\"relationTypes\":[\"regulation\",\"phosphorylation\",\"binding\"]}", RelationRetrievalRequest.class));
        assertThat(response.status()).isEqualTo(200);
        result = response.content();
        assertThat(result).hasSize(3);
        assertThat(result).contains(expectedResults);
    }

    @Test
    public void retrieveRelationsOfMultipleNodesNodeMultipleRelTypesAllowSourceAsTarget() throws JsonProcessingException {
        // One-sided retrieval scenario.
        // Retrieve all relations incident with three query nodes, MTOR, LRRC51 and SCYL3 where relations between
        // the query nodes are also returned. Due to the small test graph
        // this just results in the whole graph.
        ObjectMapper om = new ObjectMapper();
        String uriRelationRetrieval = neo4j.httpURI().resolve("concepts/concept_manager/" + ConceptManager.RETRIEVE_IE_RELATIONS).toString();
        HTTP.Response response = HTTP.POST(uriRelationRetrieval, om.readValue("{\"a_list\":{\"id_property\":\"sourceIds\",\"ids\":[\"2475\",\"120356739\",\"57147\"]},\"relationTypes\":[\"regulation\",\"phosphorylation\",\"binding\"],\"enable_inter_input_relation_retrieval\":true}", RelationRetrievalRequest.class));
        assertThat(response.status()).isEqualTo(200);
        List<Map<String, Object>> result = response.content();
        assertThat(result).hasSize(6);
        // Query: MTOR, LRRC51, SCYL3
        // Result: All relations (of the given relation types which is also all of them in the test data) where
        // one of the input genes is an argument
        // The result is, in this test case, all relations in the test database.
        Map[] expectedResults = {Map.of("arg1Name", "MTOR", "arg2Name", "SCYL3", "arg1Id", "genegroup2475", "arg2Id", "genegroup57147", "count", 3),
                Map.of("arg1Name", "MTOR", "arg2Name", "LOC117183042", "arg1Id", "genegroup2475", "arg2Id", "117183042", "count", 1),
                Map.of("arg1Name", "MTOR", "arg2Name", "LRRC51", "arg1Id", "genegroup2475", "arg2Id", "toporthology1", "count", 4),
        Map.of("arg1Name", "LRRC51", "arg2Name","MTOR", "arg1Id","toporthology1", "arg2Id","genegroup2475", "count",4),
        Map.of("arg1Name","SCYL3", "arg2Name","LOC117183042", "arg1Id","genegroup57147", "arg2Id","117183042", "count",1),
        Map.of("arg1Name","SCYL3", "arg2Name","MTOR", "arg1Id","genegroup57147", "arg2Id","genegroup2475", "count",3)};
        assertThat(result).contains(expectedResults);
    }

    @Test
    public void retrieveRelationsOfMultipleNodesNodeMultipleRelTypesExcludeSourceAsTarget() throws JsonProcessingException {
        // One-sided retrieval scenario.
        // Retrieve all relations incident with three query nodes, MTOR, LRRC51 and SCYL3 where relations between
        // the query nodes are excluded.
        ObjectMapper om = new ObjectMapper();
        String uriRelationRetrieval = neo4j.httpURI().resolve("concepts/concept_manager/" + ConceptManager.RETRIEVE_IE_RELATIONS).toString();
        HTTP.Response response = HTTP.POST(uriRelationRetrieval, om.readValue("{\"a_list\":{\"id_property\":\"sourceIds\",\"ids\":[\"2475\",\"120356739\",\"57147\"]},\"relationTypes\":[\"regulation\",\"phosphorylation\",\"binding\"]}", RelationRetrievalRequest.class));
        assertThat(response.status()).isEqualTo(200);
        List<Map<String, Object>> result = response.content();
        assertThat(result).hasSize(2);
        // Query: MTOR, LRRC51, SCYL3
        // Result: The relations with LOC117183042 since all other relations are incident to two input nodes.
        Map[] expectedResults = {
                Map.of("arg1Name", "MTOR", "arg2Name", "LOC117183042", "arg1Id", "genegroup2475", "arg2Id", "117183042", "count", 1),
                Map.of("arg1Name", "SCYL3", "arg2Name", "LOC117183042", "arg1Id", "genegroup57147", "arg2Id", "117183042", "count", 1)};
        assertThat(result).contains(expectedResults);
    }

    @Test
    public void retrieveRelationsBetweenTwoNodes() throws JsonProcessingException {
        // Two-sided retrieval scenario.
        // Retrieve the relations between MTOR and SCYL3.
        ObjectMapper om = new ObjectMapper();
        String uriRelationRetrieval = neo4j.httpURI().resolve("concepts/concept_manager/" + ConceptManager.RETRIEVE_IE_RELATIONS).toString();
        HTTP.Response response = HTTP.POST(uriRelationRetrieval, om.readValue("{\"a_list\":{\"id_property\":\"sourceIds\",\"ids\":[\"2475\"]},\"b_list\":{\"id_property\":\"sourceIds\",\"ids\":[\"57147\"]},\"relationTypes\":[\"regulation\"]}", RelationRetrievalRequest.class));
        assertThat(response.status()).isEqualTo(200);
        List<Map<String, Object>> result = response.content();
        assertThat(result).hasSize(1);
        // Query: a: MTOR, b: LRRC51
        Map[] expectedResults = {
                Map.of("arg1Name", "MTOR", "arg2Name", "SCYL3", "arg1Id", "genegroup2475", "arg2Id", "genegroup57147", "count", 2)};
        assertThat(result).contains(expectedResults);
    }

    @Test
    public void retrieveRelationsBetweenTwoNodes2() throws JsonProcessingException {
        // Two-sided retrieval scenario.
        // Retrieve the relations between MTOR and LRRC51.
        ObjectMapper om = new ObjectMapper();
        String uriRelationRetrieval = neo4j.httpURI().resolve("concepts/concept_manager/" + ConceptManager.RETRIEVE_IE_RELATIONS).toString();
        HTTP.Response response = HTTP.POST(uriRelationRetrieval, om.readValue("{\"a_list\":{\"id_property\":\"sourceIds\",\"ids\":[\"2475\"]},\"b_list\":{\"id_property\":\"sourceIds\",\"ids\":[\"120356739\"]},\"relationTypes\":[\"phosphorylation\"]}", RelationRetrievalRequest.class));
        assertThat(response.status()).isEqualTo(200);
        List<Map<String, Object>> result = response.content();
        assertThat(result).hasSize(1);
        // Query: a: MTOR, b: LRRC51
        Map[] expectedResults = {
                Map.of("arg1Name", "MTOR", "arg2Name", "LRRC51", "arg1Id", "genegroup2475", "arg2Id", "toporthology1", "count", 4)};
        assertThat(result).contains(expectedResults);

        // The same but give directly the aggregate IDs
        response = HTTP.POST(uriRelationRetrieval, om.readValue("{\"a_list\":{\"id_property\":\"sourceIds\",\"ids\":[\"genegroup2475\"]},\"b_list\":{\"id_property\":\"sourceIds\",\"ids\":[\"toporthology1\"]},\"relationTypes\":[\"phosphorylation\"]}", RelationRetrievalRequest.class));
        assertThat(response.status()).isEqualTo(200);
         result = response.content();
        assertThat(result).hasSize(1);
        // Query: a: MTOR, b: LRRC51
        assertThat(result).contains(expectedResults);
    }

    @Test
    public void absearch() throws JsonProcessingException {
        // Two-sided retrieval scenario.
        // Retrieve the relations between LRRC51/SCYL3 on the one side and LOC117183042 on the other.
        // The result is small since LRR51 does not have any relations to LOC117183042.
        ObjectMapper om = new ObjectMapper();
        String uriRelationRetrieval = neo4j.httpURI().resolve("concepts/concept_manager/" + ConceptManager.RETRIEVE_IE_RELATIONS).toString();
        HTTP.Response response = HTTP.POST(uriRelationRetrieval, om.readValue("{\"a_list\":{\"id_property\":\"sourceIds\",\"ids\":[\"120356739\",\"105927877\"]},\"b_list\":{\"id_property\":\"sourceIds\",\"ids\":[\"117183042\"]},\"relationTypes\":[\"phosphorylation\"]}", RelationRetrievalRequest.class));
        assertThat(response.status()).isEqualTo(200);
        List<Map<String, Object>> result = response.content();
        assertThat(result).hasSize(1);
        Map[] expectedResults = {
                Map.of("arg1Name", "SCYL3", "arg2Name", "LOC117183042", "arg1Id", "genegroup57147", "arg2Id", "117183042", "count", 1)};
        assertThat(result).contains(expectedResults);
    }

    @Test
    public void absearch2() throws JsonProcessingException {
        // Two-sided retrieval scenario.
        // Retrieve the relations between LOC117183042/MTOR on the one side and LRRC51/SCYL3 on the other.
        // We mix node levels: aggregates and element IDs. It should all be the same.
        ObjectMapper om = new ObjectMapper();
        String uriRelationRetrieval = neo4j.httpURI().resolve("concepts/concept_manager/" + ConceptManager.RETRIEVE_IE_RELATIONS).toString();
        HTTP.Response response = HTTP.POST(uriRelationRetrieval, om.readValue("{\"a_list\":{\"id_property\":\"sourceIds\",\"ids\":[\"117183042\",\"genegroup2475\"]},\"b_list\":{\"id_property\":\"sourceIds\",\"ids\":[\"toporthology1\",\"105927877\"]},\"relationTypes\":[\"phosphorylation\",\"regulation\"]}", RelationRetrievalRequest.class));
        assertThat(response.status()).isEqualTo(200);
        List<Map<String, Object>> result = response.content();
        assertThat(result).hasSize(3);
        Map[] expectedResults = {
                Map.of("arg1Name", "MTOR", "arg2Name", "SCYL3", "arg1Id", "genegroup2475", "arg2Id", "genegroup57147", "count", 2),
                Map.of("arg1Name", "MTOR", "arg2Name", "LRRC51", "arg1Id", "genegroup2475", "arg2Id", "toporthology1", "count", 4),
                Map.of("arg1Name", "LOC117183042", "arg2Name","SCYL3", "arg1Id","117183042", "arg2Id","genegroup57147", "count",1)};
        assertThat(result).contains(expectedResults);
    }
}