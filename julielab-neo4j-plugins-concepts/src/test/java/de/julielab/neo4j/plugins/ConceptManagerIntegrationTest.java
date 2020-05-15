package de.julielab.neo4j.plugins;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.harness.junit.rule.Neo4jRule;
import org.neo4j.test.server.HTTP;

import static de.julielab.neo4j.plugins.ConceptManager.GET_FACET_ROOTS;

public class ConceptManagerIntegrationTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withUnmanagedExtension(  "/app", ConceptManager.class );

    @Test
    public void testGetFacetRootsWithLimit() throws Exception {
        HTTP.Response get = HTTP.GET(neo4j.httpURI().resolve("app/concept_manager/"+ GET_FACET_ROOTS).toString());
        System.out.println(get);
        // Given
//        HTTP.Response response = HTTP.GET( HTTP.GET( neo4j.httpURI().resolve( CM_REST_ENDPOINT).toString() ).location() );

        // Then
//        assertEquals( 200, response.status() );


        // the exact same test as testGetAllFacetRoots() but with a limit on
        // maximum roots
//        ConceptManager tm = new ConceptManager(graphDBMS);
//        ImportConcepts testTerms = getTestTerms(3);
//        tm.insertConcepts(ConceptsJsonSerializer.toJson(testTerms));
//        // Insert two times so we have two facets
//        tm.insertConcepts(ConceptsJsonSerializer.toJson(testTerms));
//        RecursiveMappingRepresentation facetRoots = (RecursiveMappingRepresentation) tm
//                .getFacetRoots("NodeIDPrefixConstants.FACET + 0, NodeIDPrefixConstants.FACET + 1",null, 2);
//        Map<String, Object> map = facetRoots.getUnderlyingMap();
//
//        try (Transaction ignored = graphDb.beginTx()) {
//            // Roots of two facets
//            assertEquals(0, map.size());
//        }
    }
//
//    @SuppressWarnings("unchecked")
//    @Test
//    public void testGetSpecificFacetRoots() throws Exception {
//        ConceptManager tm = new ConceptManager(graphDBMS);
//        ImportConcepts testTerms = getTestTerms(3);
//        // Insert three times so we have three facets
//        tm.insertConcepts(ConceptsJsonSerializer.toJson(testTerms));
//        testTerms.getFacet().setName("secondfacet");
//        tm.insertConcepts(ConceptsJsonSerializer.toJson(testTerms));
//        testTerms.getFacet().setName("thirdfacet");
//        tm.insertConcepts(ConceptsJsonSerializer.toJson(testTerms));
//
//        Map<String, List<String>> requestedRoots = new HashMap<>();
//        requestedRoots.put(NodeIDPrefixConstants.FACET + 0, Lists.newArrayList(NodeIDPrefixConstants.TERM + 0));
//        requestedRoots.put(NodeIDPrefixConstants.FACET + 1,
//                Lists.newArrayList(NodeIDPrefixConstants.TERM + 1, NodeIDPrefixConstants.TERM + 2));
//        // for the third facet, we want all roots returned
//
//        RecursiveMappingRepresentation facetRoots = (RecursiveMappingRepresentation) tm.getFacetRoots(
//                ConceptsJsonSerializer.toJson(Map.of(KEY_FACET_IDS, List.of(NodeIDPrefixConstants.FACET + 0,
//                        NodeIDPrefixConstants.FACET + 1, NodeIDPrefixConstants.FACET + 2), KEY_CONCEPT_IDS, requestedRoots)));
//        Map<String, Object> map = facetRoots.getUnderlyingMap();
//
//        try (Transaction ignored = graphDb.beginTx()) {
//            // Roots of two facets
//            assertEquals(3, map.size());
//            List<Node> roots = (List<Node>) map.get(NodeIDPrefixConstants.FACET + 0);
//            List<String> rootIds = new ArrayList<>();
//            for (Node root : roots)
//                rootIds.add((String) root.getProperty(PROP_ID));
//
//            assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 0));
//            assertFalse(rootIds.contains(NodeIDPrefixConstants.TERM + 1));
//            assertFalse(rootIds.contains(NodeIDPrefixConstants.TERM + 2));
//
//            roots = (List<Node>) map.get(NodeIDPrefixConstants.FACET + 1);
//            rootIds = new ArrayList<>();
//            for (Node root : roots)
//                rootIds.add((String) root.getProperty(PROP_ID));
//            assertFalse(rootIds.contains(NodeIDPrefixConstants.TERM + 0));
//            assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 1));
//            assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 2));
//
//            roots = (List<Node>) map.get(NodeIDPrefixConstants.FACET + 2);
//            rootIds = new ArrayList<>();
//            for (Node root : roots)
//                rootIds.add((String) root.getProperty(PROP_ID));
//            assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 0));
//            assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 1));
//            assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 2));
//        }
//    }

//    @SuppressWarnings("unchecked")
//    @Test
//    public void testGetAllFacetRoots() throws Exception {
//        ConceptManager tm = new ConceptManager(graphDBMS);
//        ImportConcepts testTerms = getTestTerms(3);
//        tm.insertConcepts(ConceptsJsonSerializer.toJson(testTerms));
//        // Insert two times so we have two facets
//        testTerms.getFacet().setName("secondfacet");
//        tm.insertConcepts(ConceptsJsonSerializer.toJson(testTerms));
//        RecursiveMappingRepresentation facetRoots = (RecursiveMappingRepresentation) tm
//                .getFacetRoots(ConceptsJsonSerializer.toJson(Map.of(KEY_FACET_IDS, List.of(NodeIDPrefixConstants.FACET + 0, NodeIDPrefixConstants.FACET + 1))));
//        Map<String, Object> map = facetRoots.getUnderlyingMap();
//
//        try (Transaction ignored = graphDb.beginTx()) {
//            // Roots of two facets
//            assertEquals(2, map.size());
//            List<Node> roots = (List<Node>) map.get(NodeIDPrefixConstants.FACET + 0);
//            List<String> rootIds = new ArrayList<>();
//            for (Node root : roots)
//                rootIds.add((String) root.getProperty(PROP_ID));
//
//            assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 0));
//            assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 1));
//            assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 2));
//
//            roots = (List<Node>) map.get(NodeIDPrefixConstants.FACET + 1);
//            rootIds = new ArrayList<>();
//            for (Node root : roots)
//                rootIds.add((String) root.getProperty(PROP_ID));
//            assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 0));
//            assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 1));
//            assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 2));
//        }
//    }
//

}
