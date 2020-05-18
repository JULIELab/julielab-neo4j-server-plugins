package de.julielab.neo4j.plugins;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import de.julielab.neo4j.plugins.concepts.ConceptManager;
import de.julielab.neo4j.plugins.datarepresentation.*;
import de.julielab.neo4j.plugins.datarepresentation.constants.*;
import de.julielab.neo4j.plugins.datarepresentation.util.ConceptsJsonSerializer;
import de.julielab.neo4j.plugins.test.TestUtilities;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.*;
import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.RecursiveMappingRepresentation;
import org.neo4j.server.rest.repr.Representation;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Method;
import java.util.*;

import static de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants.SRC_TYPE_HIERARCHICAL;
import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_ID;
import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_NAME;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class FacetManagerTest {


    private static GraphDatabaseService graphDb;
    private static DatabaseManagementService graphDBMS;

    @BeforeClass
    public static void initialize() {
        graphDBMS = TestUtilities.getGraphDBMS();
        graphDb = graphDBMS.database(DEFAULT_DATABASE_NAME);
    }

    public static ImportFacet getImportFacet() {
        return getTestFacetMap(1);
    }

    public static ImportFacet getTestFacetMap(int n) {

        ImportFacetGroup facetGroup = new ImportFacetGroup("facetGroup1", 1,
                Lists.newArrayList("showForSearch"));
        return new ImportFacet(facetGroup, "testfacet" + n, "testfacet" + n, "testfacet" + n,
                SRC_TYPE_HIERARCHICAL, Collections.singletonList("hidden"), false);
    }

    @AfterClass
    public static void shutdown() {
        graphDBMS.shutdown();
    }

    @Before
    public void cleanForTest() {
        TestUtilities.deleteEverythingInDB(graphDb);
    }

    @Test
    public void testCreateFacetGroup() throws Exception {
        ImportFacetGroup importFacetGroup1 = new ImportFacetGroup("Test Facet Group");
        importFacetGroup1.position = 1;
        importFacetGroup1.labels = Lists.newArrayList("showForBTerms");

        ImportFacetGroup importFacetGroup2 = new ImportFacetGroup("Test Facet Group 2");
        importFacetGroup2.position = 2;
        importFacetGroup2.labels = Lists.newArrayList("showForSearch");

        FacetManager fm = new FacetManager(graphDBMS);
        Method createFacetGroupMethod = FacetManager.class.getDeclaredMethod("createFacetGroup", Transaction.class,
                Node.class, ImportFacetGroup.class);
        createFacetGroupMethod.setAccessible(true);
        try (Transaction tx = graphDb.beginTx()) {
            Node facetGroupsNode = FacetManager.getFacetGroupsNode(tx);
            Node facetGroupNode = (Node) createFacetGroupMethod.invoke(fm, tx,
                    facetGroupsNode, importFacetGroup1);
            assertEquals("fgid0", facetGroupNode.getProperty(FacetGroupConstants.PROP_ID));
            assertEquals("Test Facet Group",
                    facetGroupNode.getProperty(FacetGroupConstants.PROP_NAME));
            assertEquals(1, facetGroupNode.getProperty(FacetGroupConstants.PROP_POSITION));
            assertTrue(facetGroupNode.hasLabel(Label.label("showForBTerms")));
            assertFalse(facetGroupNode.hasLabel(Label.label("showForSearch")));

            Node facetGroupNode2 = (Node) createFacetGroupMethod.invoke(fm, tx,
                    facetGroupsNode, importFacetGroup2);
            assertEquals("fgid1", facetGroupNode2.getProperty(ConceptConstants.PROP_ID));
            assertEquals("Test Facet Group 2", facetGroupNode2.getProperty(ConceptConstants.PROP_NAME));
            assertEquals(2, facetGroupNode2.getProperty(FacetGroupConstants.PROP_POSITION));
            assertFalse(facetGroupNode2.hasLabel(Label.label("showForBTerms")));
            assertTrue(facetGroupNode2.hasLabel(Label.label("showForSearch")));

            // Check whether the new facet group node is correctly connected. It
            // should be:
            // facetGroups --> facetGroup["Test Facets"]
            Relationship facetGroupRel = facetGroupNode.getSingleRelationship(
                    FacetManager.EdgeTypes.HAS_FACET_GROUP, Direction.INCOMING);
            assertEquals(facetGroupsNode, facetGroupRel.getStartNode());
            assertEquals("facetGroups", facetGroupsNode.getProperty(PROP_NAME));
            tx.commit();
        }

    }

    @Test
    public void testGetFacetGroupsNode() {
        try (Transaction tx = graphDb.beginTx()) {
            assertNull("In the beginning, there was no facet groups node",
                    tx.findNode(
                            NodeConstants.Labels.ROOT, PROP_NAME, FacetConstants.NAME_FACET_GROUPS));
            Node facetGroupsNode = FacetManager.getFacetGroupsNode(tx);
            assertNotNull("One facet groups node should be there.", facetGroupsNode);
            // Get the node multiple times to make sure it isn't created twice.
            FacetManager.getFacetGroupsNode(tx);
            FacetManager.getFacetGroupsNode(tx);
            FacetManager.getFacetGroupsNode(tx);
            facetGroupsNode = tx.findNode(
                    NodeConstants.Labels.ROOT, PROP_NAME, FacetConstants.NAME_FACET_GROUPS);
            assertNotNull("There is one facet groups node", facetGroupsNode);
        }
    }

    @Test
    public void testCreateMinimalFacet() {
        ImportFacetGroup facetGroupMap = new ImportFacetGroup("facetGroup1");
        facetGroupMap.position = 1;
        facetGroupMap.labels = Lists.newArrayList("showForSearch");

        ImportFacet facetMap = new ImportFacet(facetGroupMap, null, "testfacet1", null, SRC_TYPE_HIERARCHICAL);
        try (Transaction tx = graphDb.beginTx()) {
            Node facet = FacetManager.createFacet(tx, facetMap);
            assertNotNull(facet);
        }
    }

    @Test
    public void testCreateFacet() {
        ImportFacet facetMap = getTestFacetMap(1);
        facetMap.setLabels(Lists.newArrayList("uniqueLabel1", "uniqueLabel2"));

        try (Transaction tx = graphDb.beginTx()) {
            // Check whether the facet itself has been created correctly.
            Node facet = FacetManager.createFacet(tx, facetMap);
            assertEquals("testfacet1", facet.getProperty(PROP_NAME));
            assertTrue(facet.hasLabel(Label.label("uniqueLabel1")));
            assertTrue(facet.hasLabel(Label.label("uniqueLabel2")));

            // Check whether the connection to the facet group node is as
            // expected.
            Relationship hasFacetRel = facet.getSingleRelationship(
                    FacetManager.EdgeTypes.HAS_FACET, Direction.INCOMING);
            Node facetGroupNode = hasFacetRel.getStartNode();
            assertEquals("facetGroup1", facetGroupNode.getProperty(PROP_NAME));
            assertEquals(1, facetGroupNode.getProperty(FacetGroupConstants.PROP_POSITION));
            assertTrue(facetGroupNode.hasLabel(Label.label("showForSearch")));
            Relationship hasFacetGroupRel = facetGroupNode.getSingleRelationship(
                    FacetManager.EdgeTypes.HAS_FACET_GROUP, Direction.INCOMING);
            Node facetGroupsNode = hasFacetGroupRel.getStartNode();
            assertEquals("facetGroups", facetGroupsNode.getProperty(PROP_NAME));
            tx.commit();
        }

        // Let's see what happens when we create a second facet. There should be
        // two facets connected to a single facetGroups node.
        facetMap = getTestFacetMap(2);
        try (Transaction tx = graphDb.beginTx()) {
            FacetManager.createFacet(tx, facetMap);
            Node facetGroups = FacetManager.getFacetGroupsNode(tx);
            Node facetGroup1 = facetGroups.getSingleRelationship(
                    FacetManager.EdgeTypes.HAS_FACET_GROUP, Direction.OUTGOING).getEndNode();
            Iterable<Relationship> facetRels = facetGroup1.getRelationships(Direction.OUTGOING,
                    FacetManager.EdgeTypes.HAS_FACET);
            Iterator<Relationship> facetIt = facetRels.iterator();
            // check if the expected facets have been found; order is not
            // important (and - most of all - not maintained by Neo4j!)
            Set<String> expectedFacets = new HashSet<>();
            expectedFacets.add("testfacet1");
            expectedFacets.add("testfacet2");
            assertTrue("There should be a first facet.", facetIt.hasNext());
            Node facet1 = facetIt.next().getEndNode();
            assertTrue(expectedFacets.remove(facet1.getProperty(PROP_NAME)));
            assertTrue("There should be a second facet.", facetIt.hasNext());
            Node facet2 = facetIt.next().getEndNode();
            assertTrue(expectedFacets.remove(facet2.getProperty(PROP_NAME)));
            assertTrue(expectedFacets.isEmpty());
            tx.commit();
        }

    }

    @Test
    public void testInsertFacets() throws Exception {
        List<ImportFacet> jsonFacets = new ArrayList<>();
        ImportFacet facetMap = getTestFacetMap(1);
        jsonFacets.add(facetMap);

        facetMap = getTestFacetMap(2);
        jsonFacets.add(facetMap);

        Gson gson = new Gson();
        String jsonFacetsString = gson.toJson(jsonFacets);

        FacetManager fm = new FacetManager(graphDBMS);
        ListRepresentation response = fm.insertFacets(jsonFacetsString);
        Iterable<Representation> content = TestUtilities.getListFromListRepresentation(response);

        Iterator<Representation> it = content.iterator();
        RecursiveMappingRepresentation facetResponse = (RecursiveMappingRepresentation) it.next();
        assertEquals(NodeIDPrefixConstants.FACET + "0",
                facetResponse.getUnderlyingMap().get(PROP_ID));
        assertEquals("testfacet1", facetResponse.getUnderlyingMap().get(PROP_NAME));

        facetResponse = (RecursiveMappingRepresentation) it.next();
        assertEquals(NodeIDPrefixConstants.FACET + "1",
                facetResponse.getUnderlyingMap().get(PROP_ID));
        assertEquals("testfacet2", facetResponse.getUnderlyingMap().get(PROP_NAME));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetFacets() throws Exception {
        new Indexes(graphDBMS).createIndexes((String) null);
        // To be able to get facets, we first have to insert a few.
        List<ImportFacet> facetMaps = new ArrayList<>();
        facetMaps.add(getTestFacetMap(1));
        facetMaps.add(getTestFacetMap(2));
        facetMaps.add(getTestFacetMap(3));
        facetMaps.add(getTestFacetMap(4));
        facetMaps.add(getTestFacetMap(5));

        FacetManager fm = new FacetManager(graphDBMS);
        Gson gson = new Gson();
        fm.insertFacets(gson.toJson(facetMaps));

        // Additionally, we insert a few terms so we later can tell these terms
        // have NOT
        // been returned by the get facets method.
        // Each facet requires at least one term to be returned by
        // FacetManager#getFacets. I.e. the last facet should
        // not be returned since we don't add terms for it.

        ImportConcepts importConcepts0 = new ImportConcepts(
                Lists.newArrayList(new ImportConcept("prefname", new ConceptCoordinates("TERM", "TEST_DATA", CoordinateType.SRC))), new ImportFacet(
                NodeIDPrefixConstants.FACET + "0"));
        ImportConcepts importConcepts1 = new ImportConcepts(
                Lists.newArrayList(new ImportConcept("prefname", new ConceptCoordinates("TERM1", "TEST_DATA", CoordinateType.SRC))), new ImportFacet(
                NodeIDPrefixConstants.FACET + "1"));
        ImportConcepts importConcepts2 = new ImportConcepts(
                Lists.newArrayList(new ImportConcept("prefname", new ConceptCoordinates("TERM2", "TEST_DATA", CoordinateType.SRC))), new ImportFacet(
                NodeIDPrefixConstants.FACET + "2"));
        ImportConcepts importConcepts3 = new ImportConcepts(
                Lists.newArrayList(new ImportConcept("prefname", new ConceptCoordinates("TERM3", "TEST_DATA", CoordinateType.SRC))), new ImportFacet(
                NodeIDPrefixConstants.FACET + "3"));

        ConceptManager ftm = new ConceptManager(graphDBMS);
        ftm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(importConcepts0).getBytes(UTF_8)));
        ftm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(importConcepts1).getBytes(UTF_8)));
        ftm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(importConcepts2).getBytes(UTF_8)));
        ftm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(importConcepts3).getBytes(UTF_8)));

        // Get the facets and check that everything is alright.
        RecursiveMappingRepresentation facetRep = (RecursiveMappingRepresentation) fm.getFacets(false);
        Map<String, ?> underlyingMap = facetRep.getUnderlyingMap();
        // There should be one element, viz. "facetGroups".
        assertEquals(1, underlyingMap.size());
        // Lets check the facet group itself.
        List<Map<String, Object>> facetGroups = (List<Map<String, Object>>) underlyingMap
                .get("facetGroups");
        // Now we should have a list of facet groups where each facet groups has
        // its list of facets. In this test, there should be only one facet
        // group.
        assertEquals(1, facetGroups.size());
        // Get this one facet group and check its values.
        Map<String, Object> facetGroup = facetGroups.get(0);
        assertEquals("facetGroup1", facetGroup.get(FacetGroupConstants.PROP_NAME));
        assertEquals(1, facetGroup.get(FacetGroupConstants.PROP_POSITION));

        List<Node> facets = (List<Node>) facetGroup.get("facets");
        assertEquals(4, facets.size());
    }

    @Test
    public void testGetFacetSize() {
        new Indexes(graphDBMS).createIndexes((String) null);
        int amount = 10;
        String facet = "fid0";

        ImportConcepts importConcepts = ConceptManagerTest.getTestConcepts(amount);
        ConceptManager termManager = new ConceptManager(graphDBMS);
        termManager.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(importConcepts).getBytes(UTF_8)));;
        FacetManager facetManager = new FacetManager(graphDBMS);

        assertEquals(amount, facetManager.getFacetSize(facet));
    }
}
