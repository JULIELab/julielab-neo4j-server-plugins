package de.julielab.neo4j.plugins;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import de.julielab.neo4j.plugins.FacetManager.FacetLabel;
import de.julielab.neo4j.plugins.auxiliaries.PropertyUtilities;
import de.julielab.neo4j.plugins.auxiliaries.semedico.NodeUtilities;
import de.julielab.neo4j.plugins.concepts.*;
import de.julielab.neo4j.plugins.datarepresentation.*;
import de.julielab.neo4j.plugins.datarepresentation.constants.*;
import de.julielab.neo4j.plugins.datarepresentation.util.ConceptsJsonSerializer;
import de.julielab.neo4j.plugins.test.TestUtilities;
import org.apache.commons.lang.StringUtils;
import org.glassfish.jersey.message.internal.OutboundJaxrsResponse;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphalgo.BasicEvaluationContext;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.*;

import java.io.ByteArrayInputStream;
import java.util.*;
import java.util.stream.Stream;

import static de.julielab.neo4j.plugins.concepts.ConceptLabel.CONCEPT;
import static de.julielab.neo4j.plugins.concepts.ConceptManager.FULLTEXT_INDEX_CONCEPTS;
import static de.julielab.neo4j.plugins.concepts.ConceptManager.KEY_CONCEPT_TERMS;
import static de.julielab.neo4j.plugins.datarepresentation.CoordinateType.SRC;
import static de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants.*;
import static de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants.NAME_NO_FACET_GROUPS;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class ConceptManagerTest {

    private static GraphDatabaseService graphDb;
    private static DatabaseManagementService graphDBMS;

    @BeforeClass
    public static void initialize() {
        graphDBMS = TestUtilities.getGraphDBMS();
        graphDb = graphDBMS.database(DEFAULT_DATABASE_NAME);
        System.setProperty(ConceptLookup.SYSPROP_ID_CACHE_ENABLED, "false");
    }

    @AfterClass
    public static void shutdown() {
        graphDBMS.shutdown();
    }

    /**
     * Returns such a map:
     * <tt>{"facet":&lt;facetMap&gt;, "terms":&lt;list of one map for each term&gt;}</tt>
     *
     * @param amount The amount of terms to generate.
     * @return The created import concepts, an ImportFacet included.
     */
    public static ImportConcepts getTestConcepts(int amount) {
        return getTestConcepts(amount, 0);
    }

    /**
     * Returns such a map:
     * <tt>{"facet":&lt;facetMap&gt;, "terms":&lt;list of one map for each term&gt;}</tt>
     *
     * @param amount The amount of terms to generate.
     * @return The created import concepts, an ImportFacet included.
     */
    public static ImportConcepts getTestConcepts(int amount, int startAt) {
        List<ImportConcept> termList = new ArrayList<>(amount);
        for (int i = startAt; i < amount + startAt; i++) {
            ConceptCoordinates coordinates = new ConceptCoordinates("CONCEPT" + i, "TEST_DATA", "CONCEPT" + i, "TEST_DATA",
                    false);
            ImportConcept term = new ImportConcept("prefname" + i, "desc of term" + i, coordinates);
            termList.add(term);
        }

        return new ImportConcepts(termList, FacetManagerTest.getImportFacet());
    }

    @Before
    public void cleanForTest() {
        TestUtilities.deleteEverythingInDB(graphDb);
        new Indexes(graphDBMS).createIndexes((String) null);
    }

    @Test
    public void testOriginalIdMerging() {
        // This test checks whether multiple, different sources and terms with
        // the same origin (same original ID and
        // original source) are stored correctly.
        ConceptManager tm = new ConceptManager(graphDBMS);

        ImportConcepts testTerms;
        testTerms = getTestConcepts(1);
        testTerms.getConceptsAsList().get(0).coordinates.originalId = "orgId";
        testTerms.getConceptsAsList().get(0).coordinates.originalSource = "src1";
        testTerms.getConceptsAsList().get(0).coordinates.source = "src1";
        tm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms).getBytes(UTF_8)));

        testTerms = getTestConcepts(1);
        testTerms.getConceptsAsList().get(0).coordinates.originalId = "orgId";
        testTerms.getConceptsAsList().get(0).coordinates.originalSource = "src1";
        testTerms.getConceptsAsList().get(0).coordinates.source = "src2";
        tm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms).getBytes(UTF_8)));

        testTerms = getTestConcepts(1);
        testTerms.getConceptsAsList().get(0).coordinates.sourceId = "CONCEPT42";
        testTerms.getConceptsAsList().get(0).coordinates.originalId = "orgId";
        testTerms.getConceptsAsList().get(0).coordinates.originalSource = "src1";
        testTerms.getConceptsAsList().get(0).coordinates.source = null;
        tm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms).getBytes(UTF_8)));

        try (Transaction tx = graphDb.beginTx()) {
            Node term = tx.findNode(CONCEPT,
                    PROP_ID, NodeIDPrefixConstants.TERM + 0);
            assertNotNull(term);
            assertEquals("orgId", term.getProperty(PROP_ORG_ID));
            String[] sourceIds = de.julielab.neo4j.plugins.auxiliaries.semedico.NodeUtilities.getSourceIds(term);
            String[] sources = (String[]) term.getProperty(ConceptConstants.PROP_SOURCES);
            assertArrayEquals(new String[]{"src1", "src2", "<unknown>"}, sources);
            assertArrayEquals(new String[]{"CONCEPT0", "CONCEPT0", "CONCEPT42"}, sourceIds);
            tx.commit();
        }

    }


    @Test
    public void testMergeOnOriginalIdWithoutSourceId() {
        // This test assures that pure term merging works by addressing terms
        // via their original ID without
        // specification of their source ID.
        ConceptManager cm = new ConceptManager(graphDBMS);

        ImportConcepts importConcepts;
        importConcepts = getTestConcepts(1);
        importConcepts.getConceptsAsList().get(0).coordinates.originalId = "orgId";
        importConcepts.getConceptsAsList().get(0).coordinates.originalSource = "src1";
        importConcepts.getConceptsAsList().get(0).coordinates.source = "someSource";
        importConcepts.getConceptsAsList().get(0).descriptions = null;
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(importConcepts).getBytes(UTF_8)));

        try (Transaction tx = graphDb.beginTx()) {
            Node term = tx.findNode(CONCEPT,
                    PROP_ID, NodeIDPrefixConstants.TERM + 0);
            assertNotNull(term);
            assertFalse(term.hasProperty(PROP_DESCRIPTIONS));
            tx.commit();
        }

        // Now add a description, only by knowing the term's original ID.
        importConcepts = getTestConcepts(1);
        importConcepts.getConceptsAsList().get(0).coordinates.originalId = "orgId";
        importConcepts.getConceptsAsList().get(0).coordinates.originalSource = "src1";
        importConcepts.getConceptsAsList().get(0).coordinates.sourceId = null;
        importConcepts.getConceptsAsList().get(0).descriptions = Lists.newArrayList("desc");
        ImportOptions importOptions = new ImportOptions();
        importOptions.merge = true;
        importConcepts.setImportOptions(importOptions);
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(importConcepts).getBytes(UTF_8)));

        try (Transaction tx = graphDb.beginTx()) {
            Node term = tx.findNode(CONCEPT,
                    PROP_ID, NodeIDPrefixConstants.TERM + 0);
            assertNotNull(term);
            assertTrue(term.hasProperty(PROP_DESCRIPTIONS));
            assertArrayEquals(new String[]{"desc"}, (String[]) term.getProperty(PROP_DESCRIPTIONS));
            assertArrayEquals(new String[]{"someSource"}, (String[]) term.getProperty(PROP_SOURCES));
            tx.commit();
        }

    }

    @Test
    public void testSameOrgIdDifferentSource() {
        // This test assures that two terms with the same original ID are still
        // different if the original sources
        // differ.
        ConceptManager tm = new ConceptManager(graphDBMS);

        ImportConcepts testTerms;
        testTerms = getTestConcepts(1);
        testTerms.getConceptsAsList().get(0).coordinates.originalId = "orgId";
        testTerms.getConceptsAsList().get(0).coordinates.originalSource = "src1";
        // we also have to set the source ID and source of at least one term to
        // a different value or we will get an exception telling us that our
        // data is inconsistent (because source ID and source would match but
        // the original ID and original source wouldn't)
        testTerms.getConceptsAsList().get(0).coordinates.sourceId = "anothersourceid";
        testTerms.getConceptsAsList().get(0).coordinates.source = "anothersource";
        tm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms).getBytes(UTF_8)));

        testTerms = getTestConcepts(1);
        testTerms.getConceptsAsList().get(0).coordinates.originalId = "orgId";
        testTerms.getConceptsAsList().get(0).coordinates.originalSource = "src2";
        tm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms).getBytes(UTF_8)));

        try (Transaction tx = graphDb.beginTx()) {
            ResourceIterable<Node> terms = () -> tx.findNodes(CONCEPT);
            int counter = 0;
            Set<String> idSet = Sets.newHashSet("src1", "src2");
            for (Node term : terms) {
                assertEquals("orgId", term.getProperty(PROP_ORG_ID));
                assertTrue(idSet.remove(term.getProperty(PROP_ORG_SRC)));
                counter++;
            }
            // There should be two terms even though they have the same original
            // id
            assertEquals(2, counter);
            tx.commit();
        }

    }

    /**
     * Tests the import of a (small) set of terms together with their complete facet
     * definition. The facet should be created and the terms be created and
     * connected to it.
     */
    @Test
    public void testImportConceptsWithFacetDefinition() throws SecurityException,
            IllegalArgumentException {
        testTermImportWithOrWithoutFacetDefinition(true);
    }

    @Test
    public void testImportConceptsWithoutFacetDefinition() {
        // Here, create the facet "manually" and then do the term import. The
        // import method "knows" which facet Id to use (there will only be this
        // one facet, so its fid0...)
        ImportFacet facetMap = FacetManagerTest.getImportFacet();
        try (Transaction tx = graphDb.beginTx()) {
            FacetManager.createFacet(tx, facetMap);
            tx.commit();
        }

        // Do the term import and tests.
        testTermImportWithOrWithoutFacetDefinition(false);
    }

    private void testTermImportWithOrWithoutFacetDefinition(boolean withFacetDefinition) {
        // ----------- THE FACET --------------
        ImportFacet importFacet;
        if (withFacetDefinition) {
            importFacet = FacetManagerTest.getImportFacet();
        } else {
            // The facet has already been created and only its ID has been
            // given.
            importFacet = new ImportFacet("fid0");
        }

        // ----------- THE CONCEPTS ---------------
        // Structure:
        // CONCEPT1
        // ===== CONCEPT2
        // ---------- CONCEPT4
        // ----- CONCEPT3
        // Note the ===== relationship: We will insert term2 twice but we expect
        // that only ONE relationship is created in the end, i.e. the duplicate
        // is recognized.
        List<ImportConcept> conceptList = new ArrayList<>();
        ConceptCoordinates coord1 = new ConceptCoordinates("CONCEPT1", "TEST_SOURCE", SRC);
        ConceptCoordinates coord2 = new ConceptCoordinates("CONCEPT2", "TEST_SOURCE", "orgId2", "orgSrc1");
        ConceptCoordinates coord3 = new ConceptCoordinates("CONCEPT3", "TEST_SOURCE", SRC);
        ConceptCoordinates coord4 = new ConceptCoordinates("CONCEPT4", "TEST_SOURCE", SRC);
        conceptList.add(new ImportConcept("prefname1", "desc of term1", coord1));
        conceptList.add(new ImportConcept("prefname2", coord2, coord1));
        // duplicate of term 2 to test relationship de-duplication.
        conceptList.add(new ImportConcept("prefname2", coord2, coord1));
        conceptList.add(new ImportConcept("prefname3", coord3, coord1));
        conceptList.add(new ImportConcept("prefname4", coord4, coord2));

        ImportConcepts importConcepts = new ImportConcepts();
        importConcepts.setFacet(importFacet);
        importConcepts.setConcepts(conceptList);

        // --------- CREATE JSON AND SEND DATA --------
        String termsAndFacetBytes = ConceptsJsonSerializer.toJson(importConcepts);
        ConceptManager cm = new ConceptManager(graphDBMS);
        cm.insertConcepts(new ByteArrayInputStream(termsAndFacetBytes.getBytes(UTF_8)));

        // --------- MAKE TESTS ---------------

        // Is the facet there?
        try (Transaction tx = graphDb.beginTx()) {
            Node facet = tx.findNode(FacetManager.FacetLabel.FACET, PROP_ID,
                    "fid0");
            assertEquals("testfacet1", facet.getProperty(PROP_NAME));

            // Are the term Properties correct?
            assertEquals(4, tx.findNodes(CONCEPT).stream().count());

            Node term1 = tx.findNode(CONCEPT, PROP_ID, "tid0");
            assertEquals("prefname1", term1.getProperty(PROP_PREF_NAME));
            assertEquals(Lists.newArrayList("desc of term1"),
                    Arrays.asList((String[]) term1.getProperty(PROP_DESCRIPTIONS)));
            assertEquals("CONCEPT1", term1.getProperty(PROP_SRC_IDS));

            Node term2 = tx.findNode(CONCEPT, PROP_ID, "tid1");
            assertEquals("prefname2", term2.getProperty(PROP_PREF_NAME));
            assertEquals("orgId2", term2.getProperty(PROP_ORG_ID));
            assertEquals("CONCEPT2", term2.getProperty(PROP_SRC_IDS));

            Node term3 = tx.findNode(CONCEPT, PROP_ID, "tid2");
            assertEquals("prefname3", term3.getProperty(PROP_PREF_NAME));
            assertEquals("CONCEPT3", term3.getProperty(PROP_SRC_IDS));

            Node term4 = tx.findNode(CONCEPT, PROP_ID, "tid3");
            assertEquals("prefname4", term4.getProperty(PROP_PREF_NAME));
            assertEquals("CONCEPT4", term4.getProperty(PROP_SRC_IDS));

            // Are the relationships correct? Reminder, they should be:
            // Structure:
            // CONCEPT1
            // ----- CONCEPT2
            // ---------- CONCEPT4
            // ----- CONCEPT3
            // where there is only ONE relationship from CONCEPT1 to CONCEPT2 although
            // we
            // have added CONCEPT2 twice. This is tested automatically by using
            // "getSingleRelationShip".
            // For simplicity, we here ask the child for its parent because in
            // this
            // test, there is only one parent (in general, there might be
            // more!).
            assertEquals(term1,
                    term2.getSingleRelationship(ConceptEdgeTypes.IS_BROADER_THAN, Direction.INCOMING)
                            .getStartNode());
            assertEquals(term1,
                    term3.getSingleRelationship(ConceptEdgeTypes.IS_BROADER_THAN, Direction.INCOMING)
                            .getStartNode());
            assertEquals(term2,
                    term4.getSingleRelationship(ConceptEdgeTypes.IS_BROADER_THAN, Direction.INCOMING)
                            .getStartNode());

            // Besides the default taxonomic relationships, there should be
            // specific relationships only valid for the
            // current facet.
            RelationshipType relBroaderThenInFacet = RelationshipType
                    .withName(ConceptEdgeTypes.IS_BROADER_THAN.toString() + "_fid0");
            assertEquals(term1, term2.getSingleRelationship(relBroaderThenInFacet, Direction.INCOMING).getStartNode());
            assertEquals(term1, term3.getSingleRelationship(relBroaderThenInFacet, Direction.INCOMING).getStartNode());
            assertEquals(term2, term4.getSingleRelationship(relBroaderThenInFacet, Direction.INCOMING).getStartNode());

            PathFinder<Path> pathFinder = GraphAlgoFactory.shortestPath(new BasicEvaluationContext(tx, graphDb), PathExpanders.allTypesAndDirections(), 6);
            Node facetGroupsNode = FacetManager.getFacetGroupsNode(tx);
            Path path = pathFinder.findSinglePath(facetGroupsNode, term1);
            assertNotNull(path);
            path = pathFinder.findSinglePath(facetGroupsNode, term2);
            assertNotNull(path);
            path = pathFinder.findSinglePath(facetGroupsNode, term3);
            assertNotNull(path);
            path = pathFinder.findSinglePath(facetGroupsNode, term4);
            assertNotNull(path);

            tx.commit();
        }
    }

    @Test
    public void testInsertTermIntoMultipleFacets() {
        // Two facets will be created. A term will be added to both, then.
        // The first facet will be sent with the term as whole facet definition.
        // The second facet will be created beforehand and then the term will
        // just be added to it.
        ImportFacet importFacet = FacetManagerTest.getTestFacetMap(1);

        List<ImportConcept> concepts = new ArrayList<>();
        concepts.add(
                new ImportConcept("prefname1", "desc of term1", new ConceptCoordinates("CONCEPT1", "TEST_SOURCE", SRC)));

        // -------- SEND CONCEPT WITH FACET DEFINITION ------
        ConceptManager cm = new ConceptManager(graphDBMS);

        ImportConcepts importConcepts = new ImportConcepts();
        importConcepts.setFacet(importFacet);
        importConcepts.setConcepts(concepts);
        String importConceptsJson = ConceptsJsonSerializer.toJson(importConcepts);
        cm.insertConcepts(new ByteArrayInputStream(importConceptsJson.getBytes(UTF_8)));
        try (Transaction tx = graphDb.beginTx()) {
            // Create the 2nd facet separately.
            importFacet = FacetManagerTest.getTestFacetMap(2);
            FacetManager.createFacet(tx, importFacet);
            tx.commit();
        }
        // ---------- SEND CONCEPT ONLY WITH FACET ID --------
        importFacet = new ImportFacet("fid1");
        importConcepts = new ImportConcepts(concepts, importFacet);
        importConceptsJson = ConceptsJsonSerializer.toJson(importConcepts);
        cm.insertConcepts(new ByteArrayInputStream(importConceptsJson.getBytes(UTF_8)));
        try (Transaction tx = graphDb.beginTx()) {
            // ------------------ MAKE TESTS ---------------

            Node concept = tx.findNode(CONCEPT, PROP_ID, "tid0");
            List<String> fids = List.of((String[]) concept.getProperty(PROP_FACETS));
            assertTrue(fids.contains("fid0"));
            assertTrue(fids.contains("fid1"));
            tx.commit();
        }
    }

    @Test
    public void testInsertNoFacet() {
        // In this test we will just ordinarily insert some terms with a facet -
        // but this facet will be set as a
        // "no-facet" and thus should be moves to the appropriate section of the
        // graph.

        ImportConcepts testTerms = getTestConcepts(5);
        testTerms.getFacet().setNoFacet(true);

        ConceptManager cm = new ConceptManager(graphDBMS);
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms).getBytes(UTF_8)));

        try (Transaction tx = graphDb.beginTx()) {
            Node facetGroupsNode = tx.findNode(NodeConstants.Labels.ROOT,
                    NodeConstants.PROP_NAME, FacetConstants.NAME_FACET_GROUPS);
            assertNull("The facet groups node exists although it should not.", facetGroupsNode);
            Node noFacetGroupsNode = tx.findNode(NodeConstants.Labels.ROOT,
                    NodeConstants.PROP_NAME, FacetConstants.NAME_NO_FACET_GROUPS);
            assertNotNull("The no facet groups node does not exists although it should.", noFacetGroupsNode);

            Node facetGroupNode = NodeUtilities.getSingleOtherNode(noFacetGroupsNode,
                    FacetManager.EdgeTypes.HAS_FACET_GROUP);
            Node facetNode = NodeUtilities.getSingleOtherNode(facetGroupNode, FacetManager.EdgeTypes.HAS_FACET);
            Iterator<Relationship> rootIt = facetNode.getRelationships(ConceptEdgeTypes.HAS_ROOT_CONCEPT)
                    .iterator();
            int rootCount = 0;
            while (rootIt.hasNext()) {
                @SuppressWarnings("unused")
                Relationship root = rootIt.next();
                rootCount++;
            }
            assertEquals("Wrong number of roots for the facet: ", 5, rootCount);

            tx.commit();
        }
    }

    @Test
    public void testMergeConceptProperties() {
        System.setProperty(ConceptLookup.SYSPROP_ID_CACHE_ENABLED, "true");
        try {
            // We will insert the same term (identified by the same original ID)
            // multiple times with additional
            // information each time. At the end, the information that can be merged
            // should be complete.
            ImportFacet facetMap = FacetManagerTest.getImportFacet();

            // ------------ INSERT 1 ---------------

            ImportConcept concept = new ImportConcept("prefname1",
                    new ConceptCoordinates("CONCEPT1", "TEST_SOURCE", "ORGID", "orgSrc1"));

            ConceptManager cm = new ConceptManager(graphDBMS);

            ImportConcepts importConcepts = new ImportConcepts();
            importConcepts.setFacet(facetMap);
            importConcepts.setConcepts(List.of(concept));
            String termsAndFacetJson = ConceptsJsonSerializer.toJson(importConcepts);
            cm.insertConcepts(new ByteArrayInputStream(termsAndFacetJson.getBytes(UTF_8)));

            // ------------ INSERT 2 ---------------
            concept = new ImportConcept("prefname1", "description1",
                    new ConceptCoordinates("CONCEPT1", "TEST_SOURCE", "ORGID", "orgSrc1"));

            importConcepts.setConcepts(List.of(concept));
            termsAndFacetJson = ConceptsJsonSerializer.toJson(importConcepts);
            cm.insertConcepts(new ByteArrayInputStream(termsAndFacetJson.getBytes(UTF_8)));

            // ------------ INSERT 3 ---------------
            concept = new ImportConcept("prefname2", Collections.singletonList("syn1"),
                    new ConceptCoordinates("CONCEPT2", "TEST_SOURCE", "ORGID", "orgSrc1"));

            importConcepts.setConcepts(List.of(concept));
            termsAndFacetJson = ConceptsJsonSerializer.toJson(importConcepts);
            cm.insertConcepts(new ByteArrayInputStream(termsAndFacetJson.getBytes(UTF_8)));

            // ------------ INSERT 4 ---------------
            concept = new ImportConcept("prefname3", Collections.singletonList("syn2"), "description2",
                    new ConceptCoordinates("CONCEPT3", "TEST_SOURCE", "ORGID", "orgSrc1"));

            importConcepts.setConcepts(List.of(concept));
            termsAndFacetJson = ConceptsJsonSerializer.toJson(importConcepts);
            cm.insertConcepts(new ByteArrayInputStream(termsAndFacetJson.getBytes(UTF_8)));

            // ------------ MAKE TESTS ---------------
            try (Transaction tx = graphDb.beginTx()) {
                assertEquals(1, tx.findNodes(CONCEPT).stream().count());
                // We only have one term, thus tid0.
                Node concept1 = tx.findNode(CONCEPT, PROP_ID, "tid0");

                assertEquals("prefname1", concept1.getProperty(PROP_PREF_NAME));
                assertEquals("ORGID", concept1.getProperty(PROP_ORG_ID));

                String[] descs = (String[]) concept1.getProperty(PROP_DESCRIPTIONS);
                assertEquals(2, descs.length);
                Arrays.sort(descs);
                assertEquals(Lists.newArrayList("description1", "description2"), Arrays.asList(descs));
                List<String> synList = Lists.newArrayList((String[]) concept1.getProperty(PROP_SYNONYMS));
                assertTrue(synList.contains("syn1"));
                assertTrue(synList.contains("syn2"));
                List<String> srcIdList = Arrays.asList(Objects.requireNonNull(NodeUtilities.getSourceIds(concept1)));
                assertTrue(srcIdList.contains("CONCEPT1"));
                assertTrue(srcIdList.contains("CONCEPT2"));
                assertTrue(srcIdList.contains("CONCEPT3"));

                tx.commit();
            }
        } finally {
            System.setProperty(ConceptLookup.SYSPROP_ID_CACHE_ENABLED, "false");
        }
    }

    @Test
    public void testMergeTermLabels() {
        // Check if we can insert a concept and then insert the term again but
        // with an additional label so that the new labels gets added to the
        // existing concept
        ImportConcepts testTerms = getTestConcepts(1);
        ConceptManager cm = new ConceptManager(graphDBMS);
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms).getBytes(UTF_8)));

        testTerms = getTestConcepts(1);
        testTerms.setImportOptions(new ImportOptions());
        testTerms.getImportOptions().merge = true;
        testTerms.getConceptsAsList().get(0).addGeneralLabel("ANOTHER_LABEL");
        // just to make am more thorough test, we change the source coordinates (but NOT
        // the original coordinates)
        testTerms.getConceptsAsList().get(0).coordinates.sourceId = "somesrcid";
        testTerms.getConceptsAsList().get(0).coordinates.source = "somesrc";
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms).getBytes(UTF_8)));

        try (Transaction tx = graphDb.beginTx()) {
            // Check that there is only one concept node
            ResourceIterator<Node> conceptNodes = tx.findNodes(CONCEPT);
            int counter = 0;
            while (conceptNodes.hasNext()) {
                @SuppressWarnings("unused")
                Node n = conceptNodes.next();
                ++counter;
            }
            assertEquals(1, counter);
            // end check only one concept node

            // now check that there also is a node with ANOTHER_LABEL
            conceptNodes = tx.findNodes(Label.label("ANOTHER_LABEL"));
            counter = 0;
            while (conceptNodes.hasNext()) {
                @SuppressWarnings("unused")
                Node n = conceptNodes.next();
                ++counter;
            }
            assertEquals(1, counter);
            tx.commit();
        }
    }

    @Test
    public void testImportConceptMultipleTimes() {
        List<ImportConcept> concepts;
        ConceptManager cm;
        concepts = new ArrayList<>();
        ImportFacet importFacet;
        cm = new ConceptManager(graphDBMS);
        ImportConcepts importConcepts;
        concepts.add(new ImportConcept("name0", new ConceptCoordinates("source0", "TEST_SOURCE", SRC)));
        concepts.add(new ImportConcept("name1", new ConceptCoordinates("source1", "TEST_SOURCE", SRC),
                new ConceptCoordinates("source0", "TEST_SOURCE", SRC)));
        importFacet = FacetManagerTest.getImportFacet();
        importConcepts = new ImportConcepts(concepts, importFacet);
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(importConcepts).getBytes(UTF_8)));

        importConcepts = new ImportConcepts(concepts, importFacet);
        // Insert another time.
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(importConcepts).getBytes(UTF_8)));


        try (Transaction tx = graphDb.beginTx()) {
            // Would throw an exception if there were multiple terms found.
            FullTextIndexUtils.getNode(tx, FULLTEXT_INDEX_CONCEPTS, PROP_SRC_IDS, "source0");
            FullTextIndexUtils.getNode(tx, FULLTEXT_INDEX_CONCEPTS, PROP_SRC_IDS, "source1");
        }
    }

    @Test
    public void testImportEdgeMultipleTimes() {
        List<ImportConcept> terms;
        ConceptManager cm;
        terms = new ArrayList<>();
        ImportFacet importFacet;
        cm = new ConceptManager(graphDBMS);
        ImportConcepts importTermAndFacet;

        terms.add(new ImportConcept("name0", new ConceptCoordinates("source0", "TEST_SOURCE", SRC)));
        terms.add(new ImportConcept("name1", new ConceptCoordinates("source1", "TEST_SOURCE", SRC),
                new ConceptCoordinates("source0", "TEST_SOURCE", SRC)));
        importFacet = FacetManagerTest.getImportFacet();
        importTermAndFacet = new ImportConcepts(terms, importFacet);
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(importTermAndFacet).getBytes(UTF_8)));

        try (Transaction tx = graphDb.beginTx()) {
            Node n1 = tx.findNode(CONCEPT,
                    PROP_ID, NodeIDPrefixConstants.TERM + 1);
            assertNotNull(n1);
            Node n0 = NodeUtilities.getSingleOtherNode(n1, ConceptEdgeTypes.IS_BROADER_THAN);
            assertEquals(NodeIDPrefixConstants.TERM + 0, n0.getProperty(PROP_ID));
        }

        terms.clear();
        terms.add(new ImportConcept("name0", new ConceptCoordinates("source0", "TEST_SOURCE", SRC)));
        terms.add(new ImportConcept("name1", new ConceptCoordinates("source1", "TEST_SOURCE", SRC),
                new ConceptCoordinates("source0", "TEST_SOURCE", SRC)));
        importFacet = FacetManagerTest.getImportFacet();
        importTermAndFacet = new ImportConcepts(terms, importFacet);
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(importTermAndFacet).getBytes(UTF_8)));

        try (Transaction tx = graphDb.beginTx()) {
            Node n1 = tx.findNode(CONCEPT,
                    PROP_ID, NodeIDPrefixConstants.TERM + 1);
            Node n0 = NodeUtilities.getSingleOtherNode(n1, ConceptEdgeTypes.IS_BROADER_THAN);
            assertEquals(NodeIDPrefixConstants.TERM + 0, n0.getProperty(PROP_ID));
            tx.commit();
        }

        terms.clear();
        terms.add(new ImportConcept("name1", new ConceptCoordinates("source1", "TEST_SOURCE", SRC),
                new ConceptCoordinates("source0", "TEST_SOURCE", SRC)));
        importFacet = FacetManagerTest.getImportFacet();
        importTermAndFacet = new ImportConcepts(terms, importFacet);
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(importTermAndFacet).getBytes(UTF_8)));

        try (Transaction tx = graphDb.beginTx()) {
            Node n1 = tx.findNode(CONCEPT,
                    PROP_ID, NodeIDPrefixConstants.TERM + 1);
            Node n0 = NodeUtilities.getSingleOtherNode(n1, ConceptEdgeTypes.IS_BROADER_THAN);
            assertEquals(NodeIDPrefixConstants.TERM + 0, n0.getProperty(PROP_ID));
            tx.commit();
        }

    }

    private int countNodesWithLabel(Label label) {
        try (Transaction tx = graphDb.beginTx()) {
            ResourceIterator<Node> it = tx.findNodes(label);
            int nodeCount = 0;
            while (it.hasNext()) {
                it.next();
                nodeCount++;
            }
            tx.commit();
            return nodeCount;
        }
    }

    private int countRelationships(Iterable<Relationship> iterable) {
        int count = 0;
        for (@SuppressWarnings("unused")
                Relationship rel : iterable) {
            ++count;
        }
        return count;
    }

    @Test
    public void testOrganizeSynonyms() {
        // In this test, we have a term that specifies its preferred term as a
        // synonym, too. The FacetConceptManager should detect this and remove
        // the synonym that equals the preferred name.
        List<ImportConcept> conceptList = new ArrayList<>();
        conceptList.add(new ImportConcept("prefname", Arrays.asList("prefname", "othersynonym"), "desc of term",
                new ConceptCoordinates("CONCEPT", "TEST_SOURCE", SRC)));
        ImportConcepts importConcepts = new ImportConcepts();
        importConcepts.setFacet(FacetManagerTest.getImportFacet());
        importConcepts.setConcepts(conceptList);

        String termsJson = ConceptsJsonSerializer.toJson(importConcepts);
        ConceptManager cm = new ConceptManager(graphDBMS);

        cm.insertConcepts(new ByteArrayInputStream(termsJson.getBytes(UTF_8)));

        try (Transaction tx = graphDb.beginTx()) {
            Node concept = FullTextIndexUtils.getNode(tx, FULLTEXT_INDEX_CONCEPTS, PROP_SRC_IDS, "CONCEPT");
            assertEquals("Preferred name", "prefname", concept.getProperty(PROP_PREF_NAME));
            assertEquals("Description", List.of("desc of term"),
                    List.of((String[]) concept.getProperty(PROP_DESCRIPTIONS)));
            assertEquals("Source ID doesn't match", "CONCEPT", concept.getProperty(PROP_SRC_IDS));
            String[] synonyms = (String[]) concept.getProperty(PROP_SYNONYMS);
            assertEquals("Number of synonyms", 1, synonyms.length);
            assertEquals("Synonym", "othersynonym", synonyms[0]);
            tx.commit();
        }
    }

    @Test
    public void testAdditionalRelationships() {
        // Multiple terms will be inserted that are "equal" according to the
        // preferred names. This will be expressed via
        // additional relationships other then the default taxonomic
        // relationship. Also, we will first insert one part of the equal terms
        // and observe that "hollow" terms are
        // created on behalf of the referenced other half. After inserting
        // this other half, there should be no more "hollow" terms.
        ImportConcepts testTermsAndFacet = getTestConcepts(4);
        List<ImportConcept> terms = testTermsAndFacet.getConceptsAsList();
        // Now we just create relationships so that:
        // 0 equals 1
        // 0 equals 3
        // 1 equals 2
        // 2 equals 3
        // 0 2
        // |X|
        // 1 3
        // Note that the test uses "CONCEPT<number>" for source IDs and that we
        // have to define the relationships via source
        // IDs (we don't know the ultimate concept IDs yet).
        String termSource = terms.get(0).coordinates.originalSource;
        terms.get(0).addRelationship(new ImportConceptRelationship(new ConceptCoordinates("CONCEPT" + 1, termSource, false),
                ConceptEdgeTypes.HAS_SAME_NAMES.name()));
        terms.get(0).addRelationship(new ImportConceptRelationship(new ConceptCoordinates("CONCEPT" + 3, termSource, false),
                ConceptEdgeTypes.HAS_SAME_NAMES.name()));
        terms.get(1).addRelationship(new ImportConceptRelationship(new ConceptCoordinates("CONCEPT" + 2, termSource, false),
                ConceptEdgeTypes.HAS_SAME_NAMES.name()));
        terms.get(2).addRelationship(new ImportConceptRelationship(new ConceptCoordinates("CONCEPT" + 3, termSource, false),
                ConceptEdgeTypes.HAS_SAME_NAMES.name()));

        // Now we split the terms in two lists in order to check the behavior of
        // the "hollow" label assignment.
        ImportConcepts firstTerms = new ImportConcepts(terms.subList(0, 2), testTermsAndFacet.getFacet());
        ImportConcepts secondTerms = new ImportConcepts(terms.subList(2, 4), testTermsAndFacet.getFacet());

        ConceptManager cm = new ConceptManager(graphDBMS);

        // Insert the first half of terms.
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(firstTerms).getBytes(UTF_8)));

        try (Transaction tx = graphDb.beginTx()) {
            final Iterator<Node> nodesIt = Stream.concat(tx.findNodes(CONCEPT).stream(), tx.findNodes(ConceptLabel.HOLLOW).stream()).iterator();

            int nodeCount = 0;
            while (nodesIt.hasNext()) {
                Node node = nodesIt.next();
                // Only the real terms have gotten an ID.
                if (node.hasProperty(PROP_ID))
                    assertFalse("Not hollow", node.hasLabel(ConceptLabel.HOLLOW));
                else
                    assertTrue("Node is hollow", node.hasLabel(ConceptLabel.HOLLOW));
                nodeCount++;
            }
            assertEquals("Number of terms", 4, nodeCount);
            tx.commit();
        }

        // Now insert the other half of terms. After this we should still have 4
        // terms, but non of them should be
        // hollow and all should have an ID, a facet and a description.
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(secondTerms).getBytes(UTF_8)));

        try (Transaction tx = graphDb.beginTx()) {
            ResourceIterator<Node> nodesIt = tx.findNodes(CONCEPT);
            while (nodesIt.hasNext()) {
                Node node = nodesIt.next();
                System.out.println(NodeUtilities.getNodePropertiesAsString(node));
            }
            assertEquals(4, tx.findNodes(CONCEPT).stream().count());
            while (nodesIt.hasNext()) {
                Node node = nodesIt.next();
                // Only the real terms have gotten an ID.
                assertFalse("Term " + NodeUtilities.getNodePropertiesAsString(node) + " is hollow but it shouldn't be",
                        node.hasLabel(ConceptLabel.HOLLOW));
                assertTrue("Has an ID", node.hasProperty(PROP_ID));
                assertTrue("Has a facet", node.hasProperty(PROP_FACETS));
                assertTrue("Has a description", node.hasProperty(PROP_DESCRIPTIONS));

                // Check the correctness of the "equal names" relationships. For
                // simplicity, we restrict ourselves to
                // outgoing relationships to test here.
                Iterator<Relationship> relIt = node
                        .getRelationships(Direction.OUTGOING, ConceptEdgeTypes.HAS_SAME_NAMES).iterator();
                String id = (String) node.getProperty(PROP_ID);
                int numRels = 0;
                if (id.endsWith("0")) {
                    while (relIt.hasNext()) {
                        Relationship rel = relIt.next();
                        String targetId = (String) rel.getEndNode().getProperty(PROP_ID);
                        assertTrue(targetId.endsWith("1") || targetId.endsWith("3"));
                        numRels++;
                    }
                    assertEquals("Number of relationships", 2, numRels);
                } else if (id.endsWith("1")) {
                    while (relIt.hasNext()) {
                        Relationship rel = relIt.next();
                        String targetId = (String) rel.getEndNode().getProperty(PROP_ID);
                        assertTrue(targetId.endsWith("2"));
                        numRels++;
                    }
                    assertEquals("Number of relationships", 1, numRels);
                } else if (id.endsWith("2")) {
                    while (relIt.hasNext()) {
                        Relationship rel = relIt.next();
                        String targetId = (String) rel.getEndNode().getProperty(PROP_ID);
                        assertTrue(targetId.endsWith("3"));
                        numRels++;
                    }
                    assertEquals("Number of relationships", 1, numRels);
                }
            }
        }
    }

    @Test
    public void testAdditionalRelationships2() {
        ImportConcepts importConcepts = getTestConcepts(4);
        List<ImportConcept> concepts = importConcepts.getConceptsAsList();
        String termSource = concepts.get(0).coordinates.originalSource;
        ImportConceptRelationship rel1 = new ImportConceptRelationship(new ConceptCoordinates("CONCEPT" + 1, termSource, true),
                ConceptEdgeTypes.HAS_SAME_NAMES.name());
        rel1.addProperty("prop1", "value1");
        rel1.addProperty("prop2", "value2");
        ImportConceptRelationship rel2 = new ImportConceptRelationship(new ConceptCoordinates("CONCEPT" + 1, termSource, true),
                ConceptEdgeTypes.HAS_SAME_NAMES.name());
        rel2.addProperty("prop1", "value1");
        rel2.addProperty("prop3", "value3");
        concepts.get(0).addRelationship(rel1);
        concepts.get(0).addRelationship(rel2);

        ConceptManager tm = new ConceptManager(graphDBMS);
        tm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(importConcepts).getBytes(UTF_8)));

        importConcepts = new ImportConcepts(concepts, importConcepts.getFacet());
        tm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(importConcepts).getBytes(UTF_8)));


        try (Transaction tx = graphDb.beginTx()) {
            Node term0 = tx.findNode(CONCEPT, PROP_ID, NodeIDPrefixConstants.TERM + 0);
            Relationship relationship = term0.getSingleRelationship(ConceptEdgeTypes.HAS_SAME_NAMES,
                    Direction.OUTGOING);
            assertNotNull(relationship);
            assertTrue(relationship.hasProperty("prop1"));
            assertEquals("value1", relationship.getProperty("prop1"));
            assertTrue(relationship.hasProperty("prop2"));
            assertEquals("value2", relationship.getProperty("prop2"));
            assertTrue(relationship.hasProperty("prop3"));
            assertEquals("value3", relationship.getProperty("prop3"));
        }
    }

    @Test
    public void testInsertAggregateTerm() {
        // Here we test the case where an aggregate term is explicitly imported
        // from external data as opposed to
        // computing it within the database.
        ImportConcepts testTerms = getTestConcepts(5);
        List<ImportConcept> terms = testTerms.getConceptsAsList();
        // Add an aggregate term with terms 0-3 as elements. The term on
        // position 4 will stay alone.
        List<String> aggregateElementSrcIds = Lists.newArrayList("CONCEPT" + 0, "CONCEPT" + 1, "CONCEPT" + 2, "CONCEPT" + 3);
        List<String> aggregateElementSources = Lists.newArrayList(terms.get(0).coordinates.source,
                terms.get(1).coordinates.source, terms.get(2).coordinates.source, terms.get(3).coordinates.source);
        List<ConceptCoordinates> aggregateElementCoords = new ArrayList<>();
        for (int i = 0; i < aggregateElementSrcIds.size(); i++) {
            String id = aggregateElementSrcIds.get(i);
            String source = aggregateElementSources.get(i);
            aggregateElementCoords.add(new ConceptCoordinates(id, source, true));
        }
        terms.add(new ImportConcept(aggregateElementCoords,
                Lists.newArrayList(PROP_PREF_NAME, PROP_SYNONYMS, PROP_DESCRIPTIONS)));

        ConceptManager cm = new ConceptManager(graphDBMS);
        ConceptAggregateManager cam = new ConceptAggregateManager(graphDBMS);
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms).getBytes(UTF_8)));

        assertEquals("Number of actual terms", 5, countNodesWithLabel(CONCEPT));
        assertEquals("Number of aggregate terms", 1, countNodesWithLabel(ConceptLabel.AGGREGATE));

        try (Transaction tx = graphDb.beginTx()) {
            ResourceIterator<Node> aggregateIt = tx.findNodes(ConceptLabel.AGGREGATE);
            assertTrue("There is at least one aggregate term", aggregateIt.hasNext());
            Node aggregate = aggregateIt.next();
            assertFalse("There is no second aggregate term", aggregateIt.hasNext());
            Iterable<Relationship> elementRels = aggregate.getRelationships(ConceptEdgeTypes.HAS_ELEMENT);
            int numElementRels = 0;
            for (Relationship elementRel : elementRels) {
                String[] termSrcIds = NodeUtilities.getSourceIds(elementRel.getEndNode());
                assert termSrcIds != null;
                assertTrue("Term is one of the defined aggregate elements",
                        aggregateElementSrcIds.contains(termSrcIds[0]));
                numElementRels++;
            }
            assertEquals("There are 4 elements", 4, numElementRels);

            Node term = FullTextIndexUtils.getNode(tx, FULLTEXT_INDEX_CONCEPTS, PROP_SRC_IDS, "CONCEPT4");
            assertNotNull("Term on position 4 was inserted and found", term);
            Iterable<Relationship> relationships = term.getRelationships();
            int numRels = 0;
            for (Relationship rel : relationships) {
                numRels++;
                assertEquals(rel.getType().name(), ConceptEdgeTypes.HAS_ROOT_CONCEPT.toString());
            }
            assertEquals("Term on position 4 has exactly one relationship (HAS_ROOT)", 1, numRels);
            // Now let's copy the term properties into the aggregate.
            OutboundJaxrsResponse report = (OutboundJaxrsResponse) cam.copyAggregateProperties();
            Map<String, ?> reportMap = (Map<String, ?>) report.getEntity();
            assertEquals("Number of aggregates", 1, reportMap.get(ConceptManager.RET_KEY_NUM_AGGREGATES));
            assertEquals("Number of element terms", 4, reportMap.get(ConceptManager.RET_KEY_NUM_ELEMENTS));
            assertEquals("Number of copied properties", 4 * 2, reportMap.get(ConceptManager.RET_KEY_NUM_PROPERTIES));

            assertFalse("Name of the aggregate has been copied",
                    StringUtils.isBlank((String) aggregate.getProperty(PROP_PREF_NAME)));
            assertEquals("Descriptions have been copied", 4,
                    ((String[]) aggregate.getProperty(PROP_DESCRIPTIONS)).length);
            tx.commit();
        }
    }

    @Test
    public void testAddAggregateAsHierarchyNode() {
        // Insert an aggregate and check the relationships.

        ImportConcepts testTerms = getTestConcepts(2);
        ImportConcept aggregate = new ImportConcept(
                Arrays.asList(new ConceptCoordinates("CONCEPT0", "TEST_DATA", true), new ConceptCoordinates("CONCEPT1", "TEST_DATA", true)),
                Collections.singletonList(PROP_PREF_NAME));
        aggregate.coordinates = new ConceptCoordinates("testagg", "TEST_DATA", SRC);
        aggregate.aggregateIncludeInHierarchy = true;
        testTerms.getConceptsAsList().add(aggregate);
        testTerms.getConceptsAsList().get(0).parentCoordinates = Collections.singletonList(new ConceptCoordinates("testagg", "TEST_DATA", SRC));
        testTerms.getConceptsAsList().get(1).parentCoordinates = Collections.singletonList(new ConceptCoordinates("testagg", "TEST_DATA", SRC));

        ConceptManager cm = new ConceptManager(graphDBMS);
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms).getBytes(UTF_8)));

        try (Transaction tx = graphDb.beginTx()) {
            int aggregateCount = countNodesWithLabel(ConceptLabel.AGGREGATE);
            assertEquals(1, aggregateCount);

            Node aggregateNode = NodeUtilities.getSingleNode(tx.findNodes(ConceptLabel.AGGREGATE));
            assertEquals(2, countRelationships(aggregateNode.getRelationships(ConceptEdgeTypes.HAS_ELEMENT)));
            assertEquals(2, countRelationships(aggregateNode.getRelationships(ConceptEdgeTypes.IS_BROADER_THAN)));
            assertEquals(1, countRelationships(aggregateNode.getRelationships(ConceptEdgeTypes.HAS_ROOT_CONCEPT)));

            Node term0 = tx.findNode(CONCEPT, PROP_ID, NodeIDPrefixConstants.TERM + 0);
            Relationship broaderThan = term0.getSingleRelationship(ConceptEdgeTypes.IS_BROADER_THAN, Direction.INCOMING);
            assertEquals(aggregateNode, broaderThan.getStartNode());

            Node term1 = tx.findNode(CONCEPT, PROP_ID, NodeIDPrefixConstants.TERM + 1);
            broaderThan = term1.getSingleRelationship(ConceptEdgeTypes.IS_BROADER_THAN, Direction.INCOMING);
            assertEquals(aggregateNode, broaderThan.getStartNode());
        }
    }

    @Test
    public void testAddAggregateAsHierarchyNode2() {
        // Insert aggregate with an additional label and check that it has been
        // applied.

        ImportConcepts testTerms = getTestConcepts(2);
        ImportConcept aggregate = new ImportConcept(
                Arrays.asList(new ConceptCoordinates("CONCEPT0", "TEST_DATA", true), new ConceptCoordinates("CONCEPT1", "TEST_DATA", true)),
                List.of(PROP_PREF_NAME));
        aggregate.coordinates = new ConceptCoordinates("testagg", "TEST_CONCEPT", SRC);
        aggregate.aggregateIncludeInHierarchy = true;
        aggregate.generalLabels = List.of("MY_COOL_AGGREGATE_LABEL");
        testTerms.getConceptsAsList().add(aggregate);
        testTerms.getConceptsAsList().get(0).parentCoordinates = List.of(new ConceptCoordinates("testagg", "TEST_CONCEPT", SRC));
        testTerms.getConceptsAsList().get(1).parentCoordinates = List.of(new ConceptCoordinates("testagg", "TEST_CONCEPT", SRC));

        ConceptManager cm = new ConceptManager(graphDBMS);
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms).getBytes(UTF_8)));

        try (Transaction ignored = graphDb.beginTx()) {
            int aggregateCount = countNodesWithLabel(Label.label("MY_COOL_AGGREGATE_LABEL"));
            assertEquals(1, aggregateCount);
        }
    }

    @Test
    public void testBuildMappingAggregate() throws Exception {
        ImportConcepts testTerms;
        ImportConcepts testTerms1;
        ImportConcepts testTerms2;
        ImportConcepts testTerms3;
        ImportConcepts testTerms4;
        ConceptManager cm = new ConceptManager(graphDBMS);
        // Create terms in DIFFERENT facets we will then map to each other
        testTerms = getTestConcepts(1, 0);
        testTerms1 = getTestConcepts(1, 1);
        testTerms2 = getTestConcepts(1, 2);
        testTerms3 = getTestConcepts(1, 3);
        testTerms4 = getTestConcepts(1, 4);
        testTerms.getFacet().setName("facet0");
        testTerms1.getFacet().setName("facet1");
        testTerms2.getFacet().setName("facet2");
        testTerms3.getFacet().setName("facet3");
        testTerms4.getFacet().setName("facet4");
        testTerms1.getConceptsAsList().get(0).parentCoordinates = List.of(new ConceptCoordinates("CONCEPT0", "TEST_DATA", SRC));
        testTerms2.getConceptsAsList().get(0).parentCoordinates = List.of(new ConceptCoordinates("CONCEPT1", "TEST_DATA", SRC));
        testTerms3.getConceptsAsList().get(0).parentCoordinates = List.of(new ConceptCoordinates("CONCEPT2", "TEST_DATA", SRC));
        testTerms4.getConceptsAsList().get(0).parentCoordinates = List.of(new ConceptCoordinates("CONCEPT3", "TEST_DATA", SRC));
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms).getBytes(UTF_8)));
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms1).getBytes(UTF_8)));
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms2).getBytes(UTF_8)));
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms3).getBytes(UTF_8)));
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms4).getBytes(UTF_8)));

        List<ImportMapping> mappings = List.of(new ImportMapping("CONCEPT0", "CONCEPT1", "loom"));

        int numInsertedRels = cm.insertMappings(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(mappings).getBytes(UTF_8)));
        assertEquals(1, numInsertedRels);


        try (Transaction tx = graphDb.beginTx()) {
            ConceptAggregateManager.buildAggregatesForMappings(tx, Sets.newHashSet("loom"), CONCEPT,
                    ConceptLabel.AGGREGATE);
            // first check if everything is alright, we expect on aggregate with
            // two elements
            Node aggregate = tx.findNode(ConceptLabel.AGGREGATE, PROP_ID,
                    NodeIDPrefixConstants.AGGREGATE_TERM + 0);
            assertNotNull("Aggregate node with ID " + NodeIDPrefixConstants.AGGREGATE_TERM + "0 could not be found",
                    aggregate);
            Iterable<Relationship> relationships = aggregate.getRelationships();
            int relCount = 0;
            for (Relationship rel : relationships) {
                assertEquals(ConceptEdgeTypes.HAS_ELEMENT.name(), rel.getType().name());
                ++relCount;
            }
            assertEquals(2, relCount);

            // now: get the "children" of the aggregate (should be the elements)
            Map<String, Object> relAndChildMap = (Map<String, Object>) ConceptRetrieval.getChildrenOfConcepts(tx, List.of(NodeIDPrefixConstants.AGGREGATE_TERM + "0"), ConceptLabel.AGGREGATE).get(NodeIDPrefixConstants.AGGREGATE_TERM + 0);
            assertEquals(2, ((Map<?, ?>) relAndChildMap.get(ConceptManager.RET_KEY_RELTYPES)).size());
            assertEquals(2, ((Set<?>) relAndChildMap.get(ConceptManager.RET_KEY_CHILDREN)).size());
        }
    }

    @Test
    public void testHollowParent() {
        // In this test, we will add a node with a single parent. However, we
        // won't add the parent itself, thus creating
        // a "hollow" parent node.
        // In a second step, we will add the former hollow term explicitly
        // together with another parent. We should see
        // how there are then no hollow terms left and the new
        // "grandparent" will become the root.
        ImportFacet importFacet = FacetManagerTest.getImportFacet();
        List<ImportConcept> concepts = new ArrayList<>();
        concepts.add(new ImportConcept("prefname1", Lists.newArrayList("syn1"), "desc1",
                new ConceptCoordinates("srcid1", "TEST_SOURCE", SRC),
                new ConceptCoordinates("parentid1", "TEST_SOURCE", SRC)));
        ImportConcepts importConcepts = new ImportConcepts(concepts, importFacet);
        // Allow hollow parents:
        importConcepts.setImportOptions(new ImportOptions());
        importConcepts.getImportOptions().doNotCreateHollowParents = false;
        ConceptManager cm = new ConceptManager(graphDBMS);
        OutboundJaxrsResponse report = (OutboundJaxrsResponse) cm.insertConcepts(
                new ByteArrayInputStream(ConceptsJsonSerializer.toJson(importConcepts).getBytes(UTF_8)));
        Map<String, ?> reportMap = (Map<String, ?>) report.getEntity();
        assertEquals("Number of inserted terms", 2, reportMap.get(ConceptManager.RET_KEY_NUM_CREATED_CONCEPTS));
        // we expect relations for the root term, broader than and broader than fid0
        assertEquals("Number of inserted relationships", 3, reportMap.get(ConceptManager.RET_KEY_NUM_CREATED_RELS));

        try (Transaction tx = graphDb.beginTx()) {
            Iterable<Node> allNodes = () -> tx.findNodes(CONCEPT);
            int numNodes = 0;
            int numHollow = 0;
            int numRoots = 0;
            for (Node n : allNodes) {
                numNodes++;
                Iterable<Relationship> rels = n.getRelationships();
                int numRootTermRels = 0;
                int numBroaderRels = 0;
                for (Relationship rel : rels) {
                    if (rel.getType().name().equals(ConceptEdgeTypes.HAS_ROOT_CONCEPT.name())) {
                        numRootTermRels++;
                        numRoots++;
                    } else if (rel.getType().name().equals(ConceptEdgeTypes.IS_BROADER_THAN.name())) {
                        numBroaderRels++;
                    }
                }
                if (n.hasLabel(ConceptLabel.HOLLOW)) {
                    numHollow++;
                    assertEquals("Hollow parent is the root term", 1, numRootTermRels);
                    assertEquals("Hollow parent has one child", 1, numBroaderRels);
                } else {
                    assertEquals("Non-hollow term is no facet root", 0, numRootTermRels);
                    assertEquals("Non-hollow term has a parent", 1, numBroaderRels);
                }
            }
            assertEquals("Correct number of nodes", 2, numNodes);
            assertEquals("One node is hollow", 1, numHollow);
            assertEquals("There is one root", 1, numRoots);
            tx.commit();
        }

        // Now we add the term that has been hollow before together with a
        // parent of its own. The new parent should
        // become the facet root, the former hollow term shouldn't be
        // hollow anymore.
        concepts.clear();
        concepts.add(new ImportConcept("prefname2", Lists.newArrayList("syn2"), "desc2",
                new ConceptCoordinates("parentid1", "TEST_SOURCE", SRC),
                new ConceptCoordinates("parentid2", "TEST_SOURCE", SRC)));
        concepts.add(new ImportConcept("prefname3", new ConceptCoordinates("parentid2", "TEST_SOURCE", SRC)));
        // We need to replace the facet definition by the ID of the already
        // created facet because a new facet will be
        // created otherwise.
        importConcepts = new ImportConcepts(concepts, new ImportFacet("fid0"));

        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(importConcepts).getBytes(UTF_8)));

        try (Transaction tx = graphDb.beginTx()) {
            Iterable<Node> allNodes = () -> tx.findNodes(CONCEPT);
            int numNodes = 0;
            int numHollow = 0;
            int numRoots = 0;
            for (Node n : allNodes) {
                numNodes++;
                Iterable<Relationship> rels = n.getRelationships();
                int numRootTermRels = 0;
                int numBroaderRels = 0;
                for (Relationship rel : rels) {
                    if (rel.getType().name().equals(ConceptEdgeTypes.HAS_ROOT_CONCEPT.name())) {
                        numRootTermRels++;
                        numRoots++;
                    } else if (rel.getType().name().equals(ConceptEdgeTypes.IS_BROADER_THAN.name())) {
                        numBroaderRels++;
                    }
                }
                if (n.hasLabel(ConceptLabel.HOLLOW))
                    numHollow++;
                if (n.getProperty(PROP_PREF_NAME).equals("prefname1")) {
                    assertEquals("Number of facet root relations", 0, numRootTermRels);
                    assertEquals("Term has a parent", 1, numBroaderRels);
                } else if (n.getProperty(PROP_PREF_NAME).equals("prefname2")) {
                    assertEquals("Number of facet root relations", 0, numRootTermRels);
                    assertEquals("Number of incident BROADER THAN relationships", 2, numBroaderRels);
                } else {
                    assertEquals("Term is the facet root", 1, numRootTermRels);
                    assertEquals("Number of incident BROADER THAN relationships", 1, numBroaderRels);
                }
            }
            assertEquals("Correct number of nodes", 3, numNodes);
            assertEquals("No node is hollow", 0, numHollow);
            assertEquals("There is one root", 1, numRoots);
            tx.commit();
        }

        // No make sure that when a hollow parent is referenced twice from the
        // same data, there is only one node
        // created.
        concepts.clear();
        concepts.add(new ImportConcept("prefname2", Lists.newArrayList("syn2"), "desc2",
                new ConceptCoordinates("parentid1", "TEST_SOURCE", SRC),
                new ConceptCoordinates("parentid42", "TEST_SOURCE", SRC)));
        concepts.add(new ImportConcept("prefname5", Lists.newArrayList("syn2"), "desc2",
                new ConceptCoordinates("parentid8", "TEST_SOURCE", SRC),
                new ConceptCoordinates("parentid42", "TEST_SOURCE", SRC)));
        importConcepts = new ImportConcepts(concepts, new ImportFacet("fid0"));

        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(importConcepts).getBytes(UTF_8)));

        try (Transaction tx = graphDb.beginTx()) {
            // Would throw an exception if there were multiple terms found.
            Node hollowParent = FullTextIndexUtils.getNode(tx, FULLTEXT_INDEX_CONCEPTS, PROP_SRC_IDS, "parentid42");
            assertNotNull(hollowParent);
            assertTrue(hollowParent.hasLabel(ConceptLabel.HOLLOW));
        }
    }

    @Test
    public void testNonFacetGroupCommand() {
        // We will test whether it works correctly to sort terms into the
        // "non-facet" facet group according to a
        // particular criterium, e.g. "no parent".
        // Terms 0 and 1 will get a parent and thus should be treated normally.
        // Terms 2 and 3 don't get a parent and should become "no-facet" terms.
        ImportConcepts testTerms = getTestConcepts(4);
        ImportOptions options = new ImportOptions();
        testTerms.setImportOptions(options);
        AddToNonFacetGroupCommand cmd = new AddToNonFacetGroupCommand();
        cmd.addParentCriterium(AddToNonFacetGroupCommand.ParentCriterium.NO_PARENT);
        options.noFacetCmd = cmd;
        List<ImportConcept> terms = testTerms.getConceptsAsList();
        // Here, we just give any parent so that the non-facet-group-command
        // does not trigger.
        List<ConceptCoordinates> parentSrcIds = Lists
                .newArrayList(new ConceptCoordinates("nonExistingParent", "TEST_DATA", SRC));
        terms.get(0).parentCoordinates = parentSrcIds;
        terms.get(1).parentCoordinates = parentSrcIds;
        options.cutParents = List.of("nonExistingParent");
        // We activate this so we can test parent cutting appropriately. If it
        // would be off, the child terms of
        // non-existing parents get to be the facet roots no matter what.
        options.doNotCreateHollowParents = true;

        ConceptManager cm = new ConceptManager(graphDBMS);
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms).getBytes(UTF_8)));

        try (Transaction tx = graphDb.beginTx()) {
            Node noFacetGroups = tx.findNode(NodeConstants.Labels.ROOT,
                    PROP_NAME, NAME_NO_FACET_GROUPS);
            assertNotNull("No-Facet group not found.", noFacetGroups);
            Iterator<Relationship> it = noFacetGroups.getRelationships(FacetManager.EdgeTypes.HAS_FACET_GROUP)
                    .iterator();
            assertTrue("There is no no-facet group.", it.hasNext());
            Node noFacetGroup = it.next().getEndNode();
            assertFalse("There is a second no-facet group.", it.hasNext());
            Node noFacet = NodeUtilities.getSingleOtherNode(noFacetGroup, FacetManager.EdgeTypes.HAS_FACET);
            Iterator<Relationship> termRelIt = noFacet.getRelationships(ConceptEdgeTypes.HAS_ROOT_CONCEPT)
                    .iterator();
            assertTrue("There is no no-facet term.", termRelIt.hasNext());
            int i = 0;
            while (termRelIt.hasNext()) {
                Node noFacetTerm = termRelIt.next().getEndNode();
                String termName = (String) noFacetTerm.getProperty(PROP_PREF_NAME);
                // It should be the third of fourth of our test terms.
                assertTrue("Term name was " + termName, termName.equals("prefname2") || termName.equals("prefname3"));
                i++;
            }
            assertEquals("Number of no-facet terms.", 2, i);

            // Assert that the "not existing parent" of term 0 and 1 has been
            // cut, making terms 0 and1 the root terms of
            // the facet, although hollow terms are allowed.
            Node facet = tx.findNode(FacetManager.FacetLabel.FACET, PROP_ID,
                    "fid0");
            for (Relationship relationship : facet.getRelationships(ConceptEdgeTypes.HAS_ROOT_CONCEPT)) {
                Node facetTerm = relationship.getEndNode();
                String termName = (String) facetTerm.getProperty(PROP_PREF_NAME);
                assertTrue("Term name was " + termName, termName.equals("prefname0") || termName.equals("prefname1"));

            }
            tx.commit();
        }

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetPathsFromFacetroots() {
        // In this test we want to see that we get back all paths at once for
        // multiple nodes. In the following, source1
        // and source2 are the facet roots, all other nodes are direct or
        // indirect descendants of them.
        List<ImportConcept> terms = new ArrayList<>();
        ConceptCoordinates coord1 = new ConceptCoordinates("source1", "TEST_SOURCE", SRC);
        ConceptCoordinates coord2 = new ConceptCoordinates("source2", "TEST_SOURCE", SRC);
        ConceptCoordinates coord3 = new ConceptCoordinates("source3", "TEST_SOURCE", SRC);
        ConceptCoordinates coord4 = new ConceptCoordinates("source4", "TEST_SOURCE", SRC);
        ConceptCoordinates coord5 = new ConceptCoordinates("source5", "TEST_SOURCE", SRC);
        ConceptCoordinates coord6 = new ConceptCoordinates("source6", "TEST_SOURCE", SRC);
        terms.add(new ImportConcept("name1", coord1));
        terms.add(new ImportConcept("name2", coord2));
        terms.add(new ImportConcept("name3", coord3, Lists.newArrayList(coord1, coord2)));
        terms.add(new ImportConcept("name4", coord4, coord3));
        terms.add(new ImportConcept("name5", coord5, coord3));
        terms.add(new ImportConcept("name6", coord6, coord5));
        ImportFacet importFacet = FacetManagerTest.getImportFacet();
        ImportConcepts importTermAndFacet = new ImportConcepts(terms, importFacet);
        ConceptManager cm = new ConceptManager(graphDBMS);
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(importTermAndFacet).getBytes(UTF_8)));


        Map<String, List<String[]>> pathsFromFacetroots;
        List<String[]> paths;

        pathsFromFacetroots = (Map<String, List<String[]>>) ((OutboundJaxrsResponse) cm.getPathsFromFacetRoots("source1,source3", PROP_SRC_IDS, null, false, null)).getEntity();
        paths = (List<String[]>) pathsFromFacetroots.get(ConceptManager.RET_KEY_PATHS);
        for (String[] path : paths) {
            // We expect one path with a single node, i.e. source1 and two paths
            // with two nodes respectively, [source1,
            // source3] and [source2, source3]
            // We must compare to node ids. Remember that node IDs are number
            // sequentially in order of appearance in the
            // input data.
            if (path.length == 1)
                assertEquals(NodeIDPrefixConstants.TERM + 0, path[0]);
            else {
                assertEquals(2, path.length);
                // First element is one of the roots
                assertTrue(path[0].equals(NodeIDPrefixConstants.TERM + 0)
                        || path[0].equals(NodeIDPrefixConstants.TERM + 1));
                // Second element is source3
                assertEquals(NodeIDPrefixConstants.TERM + 2, path[1]);
            }
        }
        assertEquals("Wrong number of paths", 3, paths.size());

        pathsFromFacetroots = (Map<String, List<String[]>>) ((OutboundJaxrsResponse) cm.getPathsFromFacetRoots("source4,source6", PROP_SRC_IDS, null, false, null)).getEntity();
        paths = (List<String[]>) pathsFromFacetroots.get(ConceptManager.RET_KEY_PATHS);
        String[] expectedPath1 = new String[]{NodeIDPrefixConstants.TERM + 0, NodeIDPrefixConstants.TERM + 2,
                NodeIDPrefixConstants.TERM + 3};
        String[] expectedPath2 = new String[]{NodeIDPrefixConstants.TERM + 1, NodeIDPrefixConstants.TERM + 2,
                NodeIDPrefixConstants.TERM + 3};
        String[] expectedPath3 = new String[]{NodeIDPrefixConstants.TERM + 0, NodeIDPrefixConstants.TERM + 2,
                NodeIDPrefixConstants.TERM + 4, NodeIDPrefixConstants.TERM + 5};
        String[] expectedPath4 = new String[]{NodeIDPrefixConstants.TERM + 1, NodeIDPrefixConstants.TERM + 2,
                NodeIDPrefixConstants.TERM + 4, NodeIDPrefixConstants.TERM + 5};
        for (String[] path : paths) {
            // We expect paths from source4 and source 6 up to the roots, i.e.
            // [source1, source3, source4]
            // [source2, source3, source4]
            // [source1, source3, source5, source6]
            // [source2, source3, source5, source6]
            // and as term IDs:
            // [tid0, tid2, tid3]
            // [tid1, tid2, tid3]
            // [tid0, tid2, tid4, tid5]
            // [tid1, tid2, tid4, tid5]
            assertTrue(Arrays.equals(expectedPath1, path) || Arrays.equals(expectedPath2, path)
                    || Arrays.equals(expectedPath3, path) || Arrays.equals(expectedPath4, path));
        }
        assertEquals("Wrong number of paths", 4, paths.size());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetPathsFromFacetrootsInSpecificFacet() {
        // In this test we want to see that we get back the right path when only
        // counting in nodes from a specific facet
        // in path finding.
        // The following is a simple chain.
        List<ImportConcept> terms = new ArrayList<>();
        ConceptCoordinates coord1 = new ConceptCoordinates("source1", "TEST_SOURCE", SRC);
        ConceptCoordinates coord2 = new ConceptCoordinates("source2", "TEST_SOURCE", SRC);
        ConceptCoordinates coord3 = new ConceptCoordinates("source3", "TEST_SOURCE", SRC);
        ConceptCoordinates coord4 = new ConceptCoordinates("source4", "TEST_SOURCE", SRC);
        ConceptCoordinates coord5 = new ConceptCoordinates("source5", "TEST_SOURCE", SRC);
        ConceptCoordinates coord6 = new ConceptCoordinates("source6", "TEST_SOURCE", SRC);
        terms.add(new ImportConcept("name1", coord1));
        terms.add(new ImportConcept("name2", coord2, coord1));
        terms.add(new ImportConcept("name3", coord3, coord2));
        terms.add(new ImportConcept("name4", coord4, coord3));
        terms.add(new ImportConcept("name5", coord5, coord4));
        for (ImportConcept term : terms)
            term.coordinates.source = "TEST_SOURCE";
        ImportFacet importFacet = FacetManagerTest.getImportFacet();
        ImportConcepts importTermAndFacet = new ImportConcepts(terms, importFacet);
        ConceptManager cm = new ConceptManager(graphDBMS);
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(importTermAndFacet).getBytes(UTF_8)));


        // Now we insert two terms in another facet that would open another path
        // from source5 to source1 if we would
        // follow the general IS_BROADER_THAN relationship instead of the
        // facet-specific relationship. Note that source5
        // is added a second time which will create the relationship to source
        // 6, thus:
        // source5 --> source6 --> source1 and that similarly source1 was added
        // as a root term for the new facet.
        terms = new ArrayList<>();
        terms.add(new ImportConcept("name1", coord1));
        terms.add(new ImportConcept("name6", coord6, coord1));
        terms.add(new ImportConcept("name5", coord5, coord6));
        for (ImportConcept term : terms)
            term.coordinates.source = "TEST_SOURCE";
        importFacet = FacetManagerTest.getImportFacet();
        importFacet.setName("otherfacet");
        importTermAndFacet = new ImportConcepts(terms, importFacet);
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(importTermAndFacet).getBytes(UTF_8)));


        Map<String, List<String[]>> pathsFromFacetroots;
        List<String[]> paths;

        pathsFromFacetroots = (Map<String, List<String[]>>) ((OutboundJaxrsResponse) cm.getPathsFromFacetRoots("source5", PROP_SRC_IDS, null, false, null)).getEntity();
        paths = pathsFromFacetroots.get(ConceptManager.RET_KEY_PATHS);
        for (String[] path : paths) {
            // We expect two paths, both chains that have been defined by the
            // two inserts above:
            // [tid0, tid1, tid2, tid3, tid4]
            // [tid0, tid5, tid4]
            assertTrue(5 == path.length || 3 == path.length);
        }
        assertEquals("Wrong number of paths", 2, paths.size());

        pathsFromFacetroots = (Map<String, List<String[]>>) ((OutboundJaxrsResponse) cm.getPathsFromFacetRoots("source5", PROP_SRC_IDS, null, false, "fid0")).getEntity();
        paths = pathsFromFacetroots.get(ConceptManager.RET_KEY_PATHS);
        assertEquals("Wrong number of paths", 1, paths.size());
        assertArrayEquals(new String[]{"tid0", "tid1", "tid2", "tid3", "tid4"}, paths.get(0));

        pathsFromFacetroots = (Map<String, List<String[]>>) ((OutboundJaxrsResponse) cm.getPathsFromFacetRoots("source5", PROP_SRC_IDS, null, false, "fid1")).getEntity();
        paths = pathsFromFacetroots.get(ConceptManager.RET_KEY_PATHS);
        assertEquals("Wrong number of paths", 1, paths.size());
        assertArrayEquals(new String[]{"tid0", "tid5", "tid4"}, paths.get(0));

    }

    @Test
    public void testUpdateChildrenInformation() {
        // In this test we check whether the array property of in which facet a
        // term has children is computed correctly.
        // For this, we do two concept imports to create two facets. We will have
        // four terms where the latter three are
        // children of the first.
        // Thus, this first term should have children in both facets.
        ImportConcepts testTerms;
        ConceptManager cm = new ConceptManager(graphDBMS);
        testTerms = getTestConcepts(2);
        testTerms.getConceptsAsList().get(1).parentCoordinates = List.of(new ConceptCoordinates("CONCEPT0", "TEST_DATA", SRC));
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms).getBytes(UTF_8)));

        // Insert terms for the second facet. We have to adjust some values here
        // to actually get four terms and not just
        // two because the duplicate source IDs would be detected.
        testTerms = getTestConcepts(2);
        testTerms.getFacet().setName("secondfacet");
        testTerms.getConceptsAsList().get(0).coordinates.sourceId = "CONCEPT2";
        testTerms.getConceptsAsList().get(0).coordinates.originalId = "CONCEPT2";
        testTerms.getConceptsAsList().get(1).coordinates.sourceId = "CONCEPT3";
        testTerms.getConceptsAsList().get(1).coordinates.originalId = "CONCEPT3";
        testTerms.getConceptsAsList().get(0).parentCoordinates = List.of(new ConceptCoordinates("CONCEPT0", "TEST_DATA", SRC));
        testTerms.getConceptsAsList().get(1).parentCoordinates = List.of(new ConceptCoordinates("CONCEPT0", "TEST_DATA", SRC));
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms).getBytes(UTF_8)));

        cm.updateChildrenInformation();

        try (Transaction tx = graphDb.beginTx()) {
            Node rootNode = tx.findNode(CONCEPT, PROP_ID,
                    NodeIDPrefixConstants.TERM + 0);
            List<String> facetsWithChildren = Lists
                    .newArrayList((String[]) rootNode.getProperty(ConceptConstants.PROP_CHILDREN_IN_FACETS));
            assertTrue(facetsWithChildren.contains(NodeIDPrefixConstants.FACET + 0));
            assertTrue(facetsWithChildren.contains(NodeIDPrefixConstants.FACET + 1));
            assertEquals(2, facetsWithChildren.size());
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetTermChildren() {
        ImportConcepts testTerms;
        ConceptManager cm = new ConceptManager(graphDBMS);
        testTerms = getTestConcepts(5);
        testTerms.getConceptsAsList().get(1).parentCoordinates = List.of(new ConceptCoordinates("CONCEPT0", "TEST_DATA", SRC));
        testTerms.getConceptsAsList().get(2).parentCoordinates = List.of(new ConceptCoordinates("CONCEPT0", "TEST_DATA", SRC));
        testTerms.getConceptsAsList().get(3).parentCoordinates = List.of(new ConceptCoordinates("CONCEPT0", "TEST_DATA", SRC));
        testTerms.getConceptsAsList().get(4).parentCoordinates = List.of(new ConceptCoordinates("CONCEPT3", "TEST_DATA", SRC));
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms).getBytes(UTF_8)));

        try (Transaction tx = graphDb.beginTx()) {
            // Get the children of a single node.
            Map<String, ?> childMap = ConceptRetrieval.getChildrenOfConcepts(tx, List.of("tid0"), CONCEPT);
            // We asked for only one node, thus there should be one result
            assertEquals(1, childMap.size());
            Map<String, Object> term0Children = (Map<String, Object>) childMap.get(NodeIDPrefixConstants.TERM + 0);
            // This result divides into two elements, namely the relationship
            // type mapping and the nodes themselves.
            assertEquals(2, term0Children.size());
            Map<String, Object> reltypeMap = (Map<String, Object>) term0Children.get(ConceptManager.RET_KEY_RELTYPES);
            assertEquals(3, reltypeMap.size());
            assertTrue(reltypeMap.containsKey(NodeIDPrefixConstants.TERM + 1));
            assertTrue(reltypeMap.containsKey(NodeIDPrefixConstants.TERM + 2));
            assertTrue(reltypeMap.containsKey(NodeIDPrefixConstants.TERM + 3));
            Set<Node> children = (Set<Node>) term0Children.get(ConceptManager.RET_KEY_CHILDREN);
            assertEquals(3, children.size());
            Set<String> childIds = new HashSet<>();
            for (Node child : children)
                childIds.add((String) child.getProperty(PROP_ID));
            assertTrue(childIds.contains(NodeIDPrefixConstants.TERM + 1));
            assertTrue(childIds.contains(NodeIDPrefixConstants.TERM + 2));
            assertTrue(childIds.contains(NodeIDPrefixConstants.TERM + 3));

            // We get the children of three nodes, where two of queried nodes
            // are the same. This shouldn't change the
            // result, i.e. there should be two elements in the result map.
            childMap = ConceptRetrieval.getChildrenOfConcepts(tx, List.of("tid0", "tid3", "tid3"), CONCEPT);
            // We asked for three nodes' children, however two were equal thus
            // there should be two elements.
            assertEquals(2, childMap.size());
        }
    }

    @Test
    public void testAddMappings() throws Exception {
        ImportConcepts testTerms;
        ImportConcepts testTerms1;
        ImportConcepts testTerms2;
        ImportConcepts testTerms3;
        ImportConcepts testTerms4;
        ConceptManager cm = new ConceptManager(graphDBMS);
        // Create terms in DIFFERENT facets we will then map to each other
        testTerms = getTestConcepts(1, 0);
        testTerms1 = getTestConcepts(1, 1);
        testTerms2 = getTestConcepts(1, 2);
        testTerms3 = getTestConcepts(1, 3);
        testTerms4 = getTestConcepts(1, 4);
        testTerms.getFacet().setName("facet0");
        testTerms1.getFacet().setName("facet1");
        testTerms2.getFacet().setName("facet2");
        testTerms3.getFacet().setName("facet3");
        testTerms4.getFacet().setName("facet4");
        testTerms1.getConceptsAsList().get(0).parentCoordinates = List.of(new ConceptCoordinates("CONCEPT0", "TEST_DATA", SRC));
        testTerms2.getConceptsAsList().get(0).parentCoordinates = List.of(new ConceptCoordinates("CONCEPT1", "TEST_DATA", SRC));
        testTerms3.getConceptsAsList().get(0).parentCoordinates = List.of(new ConceptCoordinates("CONCEPT2", "TEST_DATA", SRC));
        testTerms4.getConceptsAsList().get(0).parentCoordinates = List.of(new ConceptCoordinates("CONCEPT3", "TEST_DATA", SRC));
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms).getBytes(UTF_8)));
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms1).getBytes(UTF_8)));
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms2).getBytes(UTF_8)));
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms3).getBytes(UTF_8)));
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms4).getBytes(UTF_8)));

        List<ImportMapping> mappings = Lists.newArrayList(new ImportMapping("CONCEPT0", "CONCEPT1", "loom"),
                new ImportMapping("CONCEPT3", "CONCEPT2", "same_uris"));

        int numInsertedRels = cm.insertMappings(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(mappings).getBytes(UTF_8)));
        assertEquals(2, numInsertedRels);

        try (Transaction tx = graphDb.beginTx()) {
            Node term0 = tx.findNode(CONCEPT,
                    PROP_ID, NodeIDPrefixConstants.TERM + 0);
            Iterable<Relationship> relationships = term0.getRelationships(ConceptEdgeTypes.IS_MAPPED_TO);
            int relCounter = 0;
            for (Relationship rel : relationships) {
                Node otherNode = rel.getOtherNode(term0);
                assertEquals(NodeIDPrefixConstants.TERM + 1, otherNode.getProperty(PROP_ID));
                relCounter++;
            }
            assertEquals(1, relCounter);

            Node term1 = tx.findNode(CONCEPT,
                    PROP_ID, NodeIDPrefixConstants.TERM + 1);
            relationships = term1.getRelationships(ConceptEdgeTypes.IS_MAPPED_TO);
            relCounter = 0;
            for (Relationship rel : relationships) {
                Node otherNode = rel.getOtherNode(term1);
                assertEquals(NodeIDPrefixConstants.TERM + 0, otherNode.getProperty(PROP_ID));
                relCounter++;
            }
            assertEquals(1, relCounter);
            tx.commit();
        }

        // Check that there are no duplicates created.
        numInsertedRels = cm.insertMappings(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(mappings).getBytes(UTF_8)));
        assertEquals(0, numInsertedRels);

        // Even if we add a mapping with a new type between two classes that
        // already are mapped, there shouldn't be
        // created a new relationship.
        // We add the exactly same mapping twice, again to assure that
        // duplicates are avoided.
        // We also add a mapping where one term does not exist.
        mappings = Lists.newArrayList(new ImportMapping("CONCEPT0", "CONCEPT1", "new_type"),
                new ImportMapping("CONCEPT0", "CONCEPT1", "new_type"),
                new ImportMapping("CONCEPT_DOES_NOT_EXIST", "CONCEPT1", "new_type"));
        numInsertedRels = cm.insertMappings(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(mappings).getBytes(UTF_8)));
        assertEquals(0, numInsertedRels);
        // But the relationship now should know both mapping types.
        try (Transaction tx = graphDb.beginTx()) {
            Node term0 = tx.findNode(CONCEPT,
                    PROP_ID, NodeIDPrefixConstants.TERM + 0);
            Iterable<Relationship> relationships = term0.getRelationships(ConceptEdgeTypes.IS_MAPPED_TO);
            int relCounter = 0;
            for (Relationship rel : relationships) {
                Node otherNode = rel.getOtherNode(term0);
                assertEquals(NodeIDPrefixConstants.TERM + 1, otherNode.getProperty(PROP_ID));
                String[] mappingTypes = (String[]) rel.getProperty(ConceptRelationConstants.PROP_MAPPING_TYPE);
                List<String> mappingTypesList = Arrays.asList(mappingTypes);
                assertEquals(2, mappingTypesList.size());
                // Check the mapping types
                assertTrue(mappingTypesList.contains("loom"));
                assertTrue(mappingTypesList.contains("new_type"));
                relCounter++;
            }
            assertEquals(1, relCounter);
        }
    }

    @Test
    public void testAddMappingsInSameFacet() throws Exception {
        ImportConcepts testConcepts;
        ConceptManager cm = new ConceptManager(graphDBMS);
        // Create terms in THE SAME facets we will then map to each other
        testConcepts = getTestConcepts(5);
        testConcepts.getConceptsAsList().get(1).parentCoordinates = List.of(new ConceptCoordinates("CONCEPT0", "TEST_DATA", SRC));
        testConcepts.getConceptsAsList().get(2).parentCoordinates = List.of(new ConceptCoordinates("CONCEPT1", "TEST_DATA", SRC));
        testConcepts.getConceptsAsList().get(3).parentCoordinates = List.of(new ConceptCoordinates("CONCEPT2", "TEST_DATA", SRC));
        testConcepts.getConceptsAsList().get(4).parentCoordinates = List.of(new ConceptCoordinates("CONCEPT3", "TEST_DATA", SRC));
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testConcepts).getBytes(UTF_8)));
        ;

        List<ImportMapping> mappings = Lists.newArrayList(new ImportMapping("CONCEPT0", "CONCEPT1", "loom"),
                new ImportMapping("CONCEPT3", "CONCEPT2", "same_uris"));

        // We have a loom mapping between CONCEPT0 and CONCEPT1 which should be
        // filtered out since both terms appear in the same facet.
        int numInsertedRels = cm.insertMappings(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(mappings).getBytes(UTF_8)));
        assertEquals(1, numInsertedRels);
    }

    @Test
    public void testMergeConcepts() {
        System.setProperty(ConceptLookup.SYSPROP_ID_CACHE_ENABLED, "true");
        try {
        // Here, we will insert some terms as normal. Then, we will insert some
        // terms anew, with new property values
        // that should then be merged. We won't define a new facet, we just want
        // to add new information to existing
        // terms.
        ImportConcepts testTerms = getTestConcepts(4);
        // add a fifth term that has the first term as a parent
        testTerms.getConceptsAsList()
                .add(new ImportConcept("someterm", new ConceptCoordinates("somesrcid", "somesource", SRC),
                        new ConceptCoordinates("CONCEPT0", "TEST_DATA", SRC)));
        testTerms.getConceptsAsList().get(testTerms.getConceptsAsList().size() - 1).coordinates.source = "somesource";
        ConceptManager tm = new ConceptManager(graphDBMS);
        // first insert.
        tm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms).getBytes(UTF_8)));

        // now again get two test terms. Those will be identical to the first
        // two test terms above.
        testTerms = getTestConcepts(2);
        // IMPORTANT: this is the key in this test: We do NOT insert a facet
        // which should be not necessary because we
        // only insert terms that are already in the database.
        testTerms.setFacet(null);
        // Give new property values to the first term, leave the second
        // unchanged.
        testTerms.getConceptsAsList().get(0).generalLabels = Lists.newArrayList("newlabel1");
        testTerms.getConceptsAsList().get(0).descriptions = Lists.newArrayList("newdescription1");
        testTerms.getConceptsAsList().get(0).prefName = "newprefname1";
        // re-insert the additional term from above but with a description and a
        // synonym, without a parent; we should
        // not need it since the term is already known.
        ImportConcept term = new ImportConcept("somesrcid", List.of("newsynonym2"), "newdesc2",
                new ConceptCoordinates("somesrcid", "somesource", SRC));
        testTerms.getConceptsAsList().add(term);
        // second insert, duplicate terms should now be merged.
        tm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms).getBytes(UTF_8)));

        // Test the success of the merging.
        try (Transaction tx = graphDb.beginTx()) {
            // See that we really only have a single facet, we havn't added a
            // second one.
            ResourceIterator<Node> facets = tx.findNodes(FacetManager.FacetLabel.FACET);
            int facetCounter = 0;
            for (@SuppressWarnings("unused")
                    Node facet : (Iterable<Node>) () -> facets) {
                facetCounter++;
            }
            assertEquals(1, facetCounter);

            // Now test that the merged-in properties - and labels - are
            // actually there.
            Node mergedTerm = tx.findNode(Label.label("newlabel1"), PROP_ID,
                    "tid0");
            // have we found the term? Then it got its new label.
            assertNotNull(mergedTerm);
            List<String> descriptions = Lists.newArrayList((String[]) mergedTerm.getProperty(PROP_DESCRIPTIONS));
            assertTrue(descriptions.contains("desc of term0"));
            assertTrue(descriptions.contains("newdescription1"));

            Node facet = tx.findNode(FacetManager.FacetLabel.FACET,
                    FacetConstants.PROP_ID, "fid0");
            Iterable<Relationship> rootTermRelations = facet.getRelationships(ConceptEdgeTypes.HAS_ROOT_CONCEPT);
            int rootTermCounter = 0;
            for (@SuppressWarnings("unused")
                    Relationship rel : rootTermRelations) {
                rootTermCounter++;
            }
            // There should be no more facet roots, even though in the second
            // run we re-inserted a term and not give him
            // a parent; this normally causes the term to be a root term, but
            // not if it did exist before.
            assertEquals(4, rootTermCounter);
        }}finally {
            System.setProperty(ConceptLookup.SYSPROP_ID_CACHE_ENABLED, "false");
        }
    }

    @Test
    public void testAddFacetTwice() {
        // Two equal facets. We want to see that they are not imported twice
        ImportConcepts facet1 = getTestConcepts(1);
        ImportConcepts facet2 = getTestConcepts(1);

        ConceptManager tm = new ConceptManager(graphDBMS);
        tm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(facet1).getBytes(UTF_8)));
        tm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(facet2).getBytes(UTF_8)));

        try (Transaction ignored = graphDb.beginTx()) {
            assertEquals(1, countNodesWithLabel(FacetLabel.FACET));
        }
    }

    @Test
    public void testAddWritingVariants() throws Exception {
        {
            ImportConcepts testTerms = getTestConcepts(1);
            ConceptManager tm = new ConceptManager(graphDBMS);
            tm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms).getBytes(UTF_8)));

            Map<String, Map<String, Map<String, Integer>>> variantCountsByTermIdPerDoc = new HashMap<>();
            Map<String, Map<String, Integer>> variantCountsPerDoc = new HashMap<>();
            Map<String, Integer> variantCounts = new HashMap<>();
            variantCounts.put("var1", 1);
            variantCounts.put("var2", 2);
            variantCountsPerDoc.put("doc1", variantCounts);
            variantCounts = new HashMap<>();
            variantCounts.put("var1", 3);
            variantCountsPerDoc.put("doc2", variantCounts);
            variantCountsByTermIdPerDoc.put(NodeIDPrefixConstants.TERM + 0, variantCountsPerDoc);
            tm.addWritingVariants(ConceptsJsonSerializer.toJson(Map.of(KEY_CONCEPT_TERMS, ConceptsJsonSerializer.toJson(variantCountsByTermIdPerDoc))));
        }

        try (Transaction tx = graphDb.beginTx()) {
            Node variantsNode = tx.findNode(MorphoLabel.WRITING_VARIANT, MorphoConstants.PROP_ID, "var1");
            assertNotNull(variantsNode);
            Iterable<Relationship> relationships = variantsNode.getRelationships(Direction.INCOMING,
                    ConceptEdgeTypes.HAS_ELEMENT);
            List<Relationship> relList = new ArrayList<>();
            for (Relationship rel : relationships)
                relList.add(rel);
            // there is only a single term in this test so there should be a
            // single relationship to this writing variant
            assertEquals(1, relList.size());
            Relationship hasWritingVariantRel = relList.get(0);
            String[] docs = (String[]) hasWritingVariantRel.getProperty(MorphoRelationConstants.PROP_DOCS);
            assertTrue(docs.length >= 2);
            assertEquals("doc1", docs[0]);
            assertEquals("doc2", docs[1]);

            int[] counts = (int[]) hasWritingVariantRel.getProperty(MorphoRelationConstants.PROP_COUNTS);
            assertEquals(docs.length, counts.length);
            assertEquals(1, counts[0]);
            assertEquals(3, counts[1]);
        }
    }

    @Test
    public void testAddWritingVariants2() throws Exception {
        {
            ImportConcepts testTerms = getTestConcepts(1);
            ConceptManager tm = new ConceptManager(graphDBMS);
            tm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms).getBytes(UTF_8)));

            Map<String, Map<String, Map<String, Integer>>> variantCountsByTermIdPerDoc = new HashMap<>();
            Map<String, Map<String, Integer>> variantCountsPerDoc = new HashMap<>();
            Map<String, Integer> variantCounts = new HashMap<>();
            variantCounts.put("var1", 1);
            variantCountsPerDoc.put("doc1", variantCounts);
            variantCounts = new HashMap<>();
            variantCounts.put("var1", 2);
            variantCountsPerDoc.put("doc2", variantCounts);
            variantCounts = new HashMap<>();
            variantCounts.put("var1", 3);
            variantCountsPerDoc.put("doc3", variantCounts);
            variantCounts = new HashMap<>();
            variantCounts.put("var1", 4);
            variantCountsPerDoc.put("doc4", variantCounts);
            variantCounts = new HashMap<>();
            variantCounts.put("var1", 5);
            variantCountsPerDoc.put("doc5", variantCounts);
            variantCounts = new HashMap<>();
            variantCounts.put("var1", 6);
            variantCountsPerDoc.put("doc6", variantCounts);
            variantCounts = new HashMap<>();
            variantCounts.put("var1", 7);
            variantCountsPerDoc.put("doc7", variantCounts);
            variantCounts = new HashMap<>();
            variantCounts.put("var1", 8);
            variantCountsPerDoc.put("doc8", variantCounts);
            variantCountsByTermIdPerDoc.put(NodeIDPrefixConstants.TERM + 0, variantCountsPerDoc);
            tm.addWritingVariants(ConceptsJsonSerializer.toJson(Map.of(KEY_CONCEPT_TERMS, ConceptsJsonSerializer.toJson(variantCountsByTermIdPerDoc))));
        }

        try (Transaction tx = graphDb.beginTx()) {
            Node variantsNode = tx.findNode(MorphoLabel.WRITING_VARIANT, MorphoConstants.PROP_ID, "var1");
            assertNotNull(variantsNode);
            Iterable<Relationship> relationships = variantsNode.getRelationships(Direction.INCOMING,
                    ConceptEdgeTypes.HAS_ELEMENT);
            List<Relationship> relList = new ArrayList<>();
            for (Relationship rel : relationships)
                relList.add(rel);
            // there is only a single term in this test so there should be a
            // single relationship to this writing variant
            assertEquals(1, relList.size());
            Relationship hasWritingVariantRel = relList.get(0);
            String[] docs = (String[]) hasWritingVariantRel.getProperty(MorphoRelationConstants.PROP_DOCS);
            assertTrue(docs.length >= 8);
            System.out.println(PropertyUtilities.getNodePropertiesAsString(hasWritingVariantRel));
            assertEquals("doc1", docs[0]);
            assertEquals("doc2", docs[1]);
            assertEquals("doc3", docs[2]);
            assertEquals("doc4", docs[3]);
            assertEquals("doc5", docs[4]);
            assertEquals("doc6", docs[5]);
            assertEquals("doc7", docs[6]);
            assertEquals("doc8", docs[7]);

            int[] counts = (int[]) hasWritingVariantRel.getProperty(MorphoRelationConstants.PROP_COUNTS);
            assertEquals(docs.length, counts.length);
            assertEquals(1, counts[0]);
            assertEquals(2, counts[1]);
            assertEquals(3, counts[2]);
            assertEquals(4, counts[3]);
            assertEquals(5, counts[4]);
            assertEquals(6, counts[5]);
            assertEquals(7, counts[6]);
            assertEquals(8, counts[7]);
        }
    }

    @Test
    public void testUniqueSourceIds() {
        ImportConcepts testTerms = getTestConcepts(2);
        // first, remove original ID and source because otherwise the original
        // ID checks will take over
        testTerms.getConceptsAsList().get(0).coordinates.originalId = null;
        testTerms.getConceptsAsList().get(0).coordinates.originalSource = null;
        testTerms.getConceptsAsList().get(1).coordinates.originalId = null;
        testTerms.getConceptsAsList().get(1).coordinates.originalSource = null;

        // now set terms that should be equal by source ID despite having a
        // different source
        testTerms.getConceptsAsList().get(0).coordinates.uniqueSourceId = true;
        testTerms.getConceptsAsList().get(0).coordinates.sourceId = "id0";
        testTerms.getConceptsAsList().get(0).coordinates.source = "source0";
        testTerms.getConceptsAsList().get(1).coordinates.uniqueSourceId = true;
        testTerms.getConceptsAsList().get(1).coordinates.sourceId = "id0";
        testTerms.getConceptsAsList().get(1).coordinates.source = "source1";

        ConceptManager tm = new ConceptManager(graphDBMS);
        tm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms).getBytes(UTF_8)));
    }

    @Test
    public void testGetFacetRootsWithLimit() {

        // the exact same test as testGetAllFacetRoots() but with a limit on
        // maximum roots
        ConceptManager tm = new ConceptManager(graphDBMS);
        ImportConcepts testTerms = getTestConcepts(3);
        tm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms).getBytes(UTF_8)));
        // Insert two times so we have two facets
        testTerms = getTestConcepts(3);
        tm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms).getBytes(UTF_8)));
        try (Transaction tx = graphDb.beginTx()) {
            Map<String, List<Node>> facetRoots = FacetRootsRetrieval
                    .getFacetRoots(tx, Set.of(NodeIDPrefixConstants.FACET + 0, NodeIDPrefixConstants.FACET + 1), null, 2);
            assertThat(facetRoots).hasSize(0);
        }
    }

    @Test
    public void testGetSpecificFacetRoots() {
        ConceptManager tm = new ConceptManager(graphDBMS);
        ImportConcepts testTerms = getTestConcepts(3);
        // Insert three times so we have three facets
        tm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms).getBytes(UTF_8)));
        testTerms = getTestConcepts(3);
        testTerms.getFacet().setName("secondfacet");
        tm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms).getBytes(UTF_8)));
        testTerms = getTestConcepts(3);
        testTerms.getFacet().setName("thirdfacet");
        tm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms).getBytes(UTF_8)));

        Map<String, Set<String>> requestedRoots = new HashMap<>();
        requestedRoots.put(NodeIDPrefixConstants.FACET + 0, Set.of(NodeIDPrefixConstants.TERM + 0));
        requestedRoots.put(NodeIDPrefixConstants.FACET + 1,
                Set.of(NodeIDPrefixConstants.TERM + 1, NodeIDPrefixConstants.TERM + 2));
        // for the third facet, we want all roots returned

        try (Transaction tx = graphDb.beginTx()) {
            Map<String, List<Node>> facetRoots = FacetRootsRetrieval.getFacetRoots(tx, Set.of(NodeIDPrefixConstants.FACET + 0,
                    NodeIDPrefixConstants.FACET + 1, NodeIDPrefixConstants.FACET + 2), requestedRoots, 0);

            // Roots of two facets
            assertEquals(3, facetRoots.size());
            List<Node> roots = facetRoots.get(NodeIDPrefixConstants.FACET + 0);
            List<String> rootIds = new ArrayList<>();
            for (Node root : roots)
                rootIds.add((String) root.getProperty(PROP_ID));

            assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 0));
            assertFalse(rootIds.contains(NodeIDPrefixConstants.TERM + 1));
            assertFalse(rootIds.contains(NodeIDPrefixConstants.TERM + 2));

            roots = facetRoots.get(NodeIDPrefixConstants.FACET + 1);
            rootIds = new ArrayList<>();
            for (Node root : roots)
                rootIds.add((String) root.getProperty(PROP_ID));
            assertFalse(rootIds.contains(NodeIDPrefixConstants.TERM + 0));
            assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 1));
            assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 2));

            roots = facetRoots.get(NodeIDPrefixConstants.FACET + 2);
            rootIds = new ArrayList<>();
            for (Node root : roots)
                rootIds.add((String) root.getProperty(PROP_ID));
            assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 0));
            assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 1));
            assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 2));
        }
    }

    @Test
    public void testGetAllFacetRoots() {
        ConceptManager tm = new ConceptManager(graphDBMS);
        ImportConcepts importConcepts = getTestConcepts(3);
        tm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(importConcepts).getBytes(UTF_8)));
        // Insert two times so we have two facets
        importConcepts = getTestConcepts(3);
        importConcepts.getFacet().setName("secondfacet");
        tm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(importConcepts).getBytes(UTF_8)));
        try (Transaction tx = graphDb.beginTx()) {
            Map<String, List<Node>> facetRoots = FacetRootsRetrieval
                    .getFacetRoots(tx, Set.of(NodeIDPrefixConstants.FACET + 0, NodeIDPrefixConstants.FACET + 1), null, 0);

            // Roots of two facets
            assertEquals(2, facetRoots.size());
            List<Node> roots = facetRoots.get(NodeIDPrefixConstants.FACET + 0);
            List<String> rootIds = new ArrayList<>();
            for (Node root : roots)
                rootIds.add((String) root.getProperty(PROP_ID));

            assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 0));
            assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 1));
            assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 2));

            roots = facetRoots.get(NodeIDPrefixConstants.FACET + 1);
            rootIds = new ArrayList<>();
            for (Node root : roots)
                rootIds.add((String) root.getProperty(PROP_ID));
            assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 0));
            assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 1));
            assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 2));
        }
    }

    @Test
    public void testInsertSimpleSemanticRelation() {
        ConceptManager cm = new ConceptManager(graphDBMS);

    }
}
