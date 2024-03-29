package de.julielab.neo4j.plugins.auxiliaries.semedico;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import de.julielab.neo4j.plugins.ConceptManagerTest;
import de.julielab.neo4j.plugins.FacetManagerTest;
import de.julielab.neo4j.plugins.Indexes;
import de.julielab.neo4j.plugins.auxiliaries.PropertyUtilities;
import de.julielab.neo4j.plugins.concepts.*;
import de.julielab.neo4j.plugins.concepts.ConceptAggregateManager.CopyAggregatePropertiesStatistics;
import de.julielab.neo4j.plugins.datarepresentation.*;
import de.julielab.neo4j.plugins.datarepresentation.constants.AggregateConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.NodeIDPrefixConstants;
import de.julielab.neo4j.plugins.datarepresentation.util.ConceptsJsonSerializer;
import de.julielab.neo4j.plugins.test.TestUtilities;
import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.logging.log4j.LogConfig;

import java.io.ByteArrayInputStream;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.logging.FormattedLogFormat.PLAIN;

public class ConceptAggregateManagerTest {
    private static GraphDatabaseService graphDb;
    /**
     * Takes a source ID <code>srcId</code> and creates a coordinate for
     * <code>(srcId, TEST_SOURCE)</code> with original ID coordinates.
     */
    private static Function<String, ConceptCoordinates> coords;
    /**
     * Takes a preferred name and a source ID. Creates an ImportConcept with the
     * respective name and source ID using {@link #coords}.
     */
    private static BiFunction<String, String, ImportConcept> cs;
    private static DatabaseManagementService graphDBMS;
    private static Log log;

    @BeforeClass
    public static void initialize() {
        graphDBMS = TestUtilities.getGraphDBMS();
        graphDb = graphDBMS.database(DEFAULT_DATABASE_NAME);
        coords = srcId -> new ConceptCoordinates(srcId, "TEST_DATA", CoordinateType.SRC);
        cs = (name, srcId) -> new ImportConcept(name, coords.apply(srcId));
        Log4jLogProvider log4jLogProvider = new Log4jLogProvider(LogConfig.createBuilder(System.out, Level.INFO)
                .withFormat(PLAIN)
                .withCategory(false)
                .build());
        log = log4jLogProvider.getLog(ConceptManagerTest.class);
    }

    @AfterClass
    public static void shutdown() {
        graphDBMS.shutdown();
    }

    @Before
    public void cleanForTest() {
        TestUtilities.deleteEverythingInDB(graphDb);
        new Indexes(graphDBMS).createIndexes(DEFAULT_DATABASE_NAME);
    }

    @Test
    public void testCopyAggregateProperties() {
        try (Transaction tx = graphDb.beginTx()) {
            // Create the aggregate node and its element nodes
            Node aggregate = tx.createNode();
            Node element1 = tx.createNode();
            Node element2 = tx.createNode();
            Node element3 = tx.createNode();
            Node element4 = tx.createNode();

            // Set some properties to the element nodes that should then be
            // copied to the aggregate.
            element1.setProperty("name", "apfelsine");
            element1.setProperty("geschmack", new String[]{"suess", "saftig"});
            element1.setProperty("synonyms", new String[]{"oraNgE"});
            element2.setProperty("name", "apfelsine");
            element2.setProperty("geschmack", new String[]{"fruchtig", "spritzig"});
            element2.setProperty("synonyms", new String[]{"orange"});
            element3.setProperty("name", "orange");
            element4.setProperty("name", "orangendings");
            element4.setProperty("synonyms", new String[]{"apfelsine"});

            // Connect the element nodes to the aggregate.
            aggregate.createRelationshipTo(element1, ConceptEdgeTypes.HAS_ELEMENT);
            aggregate.createRelationshipTo(element2, ConceptEdgeTypes.HAS_ELEMENT);
            aggregate.createRelationshipTo(element3, ConceptEdgeTypes.HAS_ELEMENT);
            aggregate.createRelationshipTo(element4, ConceptEdgeTypes.HAS_ELEMENT);

            // Copy the element properties to the aggregate.
            CopyAggregatePropertiesStatistics copyStats = new CopyAggregatePropertiesStatistics();
            ConceptAggregateManager.copyAggregateProperties(aggregate, false, new String[]{"name", "geschmack", "synonyms"},
                    copyStats);

            // Check whether everything is as expected.
            // There should be as name the majority name "apfelsine".
            String name = (String) PropertyUtilities.getNonNullNodeProperty(aggregate, "name");
            assertEquals("apfelsine", name);
            // Since there were two other names - "orange" and "organgendings" -
            // we expect those names to be stored in a
            // special property of the aggregate.
            String[] minorityNames = (String[]) PropertyUtilities.getNonNullNodeProperty(aggregate,
                    "name" + AggregateConstants.SUFFIX_DIVERGENT_ELEMENT_ROPERTY);
            assertNotNull("The minority property \"" + "name" + AggregateConstants.SUFFIX_DIVERGENT_ELEMENT_ROPERTY
                    + "\" is null", minorityNames);
            List<String> minorityNamesList = asList(minorityNames);
            assertTrue(minorityNamesList.contains("orange"));
            assertTrue(minorityNamesList.contains("orangendings"));

            String[] geschmaecker = (String[]) PropertyUtilities.getNonNullNodeProperty(aggregate, "geschmack");
            List<String> geschmaeckerList = asList(geschmaecker);
            assertTrue(geschmaeckerList.contains("fruchtig"));
            assertTrue(geschmaeckerList.contains("saftig"));
            assertTrue(geschmaeckerList.contains("spritzig"));
            assertTrue(geschmaeckerList.contains("suess"));
            assertEquals("Alle geschmaecker: " + StringUtils.join(geschmaeckerList, ", "), 4, geschmaeckerList.size());

            String[] synonyms = (String[]) PropertyUtilities.getNonNullNodeProperty(aggregate, "synonyms");
            assertNotNull("Synonyms sind null", synonyms);
            assertEquals("Synonyms are: " + StringUtils.join(synonyms, ", "), 2, synonyms.length);
            assertEquals("apfelsine", synonyms[0]);
            // note: we make actually no effort to decide for a specific
            // writing variant (here: orange vs. oraNgE) so this test might fail
            // if the original array order changes for any reason
            // this test sometimes fails because on different machines, the
            // relationships may be ordered differently and another aggregate
            // element comes first
            // assertEquals("orange", synonyms[1]);

            tx.commit();
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testBuildAggregatesForMappingsSimpleCase() throws Exception {
        // In this test we will define two facets with a path of three terms,
        // respectively, where two term of those
        // parts are mapped to each other. Thus, we will expect that after
        // aggregation, the two paths will be connected
        // through
        // one aggregation node.
        ImportConcept t11 = cs.apply("t11", "t11");
        ImportConcept t12 = cs.apply("t12", "t12");
        t12.parentCoordinates = List.of(coords.apply("t11"));
        ImportConcept t13 = cs.apply("t13", "t13");
        t13.parentCoordinates = Collections.singletonList(coords.apply("t12"));
        ArrayList<ImportConcept> concepts1 = Lists.newArrayList(t11, t12, t13);
        ImportFacet importFacet1 = FacetManagerTest.getImportFacet();

        ImportConcept t21 = cs.apply("t21", "t21");
        ImportConcept t22 = cs.apply("t22", "t22");
        t22.parentCoordinates = Collections.singletonList(coords.apply("t21"));
        ImportConcept t23 = cs.apply("t23", "t3");
        t23.parentCoordinates = Collections.singletonList(coords.apply("t22"));
        ArrayList<ImportConcept> concepts2 = Lists.newArrayList(t21, t22, t23);
        ImportFacet importFacet2 = FacetManagerTest.getImportFacet();

        List<ImportMapping> mapping = List.of(new ImportMapping("t12", "t21", "EQUAL"));

        ConceptManager cm = new ConceptManager(graphDBMS, log);

        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(new ImportConcepts(concepts1, importFacet1)).getBytes(UTF_8)));
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(new ImportConcepts(concepts2, importFacet2)).getBytes(UTF_8)));
        cm.insertMappings(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(mapping).getBytes(UTF_8)));
        Label aggregatedTermsLabel = Label.label("EQUAL_AGG");

        try (Transaction tx = graphDb.beginTx()) {
            ConceptAggregateManager.buildAggregatesForMappings(tx, Sets.newHashSet("EQUAL"), null, aggregatedTermsLabel, log);
            ResourceIterable<Node> mappingAggregates = () -> tx.findNodes(aggregatedTermsLabel);
            int count = 0;
            for (Node aggregate : mappingAggregates) {
                if (!aggregate.hasLabel(ConceptLabel.AGGREGATE))
                    continue;
                count++;

                // Check that all element terms are there
                Set<String> elementIds = new HashSet<>();
                Iterable<Relationship> elementRels = aggregate.getRelationships(ConceptEdgeTypes.HAS_ELEMENT);
                for (Relationship rel : elementRels) {
                    Node element = rel.getOtherNode(aggregate);
                    String[] srcIds = NodeUtilities.getSourceIdArray(element);
                    assertNotNull(srcIds);
                    elementIds.add(srcIds[0]);
                }
                assertTrue(elementIds.contains(t12.coordinates.sourceId));
                assertTrue(elementIds.contains(t21.coordinates.sourceId));
            }
            assertEquals(1, count);

            tx.commit();
        }

        // Test that we can get the "children" of the aggregate, i.e. its
        // elements via the respective FacetManager
        // method
        try (Transaction tx = graphDb.beginTx()) {
            Map<String, Object> map = (Map<String, Object>) ConceptRetrieval.getChildrenOfConcepts(tx, List.of(NodeIDPrefixConstants.AGGREGATE_TERM + 0), aggregatedTermsLabel).get(NodeIDPrefixConstants.AGGREGATE_TERM + 0);
            Map<String, List<String>> reltypes = (Map<String, List<String>>) map.get(ConceptManager.RET_KEY_RELTYPES);
            List<String> list1 = reltypes.get(NodeIDPrefixConstants.TERM + 1);
            assertEquals("HAS_ELEMENT", list1.get(0));
            List<String> list2 = reltypes.get(NodeIDPrefixConstants.TERM + 3);
            assertEquals("HAS_ELEMENT", list2.get(0));
            Set<Node> children = (Set<Node>) map.get(ConceptManager.RET_KEY_CHILDREN);
            Set<String> childrenIds = new HashSet<>();
            for (Node term : children)
                childrenIds.add(Objects.requireNonNull(NodeUtilities.getSourceIdArray(term))[0]);
            assertTrue(childrenIds.contains(t12.coordinates.sourceId));
            assertTrue(childrenIds.contains(t21.coordinates.sourceId));
        }

        // Now test whether the removal of aggregates is working as well
        try (Transaction tx = graphDb.beginTx()) {
            ConceptAggregateManager.deleteAggregates(tx, aggregatedTermsLabel, log);

            ResourceIterable<Node> mappingAggregates = () -> tx.findNodes(aggregatedTermsLabel);
            int count = 0;
            for (@SuppressWarnings("unused")
                    Node aggregate : mappingAggregates) {
                count++;
            }
            assertEquals(0, count);

            tx.commit();
        }

    }

    @Test
    public void testBuildAggregatesForMappingsTransitiveCase() throws Exception {
        // In this test we will define a single path of three terms where the
        // first two terms are mapped by one mapping
        // type and the last two (thus with the same term "in the middle") by
        // another type (we use two types because if
        // it would be the same, the three terms would form a "mapping-clique"
        // most probably in reality).
        // The algorithm should recognize the transitive mapping by first
        // creating a "small" aggregate only spanning two
        // terms and later including the third term.
        ImportConcept t1 = cs.apply("t1", "t1");
        ImportConcept t2 = cs.apply("t2", "t2");
        t2.parentCoordinates = List.of(coords.apply("t1"));
        ImportConcept t3 = cs.apply("t3", "t3");
        t3.parentCoordinates = List.of(coords.apply("t2"));
        ArrayList<ImportConcept> terms1 = Lists.newArrayList(t1, t2, t3);
        ImportFacet importFacet1 = FacetManagerTest.getImportFacet();

        List<ImportMapping> mapping = Lists.newArrayList(new ImportMapping("t1", "t2", "EQUAL"),
                new ImportMapping("t2", "t3", "OTHER_EQUAL"));

        ConceptManager cm = new ConceptManager(graphDBMS, log);
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(new ImportConcepts(terms1, importFacet1)).getBytes(UTF_8)));
        cm.insertMappings(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(mapping).getBytes(UTF_8)));
        Label aggLabel = Label.label("EQUAL_AGG");

        try (Transaction tx = graphDb.beginTx()) {
            ConceptAggregateManager.buildAggregatesForMappings(tx, Sets.newHashSet("EQUAL", "OTHER_EQUAL"), null,
                    aggLabel, log);
            ResourceIterable<Node> mappingAggregates = () -> tx.findNodes(aggLabel);
            int count = 0;
            for (Node aggregate : mappingAggregates) {
                count++;

                // Check that all element terms are there
                Set<String> elementIds = new HashSet<>();
                Iterable<Relationship> elementRels = aggregate.getRelationships(ConceptEdgeTypes.HAS_ELEMENT);
                for (Relationship rel : elementRels) {
                    Node element = rel.getOtherNode(aggregate);
                    String[] srcIds = NodeUtilities.getSourceIdArray(element);
                    assertNotNull(srcIds);
                    assertNotNull(srcIds[0]);
                    elementIds.add(srcIds[0]);
                }
                assertTrue(aggregate.hasLabel(aggLabel));

                assertTrue(elementIds.contains(t1.coordinates.sourceId));
                assertTrue(elementIds.contains(t2.coordinates.sourceId));
                assertTrue(elementIds.contains(t3.coordinates.sourceId));
            }
            assertEquals(1, count);
        }
    }

    @Test
    public void testBuildAggregatesForMappingsTransitiveCaseTwoSets() throws Exception {
        // Here, we build three distinct sets of nodes:
        // The first three nodes are mapped to each other transitively.
        // Terms 4 and 5 are mapped to each other.
        // Term 6 is not mapped at all and thus forms "its own aggregate".
        ImportConcept t1 = cs.apply("t1", "t1");
        ImportConcept t2 = cs.apply("t2", "t2");
        t2.parentCoordinates = Collections.singletonList(coords.apply("t1"));
        ImportConcept t3 = cs.apply("t3", "t3");
        t3.parentCoordinates = Collections.singletonList(coords.apply("t2"));
        ImportConcept t4 = cs.apply("t4", "t4");
        t4.parentCoordinates = Collections.singletonList(coords.apply("t3"));
        ImportConcept t5 = cs.apply("t5", "t5");
        t5.parentCoordinates = Collections.singletonList(coords.apply("t4"));
        ImportConcept t6 = cs.apply("t6", "t6");
        t6.parentCoordinates = Collections.singletonList(coords.apply("t5"));
        ArrayList<ImportConcept> concepts = Lists.newArrayList(t1, t2, t3, t4, t5, t6);
        ImportFacet importFacet1 = FacetManagerTest.getImportFacet();

        // Define the mappings. Term 6 is not mapped.
        List<ImportMapping> mapping = Lists.newArrayList(new ImportMapping("t1", "t2", "EQUAL"),
                new ImportMapping("t2", "t3", "OTHER_EQUAL"), new ImportMapping("t4", "t5", "EQUAL"));

        ConceptManager cm = new ConceptManager(graphDBMS, log);
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(new ImportConcepts(concepts, importFacet1)).getBytes(UTF_8)));
        cm.insertMappings(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(mapping).getBytes(UTF_8)));
        // The label by which we will identify all nodes representing an
        // aggregated unit, i.e. an actual aggregate node
        // or a term without any mappings that is its own aggregate.
        Label aggLabel = Label.label("EQUAL_AGG");

        try (Transaction tx = graphDb.beginTx()) {
            ConceptAggregateManager.buildAggregatesForMappings(tx, Sets.newHashSet("EQUAL", "OTHER_EQUAL"), null,
                    aggLabel, log);
            ResourceIterable<Node> mappingAggregates = () -> tx.findNodes(aggLabel);
            // Count of the aggregation terms, i.e. the representation terms
            // that have formerly existing terms as their
            // elements.
            int aggCount = 0;
            for (Node aggregate : mappingAggregates) {
                if (!aggregate.hasLabel(ConceptLabel.AGGREGATE))
                    continue;
                aggCount++;

                // Check that all element terms are there
                Set<String> elementIds = new HashSet<>();
                Iterable<Relationship> elementRels = aggregate.getRelationships(ConceptEdgeTypes.HAS_ELEMENT);
                for (Relationship rel : elementRels) {
                    Node element = rel.getOtherNode(aggregate);
                    String[] srcIds = NodeUtilities.getSourceIdArray(element);
                    assertNotNull(srcIds);
                    assertNotNull(srcIds[0]);
                    elementIds.add(srcIds[0]);
                }
                if (elementIds.size() == 3) {
                    assertTrue(elementIds.contains(t1.coordinates.sourceId));
                    assertTrue(elementIds.contains(t2.coordinates.sourceId));
                    assertTrue(elementIds.contains(t3.coordinates.sourceId));
                }
                if (elementIds.size() == 2) {
                    assertTrue(elementIds.contains(t4.coordinates.sourceId));
                    assertTrue(elementIds.contains(t5.coordinates.sourceId));
                }
            }
            assertEquals(2, aggCount);

            ResourceIterable<Node> aggregatedTerms = () -> tx.findNodes(aggLabel);
            // Count of all terms that represent the result of the aggregation,
            // i.e. aggregate terms as well as original
            // terms that are no element of an aggregate term and as such "are
            // their own aggregate".
            int aggregatedTermsCount = 0;
            for (Node aggregatedTerm : aggregatedTerms) {
                aggregatedTermsCount++;

                // Check that all element terms are there
                Iterable<Relationship> elementRels = aggregatedTerm
                        .getRelationships(ConceptEdgeTypes.HAS_ELEMENT);
                Iterator<Relationship> elementIt = elementRels.iterator();
                if (!elementIt.hasNext()) {
                    String[] srcIds = NodeUtilities.getSourceIdArray(aggregatedTerm);
                    assertNotNull(srcIds);
                    assertNotNull(srcIds[0]);
                    assertEquals(t6.coordinates.sourceId, srcIds[0]);
                }
            }
            assertEquals(3, aggregatedTermsCount);
        }
    }

    @Test
    public void testGetNonAggregateElements() {
        // First, we create a tree of element nodes with three inner nodes (=aggregates) and three leaves (=elements).
        try (Transaction tx = graphDb.beginTx()) {
            Node topAggregate = tx.createNode(ConceptLabel.AGGREGATE);
            Node childAggregate1 = tx.createNode(ConceptLabel.AGGREGATE);
            Node childAggregate2 = tx.createNode(ConceptLabel.AGGREGATE);
            Node concept11 = tx.createNode(ConceptLabel.CONCEPT);
            Node concept12 = tx.createNode(ConceptLabel.CONCEPT);
            Node concept21 = tx.createNode(ConceptLabel.CONCEPT);

            topAggregate.createRelationshipTo(childAggregate1, ConceptEdgeTypes.HAS_ELEMENT);
            topAggregate.createRelationshipTo(childAggregate2, ConceptEdgeTypes.HAS_ELEMENT);

            childAggregate1.createRelationshipTo(concept11, ConceptEdgeTypes.HAS_ELEMENT);
            childAggregate1.createRelationshipTo(concept12, ConceptEdgeTypes.HAS_ELEMENT);

            childAggregate2.createRelationshipTo(concept21, ConceptEdgeTypes.HAS_ELEMENT);

            // From the top aggregate we should retrieve all three leaves
            List<Node> nonAggregateElements = ConceptAggregateManager.getNonAggregateElements(topAggregate);
            assertTrue(nonAggregateElements.contains(concept11));
            assertTrue(nonAggregateElements.contains(concept12));
            assertTrue(nonAggregateElements.contains(concept21));

            // Child aggregate 1 has two children
            nonAggregateElements = ConceptAggregateManager.getNonAggregateElements(childAggregate1);
            assertTrue(nonAggregateElements.contains(concept11));
            assertTrue(nonAggregateElements.contains(concept12));

            // and child aggregate 2 has one child
            nonAggregateElements = ConceptAggregateManager.getNonAggregateElements(childAggregate2);
            assertTrue(nonAggregateElements.contains(concept21));
        }
    }

    @Test
    public void someTest() {
        final ConceptManager cm = new ConceptManager(graphDBMS, log);
        final ImportConcept concept = new ImportConcept(new ConceptCoordinates("1", "muh", "1", "muh"));
        final ImportFacet importFacet = FacetManagerTest.getImportFacet();
        final ImportConcepts importConcepts = new ImportConcepts(List.of(concept), importFacet);
        try (final Transaction tx = graphDb.beginTx()) {
            cm.insertConcepts(importConcepts);
            final Node n = tx.findNode(ConceptLabel.CONCEPT, "id", "tid0");
            System.out.println(NodeUtilities.getNodePropertiesAsString(n));
        }
    }
}
