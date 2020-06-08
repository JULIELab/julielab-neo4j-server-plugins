package de.julielab.neo4j.plugins.concepts;

import de.julielab.neo4j.plugins.Indexes;
import de.julielab.neo4j.plugins.datarepresentation.*;
import de.julielab.neo4j.plugins.datarepresentation.constants.ImportIERelations;
import de.julielab.neo4j.plugins.datarepresentation.util.ConceptsJsonSerializer;
import de.julielab.neo4j.plugins.test.TestUtilities;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.*;
import org.neo4j.logging.slf4j.Slf4jLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static de.julielab.neo4j.plugins.ConceptManagerTest.getTestConcepts;
import static de.julielab.neo4j.plugins.concepts.ConceptLabel.CONCEPT;
import static de.julielab.neo4j.plugins.constants.semedico.SemanticRelationConstants.*;
import static de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants.PROP_SRC_IDS;
import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_ID;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class IERelationInsertionTest {
    private final static Logger log = LoggerFactory.getLogger(IERelationInsertionTest.class);
    private static GraphDatabaseService graphDb;
    private static DatabaseManagementService graphDBMS;

    @BeforeClass
    public static void initialize() {
        graphDBMS = TestUtilities.getGraphDBMS();
        graphDb = graphDBMS.database(DEFAULT_DATABASE_NAME);
        System.setProperty(ConceptLookup.SYSPROP_ID_CACHE_ENABLED, "false");
    }
    @Before
    public void cleanForTest() {
        TestUtilities.deleteEverythingInDB(graphDb);
        new Indexes(graphDBMS).createIndexes((String) null);
    }
    @AfterClass
    public static void shutdown() {
        graphDBMS.shutdown();
    }
    @Test
    public void testInsertSimpleSemanticRelation() {
        ImportConcepts importConcepts = getTestConcepts(2);

        ImportIERelations relations = new ImportIERelations(PROP_ID);
        RelationshipType regulationType = RelationshipType.withName("regulation");
        relations.addRelationDocument(ImportIERelationDocument.of(
                "docId", ImportIETypedRelations.of(
                        regulationType.name(), ImportIERelation.of(
                                3, ImportIERelationArgument.of("tid0"), ImportIERelationArgument.of("tid1")))));

        ConceptManager cm = new ConceptManager(graphDBMS);
        cm.insertConcepts(importConcepts);
        cm.insertIERelations(relations);

        try (Transaction tx = graphDb.beginTx()) {
            Node n = tx.findNode(CONCEPT, PROP_ID, "tid0");
            assertThat(n).isNotNull();
            assertThat(n.getDegree(regulationType)).isEqualTo(1);
            Relationship rs = n.getSingleRelationship(regulationType, Direction.OUTGOING);
            assertThat(rs).isNotNull();
            assertThat(rs.hasProperty(PROP_TOTAL_COUNT));
            assertThat(rs.getProperty(PROP_TOTAL_COUNT)).isEqualTo(3);
        }
    }

    @Test
    public void testInsertBySourceIdLocalSources() {
        ImportConcepts importConcepts = getTestConcepts(2);

        ImportIERelations relations = new ImportIERelations(PROP_ID);
        RelationshipType regulationType = RelationshipType.withName("regulation");
        relations.addRelationDocument(ImportIERelationDocument.of(
                "docId", ImportIETypedRelations.of(
                        regulationType.name(), ImportIERelation.of(
                                3, ImportIERelationArgument.of("sourceIds:CONCEPT0", "TEST_DATA"), ImportIERelationArgument.of("sourceIds:CONCEPT1", "TEST_DATA")))));

        ConceptManager cm = new ConceptManager(graphDBMS);
        cm.insertConcepts(importConcepts);
        cm.insertIERelations(relations);

        try (Transaction tx = graphDb.beginTx()) {
            Node n = tx.findNode(CONCEPT, PROP_ID, "tid0");
            assertThat(n).isNotNull();
            assertThat(n.getDegree(regulationType)).isEqualTo(1);
            Relationship rs = n.getSingleRelationship(regulationType, Direction.OUTGOING);
            assertThat(rs).isNotNull();
            assertThat(rs.hasProperty(PROP_TOTAL_COUNT));
            assertThat(rs.getProperty(PROP_TOTAL_COUNT)).isEqualTo(3);
        }
    }

    @Test
    public void testInsertBySourceIdGlobalSources() {
        ImportConcepts importConcepts = getTestConcepts(2);

        ImportIERelations relations = new ImportIERelations(PROP_SRC_IDS, "TEST_DATA");
        RelationshipType regulationType = RelationshipType.withName("regulation");
        relations.addRelationDocument(ImportIERelationDocument.of(
                "docId", ImportIETypedRelations.of(
                        regulationType.name(), ImportIERelation.of(
                                3, ImportIERelationArgument.of("CONCEPT0"), ImportIERelationArgument.of("CONCEPT1")))));

        ConceptManager cm = new ConceptManager(graphDBMS);
        cm.insertConcepts(importConcepts);
        cm.insertIERelations(relations);

        try (Transaction tx = graphDb.beginTx()) {
            Node n = tx.findNode(CONCEPT, PROP_ID, "tid0");
            assertThat(n).isNotNull();
            assertThat(n.getDegree(regulationType)).isEqualTo(1);
            Relationship rs = n.getSingleRelationship(regulationType, Direction.OUTGOING);
            assertThat(rs).isNotNull();
            assertThat(rs.hasProperty(PROP_TOTAL_COUNT));
            assertThat(rs.getProperty(PROP_TOTAL_COUNT)).isEqualTo(3);
        }
    }

    @Test
    public void testInsertBySourceIdMixedSources() {
        ImportConcepts importConcepts = getTestConcepts(2);

        ImportIERelations relations = new ImportIERelations(PROP_SRC_IDS, "TEST_DATA");
        RelationshipType regulationType = RelationshipType.withName("regulation");
        relations.addRelationDocument(ImportIERelationDocument.of(
                "docId", ImportIETypedRelations.of(
                        regulationType.name(), ImportIERelation.of(
                                3, ImportIERelationArgument.of("CONCEPT0"), ImportIERelationArgument.of("id:tid1")))));

        ConceptManager cm = new ConceptManager(graphDBMS);
        cm.insertConcepts(importConcepts);
        cm.insertIERelations(relations);

        try (Transaction tx = graphDb.beginTx()) {
            Node n = tx.findNode(CONCEPT, PROP_ID, "tid0");
            assertThat(n).isNotNull();
            assertThat(n.getDegree(regulationType)).isEqualTo(1);
            Relationship rs = n.getSingleRelationship(regulationType, Direction.OUTGOING);
            assertThat(rs).isNotNull();
            assertThat(rs.hasProperty(PROP_TOTAL_COUNT));
            assertThat(rs.getProperty(PROP_TOTAL_COUNT)).isEqualTo(3);
        }
    }

    @Test
    public void testInsertMultipleIERelations() {
        ImportConcepts importConcepts = getTestConcepts(2);

        ImportIERelations relations = new ImportIERelations(PROP_ID);
        RelationshipType regulationType = RelationshipType.withName("regulation");
        // Note that we first insert concepts for docId2, then docId1. In the result, this should be sorted.
        relations.addRelationDocument(ImportIERelationDocument.of(
                "docId2", ImportIETypedRelations.of(
                        regulationType.name(), ImportIERelation.of(
                                4, ImportIERelationArgument.of("tid1"), ImportIERelationArgument.of("tid0")))));
        relations.addRelationDocument(ImportIERelationDocument.of(
                "docId1", ImportIETypedRelations.of(
                        regulationType.name(), ImportIERelation.of(
                                3, ImportIERelationArgument.of("tid0"), ImportIERelationArgument.of("tid1")))));

        ConceptManager cm = new ConceptManager(graphDBMS);
        cm.insertConcepts(importConcepts);
        cm.insertIERelations(relations);

        try (Transaction tx = graphDb.beginTx()) {
            Node n = tx.findNode(CONCEPT, PROP_ID, "tid0");
            assertThat(n).isNotNull();
            assertThat(n.getDegree(regulationType)).isEqualTo(1);
            Relationship rs = n.getSingleRelationship(regulationType, Direction.BOTH);
            assertThat(rs).isNotNull();
            assertThat(rs.hasProperty(PROP_TOTAL_COUNT));
            assertThat(rs.getProperty(PROP_TOTAL_COUNT)).isEqualTo(7);
            assertThat((String[]) rs.getProperty(PROP_DOC_IDS)).containsExactly("docId1", "docId2");
            assertThat((int[]) rs.getProperty(PROP_COUNTS)).containsExactly(3, 4);
        }
    }

    @Test
    public void testInsertDifferentReltypes() {
        ImportConcepts importConcepts = getTestConcepts(2);

        ImportIERelations relations = new ImportIERelations(PROP_ID);
        RelationshipType regulationType1 = RelationshipType.withName("regulation");
        RelationshipType regulationType2 = RelationshipType.withName("phosphorylation");
        relations.addRelationDocument(ImportIERelationDocument.of(
                "docId1", ImportIETypedRelations.of(
                        regulationType1.name(), ImportIERelation.of(
                                3, ImportIERelationArgument.of("tid0"), ImportIERelationArgument.of("tid1"))),
                ImportIETypedRelations.of(
                        regulationType2.name(), ImportIERelation.of(
                                7, ImportIERelationArgument.of("tid0"), ImportIERelationArgument.of("tid1")))));

        ConceptManager cm = new ConceptManager(graphDBMS);
        cm.insertConcepts(importConcepts);
        cm.insertIERelations(relations);

        try (Transaction tx = graphDb.beginTx()) {
            Node n = tx.findNode(CONCEPT, PROP_ID, "tid0");
            assertThat(n).isNotNull();
            assertThat(n.getDegree(regulationType1)).isEqualTo(1);
            Relationship rs = n.getSingleRelationship(regulationType1, Direction.BOTH);
            assertThat(rs).isNotNull();
            assertThat(rs.hasProperty(PROP_TOTAL_COUNT));
            assertThat(rs.getProperty(PROP_TOTAL_COUNT)).isEqualTo(3);
            assertThat((String[]) rs.getProperty(PROP_DOC_IDS)).containsExactly("docId1");
            assertThat((int[]) rs.getProperty(PROP_COUNTS)).containsExactly(3);

            rs = n.getSingleRelationship(regulationType2, Direction.BOTH);
            assertThat(rs).isNotNull();
            assertThat(rs.hasProperty(PROP_TOTAL_COUNT));
            assertThat(rs.getProperty(PROP_TOTAL_COUNT)).isEqualTo(7);
            assertThat((String[]) rs.getProperty(PROP_DOC_IDS)).containsExactly("docId1");
            assertThat((int[]) rs.getProperty(PROP_COUNTS)).containsExactly(7);
        }
    }

    @Test
    public void testInsertDifferentReltypes2() {
        ImportConcepts importConcepts = getTestConcepts(2);

        ImportIERelations relations = new ImportIERelations(PROP_ID);
        RelationshipType regulationType1 = RelationshipType.withName("regulation");
        RelationshipType phosphorylationType = RelationshipType.withName("phosphorylation");
        relations.addRelationDocument(ImportIERelationDocument.of(
                "docId1", ImportIETypedRelations.of(
                        regulationType1.name(), ImportIERelation.of(
                                3, ImportIERelationArgument.of("tid0"), ImportIERelationArgument.of("tid1"))),
                ImportIETypedRelations.of(
                        phosphorylationType.name(), ImportIERelation.of(
                                7, ImportIERelationArgument.of("tid0"), ImportIERelationArgument.of("tid1")))));
        relations.addRelationDocument(ImportIERelationDocument.of(
                "docId2", ImportIETypedRelations.of(
                        phosphorylationType.name(), ImportIERelation.of(
                                4, ImportIERelationArgument.of("tid1"), ImportIERelationArgument.of("tid0")))));

        ConceptManager cm = new ConceptManager(graphDBMS);
        cm.insertConcepts(importConcepts);
        cm.insertIERelations(relations);

        try (Transaction tx = graphDb.beginTx()) {
            Node n = tx.findNode(CONCEPT, PROP_ID, "tid0");
            assertThat(n).isNotNull();
            assertThat(n.getDegree(regulationType1)).isEqualTo(1);
            Relationship rs = n.getSingleRelationship(regulationType1, Direction.BOTH);
            assertThat(rs).isNotNull();
            assertThat(rs.hasProperty(PROP_TOTAL_COUNT));
            assertThat(rs.getProperty(PROP_TOTAL_COUNT)).isEqualTo(3);
            assertThat((String[]) rs.getProperty(PROP_DOC_IDS)).containsExactly("docId1");
            assertThat((int[]) rs.getProperty(PROP_COUNTS)).containsExactly(3);

            rs = n.getSingleRelationship(phosphorylationType, Direction.BOTH);
            assertThat(rs).isNotNull();
            assertThat(rs.hasProperty(PROP_TOTAL_COUNT));
            assertThat(rs.getProperty(PROP_TOTAL_COUNT)).isEqualTo(11);
            assertThat((String[]) rs.getProperty(PROP_DOC_IDS)).containsExactly("docId1", "docId2");
            assertThat((int[]) rs.getProperty(PROP_COUNTS)).containsExactly(7, 4);
        }
    }

    @Test
    public void testInsertIERelThreeArgs() {
        ImportConcepts importConcepts = getTestConcepts(3);

        ImportIERelations relations = new ImportIERelations(PROP_ID);
        RelationshipType regulationType = RelationshipType.withName("regulation");
        relations.addRelationDocument(ImportIERelationDocument.of(
                "docId1", ImportIETypedRelations.of(
                        regulationType.name(), ImportIERelation.of(
                                2, ImportIERelationArgument.of("tid0"), ImportIERelationArgument.of("tid1"), ImportIERelationArgument.of("tid2"))),
                ImportIETypedRelations.of(
                        regulationType.name(), ImportIERelation.of(
                                1, ImportIERelationArgument.of("tid2"), ImportIERelationArgument.of("tid1")))));

        ConceptManager cm = new ConceptManager(graphDBMS);
        cm.insertConcepts(importConcepts);
        cm.insertIERelations(relations);

        try (Transaction tx = graphDb.beginTx()) {
            Node n = tx.findNode(CONCEPT, PROP_ID, "tid0");
            assertThat(n).isNotNull();
            assertThat(n.getDegree(regulationType)).isEqualTo(2);
            List<Relationship> relationships = StreamSupport.stream(n.getRelationships(regulationType).spliterator(), false).collect(Collectors.toList());
            assertThat(relationships).hasSize(2);
            for (Relationship rs : relationships) {
                assertThat(rs).isNotNull();
                assertThat(rs.hasProperty(PROP_TOTAL_COUNT));
                assertThat(rs.getProperty(PROP_TOTAL_COUNT)).isEqualTo(2);
                assertThat((String[]) rs.getProperty(PROP_DOC_IDS)).containsExactly("docId1");
                assertThat((int[]) rs.getProperty(PROP_COUNTS)).containsExactly(2);
            }
            assertThat(relationships).extracting(r -> r.getOtherNode(n).getProperty(PROP_ID)).containsExactlyInAnyOrder("tid1", "tid2");


            Node n2 = tx.findNode(CONCEPT, PROP_ID, "tid2");
            assertThat(n2).isNotNull();
            assertThat(n2.getDegree(regulationType)).isEqualTo(2);
            Relationship rs = StreamSupport.stream(n2.getRelationships(regulationType).spliterator(), false).filter(r -> r.getOtherNode(n2).getProperty(PROP_ID).equals("tid1")).collect(Collectors.toList()).get(0);
            assertThat(rs).isNotNull();
            assertThat(rs.hasProperty(PROP_TOTAL_COUNT));
            assertThat(rs.getProperty(PROP_TOTAL_COUNT)).isEqualTo(3);
            assertThat((String[]) rs.getProperty(PROP_DOC_IDS)).containsExactly("docId1");
            assertThat((int[]) rs.getProperty(PROP_COUNTS)).containsExactly(3);

        }
    }

    @Test
    public void testConcurrentIERelationInsertion() throws InterruptedException {
        ConceptManager cm = new ConceptManager(graphDBMS);
        ImportConcepts importConcepts = getTestConcepts(3);

        RelationshipType regulationType = RelationshipType.withName("regulation");
        ImportIERelations relations1 = new ImportIERelations(PROP_ID);
        relations1.addRelationDocument(ImportIERelationDocument.of(
                "docId1", ImportIETypedRelations.of(
                        regulationType.name(), ImportIERelation.of(
                                2, ImportIERelationArgument.of("tid0"), ImportIERelationArgument.of("tid1")))));
        ImportIERelations relations2 = new ImportIERelations(PROP_ID);
        relations2.addRelationDocument(ImportIERelationDocument.of(
                "docId2", ImportIETypedRelations.of(
                        regulationType.name(), ImportIERelation.of(
                                3, ImportIERelationArgument.of("tid0"), ImportIERelationArgument.of("tid1")))));

        cm.insertConcepts(importConcepts);
        ConcurrentInsertionThread t1 = new ConcurrentInsertionThread(cm, relations1);
        ConcurrentInsertionThread t2 = new ConcurrentInsertionThread(cm, relations2);
        t1.start();
        t2.start();
        t1.join();
        t2.join();

        try (Transaction tx = graphDb.beginTx()) {
            Node n = tx.findNode(CONCEPT, PROP_ID, "tid0");
            // This is important: When we do not synchronize, multiple relationship would be created
            Relationship rel = n.getSingleRelationship(regulationType, Direction.BOTH);
            assertThat((String[]) rel.getProperty(PROP_DOC_IDS)).containsExactly("docId1", "docId2");
            assertThat((int[]) rel.getProperty(PROP_COUNTS)).containsExactly(2, 3);
        }
    }

    private class ConcurrentInsertionThread extends Thread {
        private ConceptManager cm;
        private ImportIERelations relations;

        public ConcurrentInsertionThread(ConceptManager cm, ImportIERelations relations) {
            this.cm = cm;
            this.relations = relations;
        }

        @Override
        public void run() {
            log.debug("START");
            try(Transaction tx = graphDb.beginTx()) {
                IERelationInsertion.insertRelations(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(relations).getBytes(UTF_8)), tx, new Slf4jLog(log));
                // This is for the threads to get a race condition instead running effectively serially
                Thread.sleep(500);
                tx.commit();
                log.debug("END");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}