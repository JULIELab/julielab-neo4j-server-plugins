package de.julielab.neo4j.plugins;

import de.julielab.neo4j.plugins.test.TestUtilities;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.*;

import java.util.Map;

import static org.junit.Assert.*;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class FullTextIndexUtilsTest {
    private static GraphDatabaseService graphDb;
    private static DatabaseManagementService graphDBMS;

    @BeforeClass
    public static void initialize() {
        graphDBMS = TestUtilities.getGraphDBMS();
        graphDb = graphDBMS.database(DEFAULT_DATABASE_NAME);
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
    public void getNode() {
        Label testLabel = Label.label("TESTLABEL");
        String testProp = "testProp";
        try (Transaction tx = graphDb.beginTx()) {
            FullTextIndexUtils.createTextIndex(tx, "ftTestIndex", null, new Label[]{testLabel}, new String[]{testProp});
            tx.commit();
        }
        try (Transaction tx = graphDb.beginTx()) {
            Node testNode = tx.createNode(testLabel);
            testNode.setProperty(testProp, "testvalue");
            Node receivedNode = FullTextIndexUtils.getNode(tx, "ftTestIndex", testProp, "testvalue");
            assertEquals(testNode.getId(), receivedNode.getId());
        }
    }

    @Test
    public void getNode2() {
        Label testLabel = Label.label("TESTLABEL");
        String testProp = "testProp";
        try (Transaction tx = graphDb.beginTx()) {
            FullTextIndexUtils.createTextIndex(tx, "ftTestIndex", Map.of("analyzer", "whitespace","eventually_consistent", "false"), new Label[]{testLabel}, new String[]{testProp});
            tx.commit();
        }
        try (Transaction tx = graphDb.beginTx()) {
            Node testNode = tx.createNode(testLabel);
            testNode.setProperty(testProp, "testvalue http://lala.org/someid");
            Node receivedNode = FullTextIndexUtils.getNode(tx, "ftTestIndex", testProp, "http://lala.org/someid");
            assertEquals(testNode.getId(), receivedNode.getId());
            // We do NOT want fuzzy search here
            assertNull(FullTextIndexUtils.getNode(tx, "ftTestIndex", "testprop", "http"));
            assertNull(FullTextIndexUtils.getNode(tx, "ftTestIndex", testProp, "http:/lala.org/someid"));
        }
    }

    @Test
    public void getNodeDifficultName() {
        Label testLabel = Label.label("TESTLABEL");
        String testProp = "testProp";
        try (Transaction tx = graphDb.beginTx()) {
            FullTextIndexUtils.createTextIndex(tx, "ftTestIndex", null, new Label[]{testLabel}, new String[]{testProp});
            tx.commit();
        }
        try (Transaction tx = graphDb.beginTx()) {
            Node testNode = tx.createNode(testLabel);
            testNode.setProperty(testProp, "http://some.url");
            ResourceIterator<Object> it = FullTextIndexUtils.getNodes(tx, "ftTestIndex", testProp, "http://some.url");
            assertTrue(it.hasNext());
            Node receivedNode = (Node) it.next();
            assertEquals(testNode.getId(), receivedNode.getId());
        }
    }
}