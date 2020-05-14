package de.julielab.neo4j.plugins;

import de.julielab.neo4j.plugins.test.TestUtilities;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import static org.junit.Assert.assertEquals;
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
            FullTextIndexUtils.createTextIndex(tx, "ftTestIndex", new Label[]{testLabel}, new String[]{testProp});
            tx.commit();
        }
        try (Transaction tx = graphDb.beginTx()) {
            Node testNode = tx.createNode(testLabel);
            testNode.setProperty(testProp, "testvalue");
            Node receivedNode = FullTextIndexUtils.getNode(tx, "ftTestIndex", testProp, "testvalue");
            assertEquals(testNode.getId(), receivedNode.getId());
        }
    }
}