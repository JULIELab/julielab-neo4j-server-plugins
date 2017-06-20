package de.julielab.neo4j.plugins.auxiliaries.semedico;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import de.julielab.neo4j.plugins.auxiliaries.semedico.UniqueLabelsManager;
import de.julielab.neo4j.plugins.constants.semedico.NodeConstants;
import de.julielab.neo4j.plugins.test.TestUtilities;

public class UniqueLabelsManagerTest {

	private Logger log = Logger.getLogger(UniqueLabelsManagerTest.class.getSimpleName());

	private static GraphDatabaseService graphDb;

	@BeforeClass
	public static void initialize() {
		graphDb = TestUtilities.getGraphDB();
		TestUtilities.deleteEverythingInDB(graphDb);
	}

	@Test
	public void testAddUniqueLabel() {
		try (Transaction tx = graphDb.beginTx()) {
			Node node = graphDb.createNode();
			UniqueLabelsManager.addUniqueLabelToNode(graphDb, node, "myUniqueLabel");
			assertEquals("myUniqueLabel", ((String[]) node.getProperty(NodeConstants.PROP_UNIQUE_LABELS))[0]);

			boolean duplicateUniqueLabelRejected = false;
			try {
				UniqueLabelsManager.addUniqueLabelToNode(graphDb, node, "myUniqueLabel");
			} catch (IllegalArgumentException e) {
				log.info("Expected exception: " + e.getMessage());
				duplicateUniqueLabelRejected = true;
			}
			assertTrue(duplicateUniqueLabelRejected);
			assertEquals(1, ((String[]) node.getProperty(NodeConstants.PROP_UNIQUE_LABELS)).length);
			tx.success();
		}
	}

	@AfterClass
	public static void shutdown() {
		graphDb.shutdown();
	}
}
