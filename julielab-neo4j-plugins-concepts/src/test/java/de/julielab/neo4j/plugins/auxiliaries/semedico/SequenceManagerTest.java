package de.julielab.neo4j.plugins.auxiliaries.semedico;

import de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants;
import de.julielab.neo4j.plugins.test.TestUtilities;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.*;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class SequenceManagerTest {
	private static GraphDatabaseService graphDb;
	private static DatabaseManagementService graphDBMS;

	@BeforeClass
	public static void initialize() {
		graphDBMS = TestUtilities.getGraphDBMS();
		graphDb = graphDBMS.database(DEFAULT_DATABASE_NAME);
	}

	@Before
	public void before() {
		TestUtilities.deleteEverythingInDB(graphDb);
	}

	@Test
	public void testGetNextSequenceValue() {
		// Check whether the sequences work as expected.
		try (Transaction tx = graphDb.beginTx()) {
			assertEquals(0, SequenceManager.getNextSequenceValue(tx, "seq1"));
			assertEquals(0, SequenceManager.getNextSequenceValue(tx, "seq2"));
			assertEquals(1, SequenceManager.getNextSequenceValue(tx, "seq1"));
			assertEquals(1, SequenceManager.getNextSequenceValue(tx, "seq2"));
			assertEquals(2, SequenceManager.getNextSequenceValue(tx, "seq1"));
			assertEquals(2, SequenceManager.getNextSequenceValue(tx, "seq2"));
			tx.commit();
		}

		// Check whether there is exactly one sequences node now with exactly
		// two sequence nodes (seq1 and seq2 from above) connected to it.
		try (Transaction tx = graphDb.beginTx()) {
			Node sequencesNode = SequenceManager.getSequenceRoot(tx);
			Iterable<Relationship> hasSequenceRels = sequencesNode.getRelationships(Direction.OUTGOING, SequenceManager.EdgeTypes.HAS_SEQUENCE);
			Set<String> sequenceNames = new HashSet<>();
			int count = 0;
			for (Relationship hasSequence : hasSequenceRels) {
				sequenceNames.add((String) hasSequence.getEndNode().getProperty(NodeConstants.PROP_NAME));
				count++;
			}
			assertEquals(2, count);
			assertTrue(sequenceNames.contains("seq1"));
			assertTrue(sequenceNames.contains("seq2"));
			
			tx.commit();
		}

	}

	@AfterClass
	public static void shutdown() {
		graphDBMS.shutdown();
	}
}
