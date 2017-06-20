package de.julielab.neo4j.plugins.auxiliaries.semedico;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import de.julielab.neo4j.plugins.auxiliaries.semedico.SequenceManager;
import de.julielab.neo4j.plugins.constants.semedico.NodeConstants;
import de.julielab.neo4j.plugins.constants.semedico.SequenceConstants;
import de.julielab.neo4j.plugins.test.TestUtilities;

public class SequenceManagerTest {
	private static GraphDatabaseService graphDb;

	@BeforeClass
	public static void initialize() {
		graphDb = TestUtilities.getGraphDB();
	}

	@Before
	public void before() {
		TestUtilities.deleteEverythingInDB(graphDb);
	}

	@Test
	public void testGetNextSequenceValue() {
		// Check whether the sequences work as expected.
		assertEquals(0, SequenceManager.getNextSequenceValue(graphDb, "seq1"));
		assertEquals(0, SequenceManager.getNextSequenceValue(graphDb, "seq2"));
		assertEquals(1, SequenceManager.getNextSequenceValue(graphDb, "seq1"));
		assertEquals(1, SequenceManager.getNextSequenceValue(graphDb, "seq2"));
		assertEquals(2, SequenceManager.getNextSequenceValue(graphDb, "seq1"));
		assertEquals(2, SequenceManager.getNextSequenceValue(graphDb, "seq2"));

		// Check whether there is exactly one sequences node now with exactly
		// two sequence nodes (seq1 and seq2 from above) connected to it.
		try (Transaction tx = graphDb.beginTx()) {
			Node sequencesNode = SequenceManager.getSequenceRoot(graphDb);
			assertEquals(SequenceConstants.NAME_SEQUENCES_ROOT, sequencesNode.getProperty(NodeConstants.PROP_NAME));

			Iterable<Relationship> hasSequenceRels = sequencesNode.getRelationships(Direction.OUTGOING, SequenceManager.EdgeTypes.HAS_SEQUENCE);
			Set<String> sequenceNames = new HashSet<String>();
			int count = 0;
			for (Relationship hasSequence : hasSequenceRels) {
				sequenceNames.add((String) hasSequence.getEndNode().getProperty(NodeConstants.PROP_NAME));
				count++;
			}
			assertEquals(2, count);
			assertTrue(sequenceNames.contains("seq1"));
			assertTrue(sequenceNames.contains("seq2"));
			
			tx.success();
		}

	}

	@AfterClass
	public static void shutdown() {
		graphDb.shutdown();
	}
}
