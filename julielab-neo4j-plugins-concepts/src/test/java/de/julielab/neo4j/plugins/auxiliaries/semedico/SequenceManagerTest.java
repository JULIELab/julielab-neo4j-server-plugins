package de.julielab.neo4j.plugins.auxiliaries.semedico;

import de.julielab.neo4j.plugins.test.TestUtilities;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
public class SequenceManagerTest {
    private final static Logger log = LoggerFactory.getLogger(SequenceManagerTest.class);
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
                sequenceNames.add((String) hasSequence.getEndNode().getProperty(PROP_NAME));
                count++;
            }
            assertEquals(2, count);
            assertTrue(sequenceNames.contains("seq1"));
            assertTrue(sequenceNames.contains("seq2"));

            tx.commit();
        }

    }

    @Test
    public void concurrencyTest() throws InterruptedException {
        try (Transaction tx = graphDb.beginTx()) {
            SequenceManager.createIndexes(tx);
            tx.commit();
        }
        String seqName = "testseq";
        SeqConcurrencyThread t1 = new SeqConcurrencyThread(seqName);
        SeqConcurrencyThread t2 = new SeqConcurrencyThread(seqName);
        t1.start();
        t2.start();
        t1.join();
        t2.join();
        try (Transaction tx = graphDb.beginTx()) {
            // This is a test that we only have a single sequence root and sequence 'testseq' node.
            assertEquals("Not exactly one root sequence nodes.", 1, tx.findNodes(SequenceManager.SequenceLabel.SEQUENCE_ROOT).stream().count());
            assertEquals("Not exactly one " + seqName + " sequence nodes.", 1, tx.findNodes(SequenceManager.SequenceLabel.SEQUENCE, PROP_NAME, seqName).stream().count());
        }
        int v1 = t1.getValue();
        int v2 = t2.getValue();
        assertThat(v1).isGreaterThan(-1);
        assertThat(v2).isGreaterThan(-1);
        assertNotEquals(v1, v2);
    }

    private class SeqConcurrencyThread extends Thread {
        private final String seqName;
        private int value = -1;

        public SeqConcurrencyThread(String seqName) {
            this.seqName = seqName;
        }

        public int getValue() {
            return value;
        }

        @Override
        public void run() {
            try (Transaction tx = graphDb.beginTx()) {
                value = SequenceManager.getNextSequenceValue(tx, seqName);
                log.debug("After sequence value retrieval, before commit.");
                // The sleeping should give room for race conditions
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                tx.commit();
            }
            log.debug("After commit.");
        }
    }
}
