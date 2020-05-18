package de.julielab.neo4j.plugins.auxiliaries.semedico;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.Schema;

import java.util.Optional;

import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_NAME;
import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_VALUE;

public class SequenceManager {

    private final static String SEQUENCE_INDEX = "sequenceIndex";

    public static int getNextSequenceValue(Transaction tx, String sequenceName) {
        Node sequence = getSequence(tx, sequenceName);
        Lock readLock = tx.acquireReadLock(sequence);
        Lock writeLock = tx.acquireWriteLock(sequence);
        int currentSequenceValue = (Integer) sequence.getProperty(PROP_VALUE);
        sequence.setProperty(PROP_VALUE, currentSequenceValue + 1);
        readLock.release();
        writeLock.release();
        return currentSequenceValue;
    }

    static Node getSequenceRoot(Transaction tx) {
        Optional<Node> sequenceRootOpt = tx.findNodes(SequenceLabel.SEQUENCE_ROOT).stream().findAny();
        if (sequenceRootOpt.isEmpty()) {
            return tx.createNode(SequenceLabel.SEQUENCE_ROOT);
        } else {
            return sequenceRootOpt.get();
        }
    }

    private static synchronized Node getSequence(Transaction tx, String sequenceName) {

        try {
            Node sequence = tx.findNode(SequenceLabel.SEQUENCE, PROP_NAME, sequenceName);
            if (null == sequence) {
                // First get or create the sequences super node.
                // We do a connection not for lookup but just so the graph is
                // connected
                // and everything easily visible when viewing the graph e.g.
                // with the
                // Neo4j server graph viewer.
                Node seqRoot = getSequenceRoot(tx);
                sequence = tx.createNode(SequenceLabel.SEQUENCE);
                sequence.setProperty(PROP_NAME, sequenceName);
                sequence.setProperty(PROP_VALUE, 0);
                seqRoot.createRelationshipTo(sequence, EdgeTypes.HAS_SEQUENCE);
            }
            return sequence;
        } catch (MultipleFoundException e) {
            throw new IllegalStateException("More than one sequence node for the sequence with name \"" + sequenceName + "\" was returned.", e);
        }
    }

    /**
     * Creates a schema index for the sequence nodes.
     */
    public static void createIndexes(Transaction tx) {
        Schema schema = tx.schema();
        if (!schema.getIndexes(SequenceLabel.SEQUENCE).iterator().hasNext()) {
            schema.indexFor(SequenceLabel.SEQUENCE).withName(SEQUENCE_INDEX).on(PROP_NAME).create();
        }
    }

    public enum EdgeTypes implements RelationshipType {
        HAS_SEQUENCE
    }

    public enum SequenceLabel implements Label {
        SEQUENCE_ROOT, SEQUENCE
    }

}
