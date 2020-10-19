package de.julielab.neo4j.plugins.auxiliaries.semedico;

import de.julielab.neo4j.plugins.Indexes;
import org.neo4j.graphdb.*;

import java.util.Optional;

import static de.julielab.neo4j.plugins.Indexes.createSinglePropertyIndexIfAbsent;
import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_NAME;
import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_VALUE;

public class SequenceManager {

    private final static String SEQUENCE_INDEX = "sequenceIndex";

    public static int getNextSequenceValue(Transaction tx, String sequenceName) {
        Node sequence = getSequence(tx, sequenceName);
        tx.acquireReadLock(sequence);
        tx.acquireWriteLock(sequence);
        int currentSequenceValue = (Integer) sequence.getProperty(PROP_VALUE);
        sequence.setProperty(PROP_VALUE, currentSequenceValue + 1);
        return currentSequenceValue;
    }
    public static int getCurrentSequenceValue(Transaction tx, String sequenceName) {
        Node sequence = getSequence(tx, sequenceName);
        int currentSequenceValue = (Integer) sequence.getProperty(PROP_VALUE);
        return currentSequenceValue;
    }

    static Node getSequenceRoot(Transaction tx) {
        Node root = null;
        Optional<Node> sequenceRootOpt = tx.findNodes(SequenceLabel.SEQUENCE_ROOT).stream().findAny();
        try {
            if (sequenceRootOpt.isEmpty()) {
                root = tx.createNode(SequenceLabel.SEQUENCE_ROOT);
                root.setProperty(PROP_NAME, "SequenceRoot");
            } else {
                root = sequenceRootOpt.get();
            }
        } catch (ConstraintViolationException e) {
            // In case the root had already been created concurrently.
            root.delete();
            root = tx.findNode(SequenceLabel.SEQUENCE_ROOT, PROP_NAME, "SequenceRoot");
        }
        return root;
    }

    private static synchronized Node getSequence(Transaction tx, String sequenceName) {

        try {
            Node sequence = tx.findNode(SequenceLabel.SEQUENCE, PROP_NAME, sequenceName);
            try {
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
            } catch (ConstraintViolationException e) {
                // This happens for concurrent sequence creation. We need to delete the node we created and use the one
                // that another thread created before us.
                sequence.delete();
                sequence = tx.findNode(SequenceLabel.SEQUENCE, PROP_NAME, sequenceName);
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
        createSinglePropertyIndexIfAbsent(tx, "SequenceRootIndex", SequenceLabel.SEQUENCE_ROOT, true, Indexes.PROVIDER_NATIVE_1_0, PROP_NAME);
        createSinglePropertyIndexIfAbsent(tx, "SequenceIndex", SequenceLabel.SEQUENCE, true, Indexes.PROVIDER_NATIVE_1_0, PROP_NAME);
    }

    public enum EdgeTypes implements RelationshipType {
        HAS_SEQUENCE
    }

    public enum SequenceLabel implements Label {
        SEQUENCE_ROOT, SEQUENCE
    }

}
