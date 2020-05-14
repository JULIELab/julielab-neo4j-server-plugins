package de.julielab.neo4j.plugins.auxiliaries.semedico;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.Schema;

import java.util.Optional;

import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_NAME;
import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_VALUE;

public class SequenceManager {

    private final static String SEQUENCE_INDEX = "sequenceIndex";

    public static int getNextSequenceValue(Transaction tx, String seqFacetGroup) {
        Node sequence = getSequence(tx, seqFacetGroup);
        tx.acquireReadLock(sequence);
        tx.acquireWriteLock(sequence);
        int currentSequenceValue = (Integer) sequence.getProperty(PROP_VALUE);
        sequence.setProperty(PROP_VALUE, currentSequenceValue + 1);
        return currentSequenceValue;
    }

    static Node getSequenceRoot(Transaction tx) {
        Optional<Node> sequenceRootOpt = tx.findNodes(Labels.SEQUENCE_ROOT).stream().findAny();
        if (!sequenceRootOpt.isPresent()) {
            Node seqRoot = tx.createNode(Labels.SEQUENCE_ROOT);
            return seqRoot;
        } else {
            return sequenceRootOpt.get();
        }
    }

    private static synchronized Node getSequence(Transaction tx, String sequenceName) {

        try {
            Node sequence = tx.findNode(Labels.SEQUENCE, PROP_NAME, sequenceName);
            if (null == sequence) {
                // First get or create the sequences super node.
                // We do a connection not for lookup but just so the graph is
                // connected
                // and everything easily visible when viewing the graph e.g.
                // with the
                // Neo4j server graph viewer.
                Node seqRoot = getSequenceRoot(tx);
                sequence = tx.createNode();
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
        if (!schema.getIndexes(Labels.SEQUENCE).iterator().hasNext()) {
            schema.indexFor(Labels.SEQUENCE).withName(SEQUENCE_INDEX).on(PROP_NAME).create();
        }
    }

    private enum EdgeTypes implements RelationshipType {
        HAS_SEQUENCE
    }

    private enum Labels implements Label {
        SEQUENCE_ROOT, SEQUENCE
    }

}
