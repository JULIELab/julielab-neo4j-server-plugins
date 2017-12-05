package de.julielab.neo4j.plugins.auxiliaries.semedico;

import static de.julielab.neo4j.plugins.constants.semedico.SequenceConstants.NAME_SEQUENCES_ROOT;
import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_NAME;
import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_VALUE;

import java.util.NoSuchElementException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;

import de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants;

public class SequenceManager {

	private final static String SEQUENCE_INDEX = "sequenceIndex";

	static enum EdgeTypes implements RelationshipType {
		HAS_SEQUENCE
	}

	static enum Labels implements Label {
		SEQUENCE_ROOT
	}

	public static int getNextSequenceValue(GraphDatabaseService graphDb, String seqFacetGroup) {
		try (Transaction tx = graphDb.beginTx()) {
			Node sequence = getSequence(graphDb, seqFacetGroup);
			tx.acquireReadLock(sequence);
			tx.acquireWriteLock(sequence);
			int currentSequenceValue = (Integer) sequence.getProperty(PROP_VALUE);
			sequence.setProperty(PROP_VALUE, currentSequenceValue + 1);
			tx.success();
			return currentSequenceValue;
		}
	}

	static Node getSequenceRoot(GraphDatabaseService graphDb) {
		Index<Node> rootIndex = graphDb.index().forNodes(NodeConstants.INDEX_ROOT_NODES);
		Node seqRoot = rootIndex.get(NodeConstants.PROP_NAME, NAME_SEQUENCES_ROOT).getSingle();
		if (null == seqRoot) {
			seqRoot = graphDb.createNode();
			seqRoot.setProperty(PROP_NAME, NAME_SEQUENCES_ROOT);
			rootIndex.add(seqRoot, PROP_NAME, NAME_SEQUENCES_ROOT);
		}
		return seqRoot;
	}

	private static synchronized Node getSequence(GraphDatabaseService graphDb, String seqFacetGroup) {

		try {
			IndexHits<Node> indexHits = null;
			try //(Transaction tx = graphDb.beginTx()) 
			{
				Index<Node> seqIndex = graphDb.index().forNodes(SEQUENCE_INDEX);
				indexHits = seqIndex.get(NodeConstants.PROP_NAME, seqFacetGroup);
				Node sequence = indexHits.getSingle();
				if (null == sequence) {
					// First get or create the sequences super node.
					// We do a connection not for lookup but just so the graph is
					// connected
					// and everything easily visible when viewing the graph e.g.
					// with the
					// Neo4j server graph viewer.
					Node seqRoot = getSequenceRoot(graphDb);
					sequence = graphDb.createNode();
					sequence.setProperty(PROP_NAME, seqFacetGroup);
					sequence.setProperty(PROP_VALUE, 0);
					seqIndex.putIfAbsent(sequence, PROP_NAME, seqFacetGroup);
					seqRoot.createRelationshipTo(sequence, EdgeTypes.HAS_SEQUENCE);
//					tx.success();
				}
				return sequence;
			} 
			finally {
				if (null != indexHits)
					indexHits.close();
			}
		} catch (NoSuchElementException e) {
			throw new IllegalStateException("More than one sequence node for the sequence with name \"" + seqFacetGroup + "\" was returned.", e);
		}
	}

}
