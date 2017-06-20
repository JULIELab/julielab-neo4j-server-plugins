package de.julielab.neo4j.plugins.auxiliaries.semedico;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.shell.util.json.JSONArray;
import org.neo4j.shell.util.json.JSONException;

import de.julielab.neo4j.plugins.constants.semedico.NodeConstants;

/**
 * Used to ensure unique values in array-valued properties. For non-array values, a unique constraint in conjunction with an Neo4j schema index may be used.
 * 
 * @author faessler
 * @deprecated This is done via a unique constraint on the respective labels since Neo4j 2.0.
 */
@Deprecated
public class UniqueLabelsManager {

	public static final String INDEX_UNIQUE_LABELS = "uniqueLabelIndex";

	public static void addUniqueLabelToNode(GraphDatabaseService graphDb, Node node, String uniqueLabel) {
		IndexManager indexManager = graphDb.index();
		Index<Node> uniqueLabelIndex = indexManager.forNodes(INDEX_UNIQUE_LABELS);
		Node previousEntity = uniqueLabelIndex.putIfAbsent(node, NodeConstants.PROP_UNIQUE_LABELS, uniqueLabel);
		if (null != previousEntity) {
			throw new IllegalArgumentException("The node \"" + node + "\" cannot receive the unique label \"" + uniqueLabel
					+ "\" because this label has already been applied to the node \"" + previousEntity + "\".");
		}
		NodeUtilities.addToArrayProperty(node, NodeConstants.PROP_UNIQUE_LABELS, uniqueLabel);
	}

	public static void addUniqueLabelsToNode(GraphDatabaseService graphDb, Node node, JSONArray uniqueLabels) throws JSONException {
		if (null == uniqueLabels)
			return;
		for (int i = 0; i < uniqueLabels.length(); i++) {
			String uniqueLabel = uniqueLabels.getString(i);
			UniqueLabelsManager.addUniqueLabelToNode(graphDb, node, uniqueLabel);
		}
	}
}
