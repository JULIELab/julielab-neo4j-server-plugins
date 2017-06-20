package de.julielab.neo4j.plugins.ahocorasick;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.Stack;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.Name;
import org.neo4j.server.plugins.Parameter;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;
import org.neo4j.shell.util.json.JSONException;
import org.neo4j.shell.util.json.JSONObject;

import com.google.gson.Gson;

import de.julielab.neo4j.plugins.ahocorasick.property.ACDictionary;
import de.julielab.neo4j.plugins.ahocorasick.property.ACEntry;

public class ACRefactory extends ServerPlugin{

	/******** DELETE ENTRY ************************/

	/**
	 * 
	 * @param graphDb
	 * @param name
	 * @param entry
	 * @return
	 * @throws NoSuchFieldException
	 * @throws SecurityException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws JSONException
	 */
	@Name("delete_entry_in_dicttree")
	@Description("Delete entry in the DictTree.")
	@PluginTarget(GraphDatabaseService.class)
	public static boolean deleteEntryInDictTree(@Source GraphDatabaseService graphDb, 
			@Description("ACDictionary as JSONString") @Parameter(name = "dict") String dictJSON,
			@Description("Entry to delete") @Parameter(name = "entry") String entry) 
					throws NoSuchFieldException, SecurityException, IllegalArgumentException,
					IllegalAccessException, JSONException {

		// Initialisierung des Graphens
		Transaction tx = graphDb.beginTx();
		
		ACDictionary dict = new ACDictionary(new JSONObject(dictJSON));
		
		try{
			Node root = ACUtil.getRootNode(graphDb, dict.name());
			Node node = ACUtil.getExactNode(dict, graphDb, root, entry);
			
			if (node != null) {
				
				boolean original = true;
				
				Deque<Node> queue = new ArrayDeque<Node>();
				queue.add(node);
				
				ArrayList<Node> failNodes = new ArrayList<Node>();
				
				// Lösche zu nächste alle Knoten am äußeren Ränd
				while (!queue.isEmpty()) {
					
					node = queue.pop();
					
					if (isBoundaryNode(graphDb, node, root, entry)) {
						
						Iterator<Relationship> iterRel = node.getRelationships(Direction.INCOMING).iterator();
						
						while (iterRel.hasNext()) {
							Relationship rel = iterRel.next();
							
							if (rel.getType().name().equals(ACProperties.getFailName())) {
								failNodes.add(rel.getStartNode());
								ACUtil.updateRemoveNode(dict, rel.getStartNode(), 
										rel.getEndNode(), ACProperties.getFailName());
							} else {
								queue.add(rel.getStartNode());
								ACUtil.updateRemoveNode(dict, rel.getStartNode(), rel.getEndNode(), (String) rel.getProperty(ACProperties.LETTER));
							}
							
							ACUtil.decreaseNumberNext(rel.getStartNode());
							rel.delete();
						}
						
						node.getSingleRelationship(ACProperties.EdgeTypes.FAIL, Direction.OUTGOING).delete();
						node.delete();
						
						original = false;
					}
				}
				
				// Eintrag löschen, wenn Knoten auf der Strecke liegt
				if (!isBoundaryNode(graphDb, node, root, entry) && original) {
					
					Stack<Node> stackNodeFail = new Stack<Node>();
					stackNodeFail.add(node);
					
					while (!stackNodeFail.isEmpty()) {
						Node relToNodeFail = stackNodeFail.pop();
						
						Iterator<Relationship> iterRelFail = relToNodeFail
								.getRelationships(Direction.INCOMING, ACProperties.EdgeTypes.FAIL).iterator();
						
						while (iterRelFail.hasNext()) {
							stackNodeFail.add(iterRelFail.next().getStartNode());
						}
						
						deleteOutput(relToNodeFail, entry);
					}
					
				}
				
				// Neu Failrelationen setzen
				Collections.sort(failNodes, ACUtil.getComperator());
				
				for (int i = 0; i < failNodes.size(); i++) {
					
					Node nodeFail = failNodes.get(i);
					
					// Delete all
					if (nodeFail.hasProperty(ACProperties.OUTPUT)) {
						
						Stack<Node> stackNodeFail = new Stack<Node>();
						stackNodeFail.add(nodeFail);
						
						while (!stackNodeFail.isEmpty()) {
							Node relToNodeFail = stackNodeFail.pop();
							
							Iterator<Relationship> iterRelFail = relToNodeFail
									.getRelationships(Direction.INCOMING, ACProperties.EdgeTypes.FAIL).iterator();
							
							while (iterRelFail.hasNext()) {
								stackNodeFail.add(iterRelFail.next().getStartNode());
							}
							
							deleteOutput(relToNodeFail, entry);
						}
					}
					
					Iterator<Relationship> iterRelNodeBefore = nodeFail.getRelationships(Direction.INCOMING).iterator();
					Relationship rela = null;
					while (iterRelNodeBefore.hasNext()) {
						rela = iterRelNodeBefore.next();
						if (!rela.getType().name().equals(ACProperties.getFailName())) {
							node = rela.getStartNode();
							break;
						}
					}
					
					Node state = graphDb.getNodeById(ACUtil.getFailNode(node, root));
					Node toNode = null;
					
					while ((toNode = ACUtil.getNextNodeSearch(dict, graphDb, state, (String) rela.getProperty(ACProperties.LETTER))) == null) {
						state = graphDb.getNodeById(ACUtil.getFailNode(state, root));
					}
					
					nodeFail.createRelationshipTo(toNode, ACProperties.EdgeTypes.FAIL);
					// UPDATE
					ACUtil.updateAddNode(dict, nodeFail, toNode, ACProperties.getFailName());
					ACUtil.increaseNumberNext(nodeFail);
					
					// UPDATE DER ANDEREN FAIL-VERBINDUNGEN !!
					Stack<Node> stackNodeOutput = new Stack<Node>();
					stackNodeOutput.add(nodeFail);
					
					while (!stackNodeOutput.isEmpty()) {
						Node relToNodeFail = stackNodeOutput.pop();
						
						Iterator<Relationship> iterRelFail = relToNodeFail
								.getRelationships(Direction.INCOMING, ACProperties.EdgeTypes.FAIL).iterator();
						
						while (iterRelFail.hasNext()) {
							stackNodeOutput.add(iterRelFail.next().getStartNode());
						}
						
						ACUtil.unionOutput(relToNodeFail, toNode);
					}
					
				}
				
			}			
			tx.success();
			return node != null;
			
		}finally{
			tx.close();
		}
	}
	
	/**
	 * Change attributes of an entry in the DictTree. The function get this attributes as a map where the key is the name of the attribute which has to be change and the value is
	 * the value which should be new. Example: {att1: attVal1, att2: attVal2} NO BREAK UP IF AN ATTRIBUTE DOES NOT EXIST!!!
	 * 
	 * @param graphDb
	 *            - GraphDatabase
	 * @param name
	 *            - Name of the DictTree
	 * @param entry
	 *            - Name of the entry
	 * @param attribute
	 *            - Map of the attributes which has to be changed with the new value
	 * @return <code>true</code> if success, <code>false</code> if the attribute does not exist
	 * @throws JSONException
	 */
	@Name("change_attribute_of_entry")
	@Description("Change Attributevalues of an entry in a DictTree.")
	@PluginTarget(GraphDatabaseService.class)
	public static boolean editEntry(@Source GraphDatabaseService graphDb,
			@Description("ACDictionary as JSONString") @Parameter(name = "dict") String dictJSON,
			@Description("Entry to change") @Parameter(name = "entry") String entryACJSON)
			throws JSONException {

		ACDictionary dict = new ACDictionary(new JSONObject(dictJSON));
		ACEntry entry = new ACEntry(new JSONObject(entryACJSON));
		
		Transaction tx = graphDb.beginTx();
		
		try{
			boolean success = true;
			
			// Find the root of the DictTree
			Node root = ACUtil.getRootNode(graphDb, dict.name());
			
			if (root != null) {
				// Find original node
				Node node = ACUtil.getExactNode(dict, graphDb, root, entry.entryString());
				
				// Change Properties in the original node
				if (node == null) {
					tx.success();
					return false;
				}
				
				// Change the properties of the node which are connected with a fail
				// relation to the node
				Deque<Node> queue = new ArrayDeque<Node>();
				queue.add(node);
				
				while (!queue.isEmpty()) {
					
					Node startNode = queue.pop();
					JSONObject jsonOb = new JSONObject(String.valueOf(node.getProperty(ACProperties.OUTPUT)));
					jsonOb.putOpt(entry.entryString(), new Gson().toJson(entry.getAllAttributes()));
					startNode.setProperty(ACProperties.OUTPUT, jsonOb.toString());
					
					Iterator<Relationship> iterRel = startNode.getRelationships(Direction.INCOMING).iterator();
					while (iterRel.hasNext()) {
						Relationship rel = iterRel.next();
						if (rel.isType(ACProperties.EdgeTypes.FAIL)) {
							Node failNode = rel.getStartNode();
							queue.add(failNode);
						}
					}
				}
				
			}
			
			tx.success();
			
			return success;
			
		}finally{
			tx.close();
		}
	}
	
	private static void deleteOutput(Node node, String output) throws JSONException {
		JSONObject ob = new JSONObject(String.valueOf(node.getProperty(ACProperties.OUTPUT)));
		if (ob.has(output)) {
			ob.remove(output);
			node.setProperty(ACProperties.OUTPUT, ob.toString());
			node.setProperty(ACProperties.NUMBER_OUTPUT
					, ((Integer) node.getProperty(ACProperties.NUMBER_OUTPUT)) - 1);
		}

		if (node.hasProperty(ACProperties.ORIGINAL)) {
			if (node.getProperty(ACProperties.ORIGINAL).equals(output)) {
				node.removeProperty(ACProperties.ORIGINAL);
			}
		}
	}
	
	/**
	 * 
	 * @param graphDb
	 * @param node
	 * @param prepared
	 * @return
	 */
	private static boolean isBoundaryNode(GraphDatabaseService graphDb, Node node, Node root, String entry) {

		if (root.equals(node)) {
			return false;
		}

		if (ACUtil.toLong(node.getProperty(ACProperties.NUMBER_OUTPUT)) > 0 
				&& node.hasProperty(ACProperties.ORIGINAL)) {
			if (!node.getProperty(ACProperties.ORIGINAL).equals(entry)) {
				return false;
			}
		}

		Iterator<Relationship> iterRel = node.getRelationships(Direction.OUTGOING).iterator();

		int count = 0;

		while (iterRel.hasNext() && count < 2) {
			iterRel.next();
			count++;
		}

		boolean prepared = ACUtil.isDictTreePrepared(root);

		if (prepared && count == 1) {
			return true;
		}

		if (count == 0) {
			return true;
		}

		return false;

	}
	
}
