package de.julielab.neo4j.plugins;

import static de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants.FACET_GROUP;
import static de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants.NAME_FACET_GROUPS;
import static de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants.NAME_NO_FACET_GROUPS;
import static de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants.NO_FACET;
import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_ID;
import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_LABELS;
import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_NAME;
import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_UNIQUE_LABELS;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.TraversalMetadata;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.Name;
import org.neo4j.server.plugins.Parameter;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;
import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.MappingRepresentation;
import org.neo4j.server.rest.repr.RecursiveMappingRepresentation;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.shell.util.json.JSONArray;
import org.neo4j.shell.util.json.JSONException;
import org.neo4j.shell.util.json.JSONObject;

import de.julielab.neo4j.plugins.ConceptManager.ConceptLabel;
import de.julielab.neo4j.plugins.auxiliaries.JSON;
import de.julielab.neo4j.plugins.auxiliaries.NodeUtilities;
import de.julielab.neo4j.plugins.auxiliaries.PropertyUtilities;
import de.julielab.neo4j.plugins.auxiliaries.semedico.PredefinedTraversals;
import de.julielab.neo4j.plugins.auxiliaries.semedico.SequenceManager;
import de.julielab.neo4j.plugins.constants.semedico.SequenceConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetGroupConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.NodeIDPrefixConstants;

@Description("This plugin offers access to facets for Semedico.")
public class FacetManager extends ServerPlugin {

	/**
	 * Key of the map to send to the {@link #INSERT_FACETS} endpoint.
	 */
	public static final String KEY_FACETS = "facets";
	public static final String KEY_ID = "id";
	public static final String GET_FACETS = "get_facets";
	public static final String INSERT_FACETS = "insert_facets";
	public static final String GET_FACET_SIZE = "get_facet_size";
	public static final String PARAM_RETURN_HOLLOW_FACETS = "returnHollowFacets";
	private static final Logger log = Logger.getLogger(FacetManager.class.getName());

	public static enum EdgeTypes implements RelationshipType {
		HAS_FACET_GROUP, HAS_FACET
	}

	public static enum FacetLabel implements Label {
		FACET, NO_FACET
	}

	@Name(GET_FACET_SIZE)
	@Description("Returns the size of a facet by counting all the related children")
	@PluginTarget(GraphDatabaseService.class)
	public int getFacetSize(@Source GraphDatabaseService graphDb,
			@Description("TODO") @Parameter(name = KEY_ID) String fid) throws JSONException {
		return countFacetChildren(graphDb, fid);
	}

	@Name(INSERT_FACETS)
	// TODO
	@Description("TODO")
	@PluginTarget(GraphDatabaseService.class)
	public ListRepresentation insertFacets(@Source GraphDatabaseService graphDb,
			@Description("TODO") @Parameter(name = KEY_FACETS) String facetList) throws JSONException {
		JSONArray input = new JSONArray(new String(facetList));
		List<Node> facets = new ArrayList<Node>();
		for (int i = 0; i < input.length(); i++) {
			JSONObject jsonFacet = input.getJSONObject(i);
			Node facet = createFacet(graphDb, jsonFacet);
			facets.add(facet);
		}

		try (Transaction tx = graphDb.beginTx()) {
			// The response is a list - according to the input order - where for
			// each facet is shown its name and which ID it received.
			List<Representation> facetRepList = new ArrayList<Representation>();
			for (Node facet : facets) {
				Map<String, Object> map = new HashMap<String, Object>();
				map.put(PROP_NAME, facet.getProperty(PROP_NAME));
				map.put(PROP_ID, facet.getProperty(PROP_ID));
				RecursiveMappingRepresentation facetResponseRep = new RecursiveMappingRepresentation(Representation.MAP,
						map);
				facetRepList.add(facetResponseRep);
			}
			ListRepresentation listRep = new ListRepresentation(Representation.MAP, facetRepList);
			tx.success();
			return listRep;
		}

	}

	@SuppressWarnings("unchecked")
	@Name(GET_FACETS)
	// TODO
	@Description("TODO")
	@PluginTarget(GraphDatabaseService.class)
	public MappingRepresentation getFacets(@Source GraphDatabaseService graphDb,
			@Description("TODO") @Parameter(name = PARAM_RETURN_HOLLOW_FACETS, optional = true) Boolean returnHollowfacets)
					throws JSONException {
		// As of Neo4j 2.0, read operations are required to be inside a
		// transaction.
		RecursiveMappingRepresentation facetGroupsRep;
		try (Transaction tx = graphDb.beginTx()) {
			Node facetGroupsNode = getFacetGroupsNode(graphDb);
			TraversalDescription td = PredefinedTraversals.getFacetTraversal(graphDb);
			Traverser traverse = td.traverse(facetGroupsNode);

			Map<String, Object> facetsByFacetGroupName = new HashMap<String, Object>();
			Map<String, Node> facetGroupsMap = new HashMap<String, Node>();
			List<Map<String, Object>> facetGroupsWithFacetsList = new ArrayList<Map<String, Object>>();

			// First build intermediate maps where the facet group nodes and
			// facets
			// are
			// organized by facet group name.
			for (Path facetPath : traverse) {
				Node facet = facetPath.endNode();
				Object sourceType = facet.getProperty(FacetConstants.PROP_SOURCE_TYPE);
				boolean isFacetWithoutPredefinedRoots = !sourceType.equals(FacetConstants.SRC_TYPE_HIERARCHICAL);

				// For string-sourced facets it doesn't make sense to check
				// their roots since they have none in the
				// database anyway. Same with flat facets that don't have a
				// hierarchic structure.
				if (!isFacetWithoutPredefinedRoots) {
					// Leave out facets without any root terms (this may happen
					// by some weird BioPortal ontologies).
					Iterator<Relationship> rootIt = facet.getRelationships(ConceptManager.EdgeTypes.HAS_ROOT_CONCEPT)
							.iterator();
					if (!rootIt.hasNext())
						continue;
					// Also leave out facets that only have Hollow root terms
					// (happens with BioPortal ontology IMMDIS
					// for
					// example since all classes there seem to be subclasses of
					// classes that are not defined anywhere).
					if (!returnHollowfacets) {
						boolean onlyHollowRoots = true;
						while (rootIt.hasNext() && onlyHollowRoots) {
							Node rootTerm = rootIt.next().getEndNode();
							boolean isHollow = false;
							for (Label label : rootTerm.getLabels()) {
								if (label.equals(ConceptLabel.HOLLOW)) {
									isHollow = true;
									break;
								}
							}
							if (!isHollow)
								onlyHollowRoots = false;
						}
						if (onlyHollowRoots)
							continue;
					}
				}

				Iterable<Relationship> facetRels = facet.getRelationships(Direction.INCOMING, EdgeTypes.HAS_FACET);
				for (Relationship facetRel : facetRels) {
					Node facetGroupNode = facetRel.getStartNode();
					String facetGroupName = (String) facetGroupNode.getProperty(PROP_NAME);

					facetGroupsMap.put(facetGroupName, facetGroupNode);

					List<Object> facets = (List<Object>) facetsByFacetGroupName.get(facetGroupName);
					if (facets == null) {
						facets = new ArrayList<Object>();
						facetsByFacetGroupName.put(facetGroupName, facets);
					}
					facets.add(facet);
				}

			}

			// Now connect the intermediate maps into a final list of facet
			// groups
			// where each facet group contains the facets belonging to it.
			for (String facetGroupName : facetsByFacetGroupName.keySet()) {
				Node facetGroupNode = facetGroupsMap.get(facetGroupName);
				Object facets = facetsByFacetGroupName.get(facetGroupName);

				Map<String, Object> facetGroupMap = new HashMap<String, Object>();
				for (String propKey : facetGroupNode.getPropertyKeys())
					facetGroupMap.put(propKey, facetGroupNode.getProperty(propKey));
				List<String> facetGroupLabels = new ArrayList<>();
				for (Label label : facetGroupNode.getLabels())
					facetGroupLabels.add(label.name());
				facetGroupMap.put(FacetGroupConstants.KEY_LABELS, facetGroupLabels);
				facetGroupMap.put(KEY_FACETS, facets);

				facetGroupsWithFacetsList.add(facetGroupMap);
			}
			Map<String, Object> ret = new HashMap<String, Object>();
			ret.put("facetGroups", facetGroupsWithFacetsList);
			facetGroupsRep = new RecursiveMappingRepresentation(Representation.MAP, ret);
			tx.success();
		}

		return facetGroupsRep;
	}

	public static Node createFacet(GraphDatabaseService graphDb, JSONObject jsonFacet) throws JSONException {
		log.info("Creating facet with the following data: " + jsonFacet);

		JSONArray generalLabels = JSON.getJSONArray(jsonFacet, PROP_LABELS);
		JSONArray uniqueLabels = JSON.getJSONArray(jsonFacet, PROP_UNIQUE_LABELS);
		boolean isNoFacet = JSON.getBoolean(jsonFacet, NO_FACET);

		JSONObject jsonFacetGroup = jsonFacet.getJSONObject(FACET_GROUP);

		try (Transaction tx = graphDb.beginTx()) {
			Node facetGroupsNode;
			if (isNoFacet)
				facetGroupsNode = getNoFacetGroupsNode(graphDb);
			else
				facetGroupsNode = getFacetGroupsNode(graphDb);
			Node facetGroup = createFacetGroup(graphDb, facetGroupsNode, jsonFacetGroup);

			// Create the actual facet node and populate it with data.
			Node facet = graphDb.createNode(FacetLabel.FACET);
			PropertyUtilities.copyJSONObjectToPropertyContainer(jsonFacet, facet, NO_FACET, PROP_LABELS,
					PROP_UNIQUE_LABELS, FACET_GROUP);

			// If everything is alright, get an ID for the facet.
			String facetId = NodeIDPrefixConstants.FACET
					+ SequenceManager.getNextSequenceValue(graphDb, SequenceConstants.SEQ_FACET);
			facet.setProperty(PROP_ID, facetId);
			facetGroup.createRelationshipTo(facet, EdgeTypes.HAS_FACET);
			if (null != generalLabels) {
				for (int i = 0; i < generalLabels.length(); i++) {
					String labelString = generalLabels.getString(i);
					Label label = DynamicLabel.label(labelString);
					facet.addLabel(label);
				}
			}
			if (null != uniqueLabels) {
				for (int i = 0; i < uniqueLabels.length(); i++) {
					String labelString = uniqueLabels.getString(i);
					Label label = DynamicLabel.label(labelString);
					facet.addLabel(label);
				}
			}

			tx.success();
			return facet;
		}
	}

	private static int countFacetChildren(GraphDatabaseService graphDb, String fid) {
		// -1, because starting node gets counted, too
		int childCount = -1;
		try (Transaction tx = graphDb.beginTx()) {
			// there is a relationship of the type "IS_BROADER_THAN_fidxxx" for
			// every facet
			// they need to be created dynamically because it makes no sense to
			// store all of them beforehand
			RelationshipType dynRel = DynamicRelationshipType.withName("IS_BROADER_THAN_" + fid);
			Node node = getFacetNode(graphDb, fid);

			Traverser traverser = graphDb.traversalDescription().breadthFirst().uniqueness(Uniqueness.NODE_GLOBAL)
					.relationships(ConceptManager.EdgeTypes.HAS_ROOT_CONCEPT, Direction.OUTGOING)
					.relationships(dynRel, Direction.OUTGOING).traverse(node);
			for (@SuppressWarnings("unused")
			Node n : traverser.nodes()) {
				childCount++;
			}
		}
		return childCount;
	}

	/**
	 * Get the node with name <tt>facetGroupName</tt> which is connected as a
	 * facet group to <tt>facetGroupsNode</tt> or <tt>null</tt> if no such node
	 * exists.
	 * 
	 * @param graphDb
	 * @param facetGroupsNode
	 * @param facetGroupName
	 * @return
	 */
	private static Node getFacetGroup(GraphDatabaseService graphDb, Node facetGroupsNode, String facetGroupName) {
		// Traversal to get the particular facet group node with id
		// 'facetGroupId' (and the facet groups node, in case we yet need to
		// create the facet group node).
		TraversalDescription td = PredefinedTraversals.getFacetGroupTraversal(graphDb, facetGroupName);
		Traverser traverse = td.traverse(facetGroupsNode);

		TraversalMetadata metadata = traverse.metadata();
		if (metadata != null && metadata.getNumberOfPathsReturned() > 1)
			throw new IllegalStateException(
					"There is more than one path from the reference node to the facet group node with name '"
							+ facetGroupName + "'.");

		Iterator<Path> pathIterator = traverse.iterator();
		Node facetGroupNode = null;
		while (pathIterator.hasNext()) {
			Path path = pathIterator.next();
			if (path.length() == 1)
				facetGroupNode = path.endNode();
		}
		return facetGroupNode;
	}

	/**
	 * Creates the facetGroup node with the delivered information in
	 * <tt>jsonFacetGroup</tt>, connects it with <tt>facetGroupsNode</tt> and
	 * return the created node, or returns the existing facet group node with
	 * the name found at the property <tt>FacetGroupConstants.PROP_NAME</tt> in
	 * <tt>jsonFacetGroup</tt>.
	 * 
	 * @param graphDb
	 * @param jsonFacetGroup
	 * @return The facet group node with the name found at the property
	 *         <tt>FacetGroupConstants.PROP_NAME</tt> in <tt>jsonFacetGroup</tt>
	 *         .
	 * @throws JSONException
	 */
	private static Node createFacetGroup(GraphDatabaseService graphDb, Node facetGroupsNode,
			final JSONObject jsonFacetGroup) throws JSONException {
		String facetGroupName = jsonFacetGroup.getString(FacetGroupConstants.PROP_NAME);
		Node facetGroupNode = getFacetGroup(graphDb, facetGroupsNode, facetGroupName);

		if (null == facetGroupNode) {
			log.log(Level.FINE, "Facet group \"" + facetGroupName + "\" (ID: " + facetGroupName
					+ ") does not exist and is created.");
			facetGroupNode = graphDb.createNode();
			PropertyUtilities.copyJSONObjectToPropertyContainer(jsonFacetGroup, facetGroupNode, PROP_LABELS);

			int nextSequenceValue = SequenceManager.getNextSequenceValue(graphDb, SequenceConstants.SEQ_FACET_GROUP);
			facetGroupNode.setProperty(PROP_ID, NodeIDPrefixConstants.FACET_GROUP + nextSequenceValue);
			facetGroupsNode.createRelationshipTo(facetGroupNode, EdgeTypes.HAS_FACET_GROUP);
		}
		JSONArray labels = JSON.getJSONArray(jsonFacetGroup, FacetGroupConstants.PROP_LABELS);

		if (null != labels) {
			for (int i = 0; i < labels.length(); i++) {
				String labelString = labels.getString(i);
				Label label = DynamicLabel.label(labelString);
				facetGroupNode.addLabel(label);
			}
		}

		if (null == facetGroupNode.getProperty(FacetGroupConstants.PROP_POSITION))
			throw new IllegalArgumentException("The facet group \"" + facetGroupName
					+ "\" does not have the required property \"" + FacetGroupConstants.PROP_POSITION
					+ "\". It must either be passed with the inserted facets or already exist.");

		return facetGroupNode;
	}

	public static Node getFacetGroupsNode(GraphDatabaseService graphDb) {
		Node facetGroupsNode = null;
		try (Transaction tx = graphDb.beginTx()) {
			facetGroupsNode = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, NodeConstants.Labels.ROOT,
					PROP_NAME, NAME_FACET_GROUPS);
			if (null == facetGroupsNode) {
				facetGroupsNode = graphDb.createNode(NodeConstants.Labels.ROOT);
				facetGroupsNode.setProperty(PROP_NAME, NAME_FACET_GROUPS);
			}
			tx.success();
		}
		return facetGroupsNode;
	}

	public static Node getNoFacetGroupsNode(GraphDatabaseService graphDb) {
		Node facetGroupsNode = null;
		try (Transaction tx = graphDb.beginTx()) {
			facetGroupsNode = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, NodeConstants.Labels.ROOT,
					PROP_NAME, NAME_NO_FACET_GROUPS);
			if (null == facetGroupsNode) {
				facetGroupsNode = graphDb.createNode(NodeConstants.Labels.ROOT);
				facetGroupsNode.setProperty(PROP_NAME, NAME_NO_FACET_GROUPS);
			}
			tx.success();
		}
		return facetGroupsNode;
	}

	public static Node getFacetNode(GraphDatabaseService graphDb, String facetId) {
		return NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, FacetLabel.FACET, PROP_ID, facetId);
	}
	
	public static Node getNoFacet(GraphDatabaseService graphDb, String facetId) {
		Node noFacetNode = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, FacetLabel.NO_FACET, PROP_ID,
				facetId);
		if (null == noFacetNode) {
			Node facetNode = getFacetNode(graphDb, facetId);
			noFacetNode = NodeUtilities.copyNode(graphDb, facetNode);
			noFacetNode.addLabel(FacetManager.FacetLabel.NO_FACET);

			Node noFacetGroupsNode = getNoFacetGroupsNode(graphDb);
			Node facetGroupNode = NodeUtilities.getSingleOtherNode(facetNode, EdgeTypes.HAS_FACET);
			Node noFacetGroupNode = getFacetGroup(graphDb, noFacetGroupsNode,
					(String) facetGroupNode.getProperty(PROP_NAME));
			if (null == noFacetGroupNode) {
				noFacetGroupNode = NodeUtilities.copyNode(graphDb, facetGroupNode);
				noFacetGroupsNode.createRelationshipTo(noFacetGroupNode, EdgeTypes.HAS_FACET_GROUP);
			}
			noFacetGroupNode.createRelationshipTo(noFacetNode, EdgeTypes.HAS_FACET);
		}
		return noFacetNode;
	}
}
