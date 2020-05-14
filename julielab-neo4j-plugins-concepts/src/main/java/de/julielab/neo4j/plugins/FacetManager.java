package de.julielab.neo4j.plugins;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.julielab.neo4j.plugins.ConceptManager.ConceptLabel;
import de.julielab.neo4j.plugins.auxiliaries.NodeUtilities;
import de.julielab.neo4j.plugins.auxiliaries.PropertyUtilities;
import de.julielab.neo4j.plugins.auxiliaries.semedico.PredefinedTraversals;
import de.julielab.neo4j.plugins.auxiliaries.semedico.SequenceManager;
import de.julielab.neo4j.plugins.constants.semedico.SequenceConstants;
import de.julielab.neo4j.plugins.datarepresentation.ImportFacet;
import de.julielab.neo4j.plugins.datarepresentation.ImportFacetGroup;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetGroupConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.NodeIDPrefixConstants;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.TraversalMetadata;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.MappingRepresentation;
import org.neo4j.server.rest.repr.RecursiveMappingRepresentation;
import org.neo4j.server.rest.repr.Representation;

import javax.ws.rs.Path;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants.*;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@Path("/facet_manager")
public class FacetManager  {

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
    private final DatabaseManagementService dbms;

    public FacetManager( @Context DatabaseManagementService dbms )
    {
        this.dbms = dbms;
    }
    public static Node createFacet(GraphDatabaseService graphDb, ImportFacet jsonFacet) {
        log.info("Creating facet with the following data: " + jsonFacet);

        Collection<String> generalLabels = jsonFacet.getLabels();
        boolean isNoFacet = jsonFacet.isNoFacet();

        ImportFacetGroup jsonFacetGroup = jsonFacet.getFacetGroup();

        try (Transaction tx = graphDb.beginTx()) {
            Node facetGroupsNode;
            if (isNoFacet)
                facetGroupsNode = getNoFacetGroupsNode(graphDb);
            else
                facetGroupsNode = getFacetGroupsNode(graphDb);
            Node facetGroup = createFacetGroup(tx, facetGroupsNode, jsonFacetGroup);

            // Create the actual facet node and populate it with data.
            Node facet = tx.createNode(FacetLabel.FACET);
            //PropertyUtilities.copyObjectToPropertyContainer(jsonFacet, facet, NO_FACET, PROP_LABELS,
            //		FACET_GROUP);

            PropertyUtilities.setNonNullNodeProperty(facet, PROP_NAME, jsonFacet.getName());
            PropertyUtilities.setNonNullNodeProperty(facet, PROP_SHORT_NAME, jsonFacet.getShortName());
            PropertyUtilities.setNonNullNodeProperty(facet, PROP_CUSTOM_ID, jsonFacet.getCustomId());
            PropertyUtilities.setNonNullNodeProperty(facet, PROP_LABELS, () -> jsonFacet.getLabels().toArray(new String[0]));
            PropertyUtilities.setNonNullNodeProperty(facet, PROP_SOURCE_TYPE, jsonFacet.getSourceType());


            // If everything is alright, get an ID for the facet.
            String facetId = NodeIDPrefixConstants.FACET
                    + SequenceManager.getNextSequenceValue(tx, SequenceConstants.SEQ_FACET);
            facet.setProperty(PROP_ID, facetId);
            facetGroup.createRelationshipTo(facet, EdgeTypes.HAS_FACET);
            if (null != generalLabels) {
                for (String labelString : generalLabels) {
                    Label label = Label.label(labelString);
                    facet.addLabel(label);
                }
            }

            tx.commit();
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
            RelationshipType dynRel = RelationshipType.withName("IS_BROADER_THAN_" + fid);
            Node node = getFacetNode(graphDb, fid);

            Traverser traverser = tx.traversalDescription().breadthFirst().uniqueness(Uniqueness.NODE_GLOBAL)
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
     * @param tx The current transactino.
     * @param facetGroupsNode The global facet groups node to get the facet group from.
     * @param facetGroupName The name of the facet group to retrieve.
     * @return The facet group with the given facetGroupName.
     */
    private static Node getFacetGroup(Transaction tx, Node facetGroupsNode, String facetGroupName) {
        // Traversal to get the particular facet group node with id
        // 'facetGroupId' (and the facet groups node, in case we yet need to
        // create the facet group node).
        TraversalDescription td = PredefinedTraversals.getFacetGroupTraversal(tx, facetGroupName);
        Traverser traverse = td.traverse(facetGroupsNode);

        TraversalMetadata metadata = traverse.metadata();
        if (metadata != null && metadata.getNumberOfPathsReturned() > 1)
            throw new IllegalStateException(
                    "There is more than one path from the reference node to the facet group node with name '"
                            + facetGroupName + "'.");

        Iterator<org.neo4j.graphdb.Path> pathIterator = traverse.iterator();
        Node facetGroupNode = null;
        while (pathIterator.hasNext()) {
            org.neo4j.graphdb.Path path = pathIterator.next();
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
     * @param tx The current transaction.
     * @param jsonFacetGroup The JSON description of the facet group.
     * @return The facet group node with the name found at the property
     * <tt>FacetGroupConstants.PROP_NAME</tt> in <tt>jsonFacetGroup</tt>
     * .
     */
    private static Node createFacetGroup(Transaction tx, Node facetGroupsNode,
                                         final ImportFacetGroup jsonFacetGroup) {
        String facetGroupName = jsonFacetGroup.name;
        Node facetGroupNode = getFacetGroup(tx, facetGroupsNode, facetGroupName);

        if (null == facetGroupNode) {
            log.log(Level.FINE, "Facet group \"" + facetGroupName + "\" (ID: " + facetGroupName
                    + ") does not exist and is created.");
            facetGroupNode = tx.createNode();
            PropertyUtilities.copyObjectToEntity(jsonFacetGroup, facetGroupNode, PROP_LABELS);

            int nextSequenceValue = SequenceManager.getNextSequenceValue(tx, SequenceConstants.SEQ_FACET_GROUP);
            facetGroupNode.setProperty(PROP_ID, NodeIDPrefixConstants.FACET_GROUP + nextSequenceValue);
            facetGroupsNode.createRelationshipTo(facetGroupNode, EdgeTypes.HAS_FACET_GROUP);
        }
        List<String> labels = jsonFacetGroup.labels;

        if (null != labels) {
            for (String labelString : labels) {
                Label label = Label.label(labelString);
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
                facetGroupsNode = tx.createNode(NodeConstants.Labels.ROOT);
                facetGroupsNode.setProperty(PROP_NAME, NAME_FACET_GROUPS);
            }
            tx.commit();
        }
        return facetGroupsNode;
    }

    public static Node getNoFacetGroupsNode(GraphDatabaseService graphDb) {
        Node facetGroupsNode;
        try (Transaction tx = graphDb.beginTx()) {
            facetGroupsNode = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, NodeConstants.Labels.ROOT,
                    PROP_NAME, NAME_NO_FACET_GROUPS);
            if (null == facetGroupsNode) {
                facetGroupsNode = tx.createNode(NodeConstants.Labels.ROOT);
                facetGroupsNode.setProperty(PROP_NAME, NAME_NO_FACET_GROUPS);
            }
            tx.commit();
        }
        return facetGroupsNode;
    }

    public static Node getFacetNode(GraphDatabaseService graphDb, String facetId) {
        return NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, FacetLabel.FACET, PROP_ID, facetId);
    }

    public static Node getNoFacet(GraphDatabaseService graphDb, Transaction tx, String facetId) {
        Node noFacetNode = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, FacetLabel.NO_FACET, PROP_ID,
                facetId);
        if (null == noFacetNode) {
            Node facetNode = getFacetNode(graphDb, facetId);
            noFacetNode = NodeUtilities.copyNode(graphDb, facetNode);
            noFacetNode.addLabel(FacetManager.FacetLabel.NO_FACET);

            Node noFacetGroupsNode = getNoFacetGroupsNode(graphDb);
            Node facetGroupNode = NodeUtilities.getSingleOtherNode(facetNode, EdgeTypes.HAS_FACET);
            Node noFacetGroupNode = getFacetGroup(tx, noFacetGroupsNode,
                    (String) facetGroupNode.getProperty(PROP_NAME));
            if (null == noFacetGroupNode) {
                noFacetGroupNode = NodeUtilities.copyNode(graphDb, facetGroupNode);
                noFacetGroupsNode.createRelationshipTo(noFacetGroupNode, EdgeTypes.HAS_FACET_GROUP);
            }
            noFacetGroupNode.createRelationshipTo(noFacetNode, EdgeTypes.HAS_FACET);
        }
        return noFacetNode;
    }

    @GET
    @Produces( MediaType.TEXT_PLAIN )
    @Path( "/{"+GET_FACET_SIZE+"}" )
    public int getFacetSize(@PathParam(KEY_ID) String fid) {
        return countFacetChildren(dbms.database(DEFAULT_DATABASE_NAME), fid);
    }

    @POST
    @Consumes( MediaType.APPLICATION_JSON )
    @Produces( MediaType.APPLICATION_JSON )
    @Path( "/{"+INSERT_FACETS+"}" )
    public ListRepresentation insertFacets(String facetList) throws IOException {
        final ObjectMapper om = new ObjectMapper();
        List<ImportFacet> input = om.readValue(facetList, new TypeReference<List<ImportFacet>>() {
        });
        List<Node> facets = new ArrayList<>();
        GraphDatabaseService graphDb = dbms.database(DEFAULT_DATABASE_NAME);
        for (ImportFacet jsonFacet : input) {
            Node facet = createFacet(graphDb, jsonFacet);
            facets.add(facet);
        }

        try (Transaction tx = graphDb.beginTx()) {
            // The response is a list - according to the input order - where for
            // each facet is shown its name and which ID it received.
            List<Representation> facetRepList = new ArrayList<>();
            for (Node facet : facets) {
                Map<String, Object> map = new HashMap<>();
                map.put(PROP_NAME, facet.getProperty(PROP_NAME));
                map.put(PROP_ID, facet.getProperty(PROP_ID));
                RecursiveMappingRepresentation facetResponseRep = new RecursiveMappingRepresentation(Representation.MAP,
                        map);
                facetRepList.add(facetResponseRep);
            }
            ListRepresentation listRep = new ListRepresentation(Representation.MAP, facetRepList);
            tx.commit();
            return listRep;
        }

    }

    @SuppressWarnings("unchecked")
    @GET
    @Produces( MediaType.APPLICATION_JSON )
    @Path( "/{"+GET_FACETS+"}" )
    public MappingRepresentation getFacets(@PathParam(PARAM_RETURN_HOLLOW_FACETS) Boolean returnHollowfacets) {
        RecursiveMappingRepresentation facetGroupsRep;
        GraphDatabaseService graphDb = dbms.database(DEFAULT_DATABASE_NAME);
        try (Transaction tx = graphDb.beginTx()) {
            Node facetGroupsNode = getFacetGroupsNode(graphDb);
            TraversalDescription td = PredefinedTraversals.getFacetTraversal(tx);
            Traverser traverse = td.traverse(facetGroupsNode);

            Map<String, Object> facetsByFacetGroupName = new HashMap<>();
            Map<String, Node> facetGroupsMap = new HashMap<>();
            List<Map<String, Object>> facetGroupsWithFacetsList = new ArrayList<>();

            // First build intermediate maps where the facet group nodes and
            // facets
            // are
            // organized by facet group name.
            for (org.neo4j.graphdb.Path facetPath : traverse) {
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
                        facets = new ArrayList<>();
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

                Map<String, Object> facetGroupMap = new HashMap<>();
                for (String propKey : facetGroupNode.getPropertyKeys())
                    facetGroupMap.put(propKey, facetGroupNode.getProperty(propKey));
                List<String> facetGroupLabels = new ArrayList<>();
                for (Label label : facetGroupNode.getLabels())
                    facetGroupLabels.add(label.name());
                facetGroupMap.put(FacetGroupConstants.KEY_LABELS, facetGroupLabels);
                facetGroupMap.put(KEY_FACETS, facets);

                facetGroupsWithFacetsList.add(facetGroupMap);
            }
            Map<String, Object> ret = new HashMap<>();
            ret.put("facetGroups", facetGroupsWithFacetsList);
            facetGroupsRep = new RecursiveMappingRepresentation(Representation.MAP, ret);
            tx.commit();
        }

        return facetGroupsRep;
    }

    public static enum EdgeTypes implements RelationshipType {
        HAS_FACET_GROUP, HAS_FACET
    }

    public static enum FacetLabel implements Label {
        FACET, NO_FACET
    }
}
