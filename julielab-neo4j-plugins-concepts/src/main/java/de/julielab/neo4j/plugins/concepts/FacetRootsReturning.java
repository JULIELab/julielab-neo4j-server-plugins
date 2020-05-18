package de.julielab.neo4j.plugins.concepts;

import de.julielab.neo4j.plugins.FacetManager;
import de.julielab.neo4j.plugins.auxiliaries.semedico.PredefinedTraversals;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.UriInfo;
import java.util.*;

import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_ID;

public class FacetRootsReturning {
    private final static Logger log = LoggerFactory.getLogger(FacetRootsReturning.class);
    /**
     * Returns the facet roots of the requested facets, optionally filtered by the given IDs per facet.
     *
     * @param requestedFacetId    A set of facet IDs for which to return their root concepts.
     * @param requestedConceptIds Optional. Groups concept root IDs to the facet they should be returned for.
     * @return The facet roots grouped by facet ID.
     * @see ConceptManager#getFacetRoots(UriInfo)
     */
    public static Map<String, List<Node>> getFacetRoots(Transaction tx, Set<String> requestedFacetId, Map<String, Set<String>> requestedConceptIds, int maxRoots) {

        Map<String, List<Node>> facetRoots = new HashMap<>();


        log.info("Returning roots for facets " + requestedFacetId);
        Node facetGroupsNode = FacetManager.getFacetGroupsNode(tx);
        TraversalDescription facetTraversal = PredefinedTraversals.getFacetTraversal(tx, null, null);
        Traverser traverse = facetTraversal.traverse(facetGroupsNode);
        for (Path path : traverse) {
            Node facetNode = path.endNode();
            String facetId = (String) facetNode.getProperty(FacetConstants.PROP_ID);
            if (maxRoots > 0 && facetNode.hasProperty(FacetConstants.PROP_NUM_ROOT_TERMS)
                    && (long) facetNode.getProperty(FacetConstants.PROP_NUM_ROOT_TERMS) > maxRoots) {
                log.info("Skipping facet with ID {} because it has more than {} root concepts ({}).", facetId,
                        maxRoots, facetNode.getProperty(FacetConstants.PROP_NUM_ROOT_TERMS));
            }
            Set<String> requestedIdSet = null;
            if (null != requestedConceptIds)
                requestedIdSet = requestedConceptIds.get(facetId);
            if (requestedFacetId.contains(facetId)) {
                List<Node> roots = new ArrayList<>();
                Iterable<Relationship> relationships = facetNode.getRelationships(Direction.OUTGOING,
                        EdgeTypes.HAS_ROOT_CONCEPT);
                for (Relationship rel : relationships) {
                    Node rootConcept = rel.getEndNode();
                    boolean include = true;
                    if (null != requestedIdSet) {
                        String rootId = (String) rootConcept.getProperty(PROP_ID);
                        if (!requestedIdSet.contains(rootId))
                            include = false;
                    }
                    if (include)
                        roots.add(rootConcept);
                }
                if (!roots.isEmpty() && (maxRoots <= 0 || roots.size() <= maxRoots))
                    facetRoots.put(facetId, roots);
                else
                    log.info("Skipping facet with ID " + facetId + " because it has more than " + maxRoots
                            + " root concepts (" + roots.size() + ").");
            }
        }

        return facetRoots;
    }
}
