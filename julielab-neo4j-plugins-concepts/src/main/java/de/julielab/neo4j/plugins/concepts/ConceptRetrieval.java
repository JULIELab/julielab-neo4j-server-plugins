package de.julielab.neo4j.plugins.concepts;

import de.julielab.neo4j.plugins.FullTextIndexUtils;
import de.julielab.neo4j.plugins.auxiliaries.semedico.NodeUtilities;
import org.apache.commons.lang.StringUtils;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static de.julielab.neo4j.plugins.concepts.ConceptManager.*;
import static de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants.PROP_FACETS;
import static de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants.PROP_SRC_IDS;
import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_ID;

public class ConceptRetrieval {
    private final static Logger log = LoggerFactory.getLogger(ConceptRetrieval.class);
    public static Map<String, Object> getPathsFromFacetRoots(Transaction tx, List<String> conceptIds,  String idProperty, String returnIdProperty,boolean sort, String facetId) {
        Evaluator rootConceptEvaluator = path -> {
            Node endNode = path.endNode();

            Iterator<Relationship> iterator = endNode.getRelationships(EdgeTypes.HAS_ROOT_CONCEPT).iterator();
            if (iterator.hasNext()) {
                if (StringUtils.isBlank(facetId)) {
                    return Evaluation.INCLUDE_AND_CONTINUE;
                } else {
                    String[] facetIds = (String[]) endNode.getProperty(PROP_FACETS);
                    for (String facetIdOfRootNode : facetIds) {
                        if (facetIdOfRootNode.equals(facetId))
                            return Evaluation.INCLUDE_AND_CONTINUE;
                    }
                }
            }
            return Evaluation.EXCLUDE_AND_CONTINUE;
        };
        RelationshipType relType = StringUtils.isBlank(facetId) ? EdgeTypes.IS_BROADER_THAN
                : RelationshipType.withName(EdgeTypes.IS_BROADER_THAN.name() + "_" + facetId);
        TraversalDescription td = tx.traversalDescription().uniqueness(Uniqueness.NODE_PATH).depthFirst()
                .relationships(relType, Direction.INCOMING).evaluator(rootConceptEvaluator);

        Node[] startNodes = new Node[conceptIds.size()];
        for (int i = 0; i < conceptIds.size(); i++) {
            String conceptId = conceptIds.get(i);
            Node node = idProperty.equals(PROP_SRC_IDS) ? FullTextIndexUtils.getNode(tx, FULLTEXT_INDEX_CONCEPTS, idProperty, conceptId) : tx.findNode(ConceptLabel.CONCEPT, idProperty, conceptId);
            if (node == null)
                throw new IllegalArgumentException("Could not find a node with ID " + conceptId + " for property " + idProperty);
            startNodes[i] = node;
        }

        Traverser traverse = td.traverse(startNodes);
        List<String[]> pathsConceptIds = new ArrayList<>();
        int c = 0;
        for (Path p : traverse) {
            log.info("Path nr. " + c++ + ":" + p.toString());
            // The length of paths is measured in the number of edges, not
            // nodes, in Neo4j.
            String[] pathConceptIds = new String[p.length() + 1];
            Iterator<Node> nodesIt = p.nodes().iterator();
            boolean error = false;
            for (int i = p.length(); i >= 0; i--) {
                Node n;
                if (nodesIt.hasNext())
                    n = nodesIt.next();
                else
                    throw new IllegalStateException("Length of path wrong, more nodes expected.");
                if (!n.hasProperty(idProperty)) {
                    log.warn("Came across the concept " + n + " (" + NodeUtilities.getNodePropertiesAsString(n)
                            + ") when computing root paths. But this concept does not have an ID.");
                    error = true;
                    break;
                }
                pathConceptIds[i] = (String) n.getProperty(returnIdProperty != null ? returnIdProperty : PROP_ID);
            }
            if (!error)
                pathsConceptIds.add(pathConceptIds);
        }
        if (sort)
            pathsConceptIds.sort(Comparator.comparingInt(o -> o.length));
        Map<String, Object> pathsWrappedInMap = new HashMap<>();
        pathsWrappedInMap.put(RET_KEY_PATHS, pathsConceptIds);
        return pathsWrappedInMap;
    }
    public static Map<String, Object> getChildrenOfConcepts(Transaction tx, List<String> conceptIds, Label label) throws IOException {
        Map<String, Object> childrenByConceptId = new HashMap<>();
        for (String id : conceptIds) {
            Map<String, List<String>> reltypesByNodeId = new HashMap<>();
            Set<Node> childList = new HashSet<>();
            String conceptId = id;
            Node concept = tx.findNode(label, PROP_ID, conceptId);
            if (null != concept) {
                for (Relationship rel : concept.getRelationships(Direction.OUTGOING)) {
                    String reltype = rel.getType().name();
                    Node child = rel.getEndNode();
                    boolean isHollow = false;
                    for (Label l : child.getLabels())
                        if (l.equals(ConceptLabel.HOLLOW))
                            isHollow = true;
                    if (isHollow)
                        continue;
                    String childId = (String) child.getProperty(PROP_ID);
                    List<String> reltypeList = reltypesByNodeId.computeIfAbsent(childId, k -> new ArrayList<>());
                    reltypeList.add(reltype);
                    childList.add(child);
                }
                Map<String, Object> childrenAndReltypes = new HashMap<>();
                childrenAndReltypes.put(RET_KEY_CHILDREN, childList);
                childrenAndReltypes.put(RET_KEY_RELTYPES, reltypesByNodeId);
                childrenByConceptId.put(conceptId, childrenAndReltypes);
            }
        }
        return childrenByConceptId;
    }
}
