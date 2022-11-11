package de.julielab.neo4j.plugins.concepts;

import de.julielab.neo4j.plugins.constants.semedico.SemanticRelationConstants;
import de.julielab.neo4j.plugins.datarepresentation.RelationIdList;
import de.julielab.neo4j.plugins.datarepresentation.RelationRetrievalRequest;
import de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class IERelationRetrieval {

    private final static Label LABEL_GENEGROUP = Label.label("AGGREGATE_GENEGROUP");
    private final static Label LABEL_TOP_ORTHOLOGY = Label.label("AGGREGATE_TOP_ORTHOLOGY");

    public static List<Map<String, Object>> retrieve(RelationRetrievalRequest retrievalRequest, GraphDatabaseService dbms, Log log) {
        try (Transaction tx = dbms.beginTx()) {
            RelationshipType[] relationTypes = retrievalRequest.getRelationTypes().stream().map(RelationshipType::withName).toArray(RelationshipType[]::new);
            if (retrievalRequest.getBlist() == null || retrievalRequest.getBlist().getIds().isEmpty())
                return serveOneSidedRequest(tx, retrievalRequest.getAlist(), relationTypes, retrievalRequest.isInterInputRelationRetrievalEnabled());
            else
                return serveTwoSidedRequest(tx, retrievalRequest.getAlist(), retrievalRequest.getBlist(), relationTypes);
        }
    }

    private static List<Map<String, Object>> serveTwoSidedRequest(Transaction tx, RelationIdList aList, RelationIdList bList, RelationshipType[] relationTypes) {
        boolean aIsLarger = aList.getIds().size() > bList.getIds().size();
        // We iterate over the smaller set of IDs and search their relationships for connections to nodes from the larger set.
        List<Node> smallerList = getNodes(tx, aIsLarger ? bList : aList);
        String smallerListIdProperty = getIdEffectiveIdProperty(aIsLarger ? bList : aList);
        String largerListIdProperty = getIdEffectiveIdProperty(aIsLarger ? aList : bList);
        Set<Node> largerAggs = getNodes(tx, aIsLarger ? aList : bList).stream().map(IERelationRetrieval::findHighestOrthologyAggregate).collect(Collectors.toSet());
        List<Map<String, Object>> results = new ArrayList<>();
        Map<Pair<String, String>, Map<String, Object>> accumulator = new HashMap<>();
        for (Node a : smallerList) {
            Node orthologyAggregate = findHighestOrthologyAggregate(a);
            List<Node> elementNodes = ConceptAggregateManager.getNonAggregateElements(orthologyAggregate);
            for (Node element : elementNodes) {
                Iterable<Relationship> relationships = element.getRelationships(relationTypes);
                for (Relationship r : relationships) {
                    Node otherNode = r.getOtherNode(element);

                    Node otherOrthologyAggregate = findHighestOrthologyAggregate(otherNode);
                    // If the aggregate of the otherNode is not in the map, it is also not in the target list.
                    if (!largerAggs.contains(otherOrthologyAggregate))
                        continue;
                    String smallListNodeName = (String) orthologyAggregate.getProperty(ConceptConstants.PROP_PREF_NAME);
                    String smallListNodeId = (String) orthologyAggregate.getProperty(smallerListIdProperty);

                    String largeListNodeName = (String) otherOrthologyAggregate.getProperty(ConceptConstants.PROP_PREF_NAME);
                    String largeListNodeId = (String) otherOrthologyAggregate.getProperty(largerListIdProperty);

                    String arg1Name = aIsLarger ? largeListNodeName : smallListNodeName;
                    String arg1Id = aIsLarger ? largeListNodeId : smallListNodeId;
                    String arg2Name = aIsLarger ? smallListNodeName : largeListNodeName;
                    String arg2Id = aIsLarger ? smallListNodeId : largeListNodeId;

                    int count = (int) r.getProperty(SemanticRelationConstants.PROP_TOTAL_COUNT);

                    accumulator.merge(new ImmutablePair<>(arg1Id, arg2Id), makeResultLine(arg1Name, arg1Id, arg2Name, arg2Id, count), (m1, m2) -> {
                        m1.put("count", (int) m1.get("count") + (int) m2.get("count"));
                        return m1;
                    });
                }
            }
        }
        return accumulator.values().stream().collect(Collectors.toList());
    }

    private static List<Map<String, Object>> serveOneSidedRequest(Transaction tx, RelationIdList aList, RelationshipType[] relationTypes, boolean interInputRelationRetrievalEnabled) {
        // If the inter input relation retrieval is not enabled, we need the IDs as a set to quickly sort out
        // undesired relations.
        List<Node> aNodes = getNodes(tx, aList);
        Map<Node, Node> el2agg = aNodes.stream().collect(Collectors.toMap(Function.identity(), IERelationRetrieval::findHighestOrthologyAggregate));
        // We must map the input IDs to their highest aggregate because this is the level we eventually work on
        Set<String> requestedAggregateIds = !interInputRelationRetrievalEnabled ? el2agg.values().stream().map(n -> n.getProperty(getIdEffectiveIdProperty(aList))).map(String.class::cast).collect(Collectors.toSet()) : null;
        Map<Pair<String, String>, Map<String, Object>> accumulator = new HashMap<>();
        String effectiveIdProperty = getIdEffectiveIdProperty(aList);
        for (Node a : aNodes) {
            Node orthologyAggregate = el2agg.get(a);
            List<Node> elementNodes = ConceptAggregateManager.getNonAggregateElements(orthologyAggregate);
            for (Node element : elementNodes) {
                Iterable<Relationship> relationships = element.getRelationships(relationTypes);
                for (Relationship r : relationships) {
                    Node otherNode = r.getOtherNode(element);
                    String arg1Name = (String) orthologyAggregate.getProperty(ConceptConstants.PROP_PREF_NAME);
                    String arg1Id = (String) orthologyAggregate.getProperty(effectiveIdProperty);

                    Node otherOrthologyAggregate = findHighestOrthologyAggregate(otherNode);
                    String arg2Name = (String) otherOrthologyAggregate.getProperty(ConceptConstants.PROP_PREF_NAME);
                    String arg2Id = (String) otherOrthologyAggregate.getProperty(effectiveIdProperty);
                    // skip this relation if the end node is also an input node
                    if (!interInputRelationRetrievalEnabled && requestedAggregateIds.contains(arg2Id))
                        continue;

                    int count = (int) r.getProperty(SemanticRelationConstants.PROP_TOTAL_COUNT);

                    accumulator.merge(new ImmutablePair<>(arg1Id, arg2Id), makeResultLine(arg1Name, arg1Id, arg2Name, arg2Id, count), (m1, m2) -> {
                        m1.put("count", (int) m1.get("count") + (int) m2.get("count"));
                        return m1;
                    });
                }
            }
        }
        return accumulator.values().stream().collect(Collectors.toList());
    }

    private static Map<String, Object> makeResultLine(String arg1Name, String arg1Id, String arg2Name, String arg2Id, int count) {
        Map<String, Object> map = new HashMap<>();
        map.put("arg1Name", arg1Name);
        map.put("arg2Name", arg2Name);
        map.put("arg1Id", arg1Id);
        map.put("arg2Id", arg2Id);
        map.put("count", count);
        return map;
    }


    /**
     * Follows the HAS_ELEMENT relationships to aggregate nodes that have one of the labels {@link #LABEL_GENEGROUP} or
     * {@link #LABEL_TOP_ORTHOLOGY}. If such an aggregate is found, the algorithm is recursively called for that
     * node. The algorithm stops when there is no governing node with either of those labels and returns the highest
     * node found.
     *
     * @param n The start node for which we want the highest aggregation node.
     * @return The highest aggregation node for <tt>n</tt>.
     */
    private static Node findHighestOrthologyAggregate(Node n) {
        Node currentNode = n;
        Iterable<Relationship> hasElementRelationships = currentNode.getRelationships(Direction.INCOMING, ConceptEdgeTypes.HAS_ELEMENT);
        for (Relationship hasElement : hasElementRelationships) {
            Node aggregateNode = hasElement.getStartNode();
            if (aggregateNode.hasLabel(LABEL_GENEGROUP) || aggregateNode.hasLabel(LABEL_TOP_ORTHOLOGY))
                return findHighestOrthologyAggregate(aggregateNode);
        }
        // If we got here we didn't find a governing aggregate. Thus, this node is already the highest orthology
        // aggregation node. Note that this might well be a concrete gene node without any orthology aggregations
        // since not all genes take part of in the gene_orthology file.
        return n;
    }

    private static List<Node> getNodes(Transaction tx, RelationIdList idListRequest) {
        List<Node> nodeListRequest = new ArrayList<>();
        for (String id : idListRequest.getIds()) {
            Node node = tx.findNode(ConceptLabel.CONCEPT, getIdEffectiveIdProperty(idListRequest), id);
            if (node != null)
                nodeListRequest.add(node);
        }
        return nodeListRequest;
    }

    /**
     * Maps 'sourceIds' to 'sourceIds0' since we create a new source ID property for each new source ID for indexing
     * reasons (there is no indexing for array-valued properties).
     * @param idListRequest
     * @return
     */
    private static String getIdEffectiveIdProperty(RelationIdList idListRequest) {
        return idListRequest.getIdProperty().equals(ConceptConstants.PROP_SRC_IDS) ? ConceptConstants.PROP_SRC_IDS+"0" : idListRequest.getIdProperty();
    }
}
