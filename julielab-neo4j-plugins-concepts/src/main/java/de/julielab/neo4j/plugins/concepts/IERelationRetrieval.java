package de.julielab.neo4j.plugins.concepts;

import de.julielab.neo4j.plugins.constants.semedico.SemanticRelationConstants;
import de.julielab.neo4j.plugins.datarepresentation.ConceptCoordinates;
import de.julielab.neo4j.plugins.datarepresentation.CoordinateType;
import de.julielab.neo4j.plugins.datarepresentation.RelationIdList;
import de.julielab.neo4j.plugins.datarepresentation.RelationRetrievalRequest;
import de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class IERelationRetrieval {

    private final static Label LABEL_GENEGROUP = Label.label("AGGREGATE_GENEGROUP");
    private final static Label LABEL_TOP_ORTHOLOGY = Label.label("AGGREGATE_TOP_ORTHOLOGY");

    public static List<String> retrieve(RelationRetrievalRequest retrievalRequest, GraphDatabaseService dbms, Log log) {
        try (Transaction tx = dbms.beginTx()) {
            RelationIdList aListRequest = retrievalRequest.getAlist();
            RelationshipType[] relationTypes = retrievalRequest.getRelationTypes().stream().map(RelationshipType::withName).toArray(RelationshipType[]::new);
            if (retrievalRequest.getBlist() == null || retrievalRequest.getBlist().getIds().isEmpty())
                return serveOneSidedRequest(tx, getNodes(tx, aListRequest), relationTypes);
            else
                return serveTwoSidedRequest(tx, retrievalRequest.getAlist(), retrievalRequest.getBlist(), relationTypes);
        }
    }

    private static List<String> serveTwoSidedRequest(Transaction tx, RelationIdList aList, RelationIdList bList, RelationshipType[] relationTypes) {
        boolean aIsLarger = aList.getIds().size() > bList.getIds().size();
        // We iterate over the smaller set of IDs and search their relationships for connections to nodes from the larger set.
        List<Node> smallerList = getNodes(tx, aIsLarger ? bList : aList);
        Set<String> largeListIds = new HashSet<>(aIsLarger ? aList.getIds() : bList.getIds());
        String largerListIdProperty = aIsLarger ? aList.getIdProperty() : bList.getIdProperty();
        String largerListIdSource = aIsLarger ? aList.getIdSource() : bList.getIdSource();
        List<String> results = new ArrayList<>();
        for (Node a : smallerList) {
            Node orthologyAggregate = findHighestOrthologyAggregate(a);
            List<Node> elementNodes = ConceptAggregateManager.getNonAggregateElements(orthologyAggregate);
            for (Node element : elementNodes) {
                Iterable<Relationship> relationships = element.getRelationships(relationTypes);
                for (Relationship r : relationships) {
                    Node otherNode = r.getOtherNode(element);
                    // Check if this node is identified in the large ID list; then it is a sought relation partner
                    // and this is a relation of interest.
                    if (!ConceptLookup.isCorrectNode(otherNode, largerListIdProperty, largeListIds, largerListIdSource))
                        continue;
                    String arg1Name = (String) element.getProperty(ConceptConstants.PROP_PREF_NAME);
                    String arg1Id = (String) element.getProperty(ConceptConstants.PROP_ORG_ID);

                    String arg2Name = (String) otherNode.getProperty(ConceptConstants.PROP_PREF_NAME);
                    String arg2Id = (String) otherNode.getProperty(ConceptConstants.PROP_ORG_ID);

                    int count = (int) r.getProperty(SemanticRelationConstants.PROP_TOTAL_COUNT);

                    String resultLine = aIsLarger ? makeResultLine(arg2Name, arg2Id, arg1Name, arg1Id, count) : makeResultLine(arg1Name, arg1Id, arg2Name, arg2Id, count);
                    results.add(resultLine);
                }
            }
        }
        return results;
    }

    private static List<String> serveOneSidedRequest(Transaction tx, List<Node> aList, RelationshipType[] relationTypes) {
        List<String> results = new ArrayList<>();
        for (Node a : aList) {
            Node orthologyAggregate = findHighestOrthologyAggregate(a);
            List<Node> elementNodes = ConceptAggregateManager.getNonAggregateElements(orthologyAggregate);
            for (Node element : elementNodes) {
                Iterable<Relationship> relationships = element.getRelationships(relationTypes);
                for (Relationship r : relationships) {
                    Node otherNode = r.getOtherNode(element);
                    String arg1Name = (String) element.getProperty(ConceptConstants.PROP_PREF_NAME);
                    String arg1Id = (String) element.getProperty(ConceptConstants.PROP_ORG_ID);

                    String arg2Name = (String) otherNode.getProperty(ConceptConstants.PROP_PREF_NAME);
                    String arg2Id = (String) otherNode.getProperty(ConceptConstants.PROP_ORG_ID);

                    int count = (int) r.getProperty(SemanticRelationConstants.PROP_TOTAL_COUNT);

                    results.add(makeResultLine(arg1Name, arg1Id, arg2Name, arg2Id, count));
                }
            }
        }
        return results;
    }

    private static String makeResultLine(String arg1Name, String arg1Id, String arg2Name, String arg2Id, int count) {
        // TODO make map
        return String.format("{\"arg1Name\":\"%s\",\"arg2Name\":\"%s\",\"arg1Id\":\"%s\",\"arg2Id\":\"%s\",\"count\":%d}", arg1Name, arg2Name, arg1Id, arg2Id, count);
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
            ConceptCoordinates coordinates = new ConceptCoordinates(id, idListRequest.getIdSource(), idListRequest.getIdSource().equals(ConceptConstants.PROP_ORG_ID) ? CoordinateType.OSRC : CoordinateType.SRC);
            Node node = ConceptLookup.lookupConcept(tx, coordinates);
            if (node != null)
                nodeListRequest.add(node);
        }
        return nodeListRequest;
    }
}
