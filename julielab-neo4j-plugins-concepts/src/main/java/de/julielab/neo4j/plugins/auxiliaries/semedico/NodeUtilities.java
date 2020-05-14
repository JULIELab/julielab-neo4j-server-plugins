package de.julielab.neo4j.plugins.auxiliaries.semedico;

import de.julielab.neo4j.plugins.ConceptManager;
import de.julielab.neo4j.plugins.ConceptManager.EdgeTypes;
import de.julielab.neo4j.plugins.FullTextIndexUtils;
import de.julielab.neo4j.plugins.auxiliaries.PropertyUtilities;
import de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants;
import org.apache.commons.lang.StringUtils;
import org.neo4j.graphdb.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants.*;
import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_ID;
import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_NAME;

public class NodeUtilities extends de.julielab.neo4j.plugins.auxiliaries.NodeUtilities {

    public static String getNodeRelationshipsString(Node n) {
        List<String> relsStrings = new ArrayList<>();
        for (Relationship r : n.getRelationships()) {
            Node src = r.getStartNode();
            Node tgt = r.getEndNode();

            String srcLabel = getNodeIdentifier(src);
            String tgtLabel = getNodeIdentifier(tgt);
            String rType = r.getType().name();

            relsStrings.add(srcLabel + "-" + rType + "->" + tgtLabel);
        }
        return StringUtils.join(relsStrings, "\n");
    }

    public static String getNodeIdentifier(Node n) {
        String identifier = String.valueOf(n.getId());
        if (n.hasProperty(PROP_ID))
            identifier = (String) n.getProperty(PROP_ID);
        if (n.hasProperty(PROP_SRC_IDS))
            identifier = ((String[]) n.getProperty(PROP_SRC_IDS))[0];
        if (n.hasProperty(PROP_NAME))
            identifier = (String) n.getProperty(PROP_NAME);
        if (n.hasProperty(PROP_PREF_NAME))
            identifier = (String) n.getProperty(PROP_PREF_NAME);
        return "(" + identifier + ":" + StringUtils.join(n.getLabels().iterator(), ",") + ")";
    }

    public static Set<String> getSourcesForSourceId(Node conceptNode, String sourceId) {
        Set<String> sourcesForSourceId = new HashSet<>();

        String[] conceptSrcIds = (String[]) conceptNode.getProperty(ConceptConstants.PROP_SRC_IDS);
        String[] conceptSources = conceptNode.hasProperty(PROP_SOURCES)
                ? (String[]) conceptNode.getProperty(PROP_SOURCES) : new String[0];
        if (conceptSources.length > 0 && conceptSrcIds.length != conceptSources.length) {
            throw new IllegalStateException("Concept " + NodeUtilities.getNodePropertiesAsString(conceptNode)
                    + " has a differing number of source IDs and sources.");
        }
        for (int i = 0; i < conceptSources.length; ++i) {
            String source = conceptSources[i];
            String conceptSrcId = conceptSrcIds[i];
            if (source != null && conceptSrcId != null && conceptSrcId.equals(sourceId))
                sourcesForSourceId.add(source);
        }

        return sourcesForSourceId;
    }

    /**
     * Iterates over source IDs and source ID unique markers. If at least one
     * unique marker for <tt>srcId</tt> (which might occur multiple times from
     * different sources) is <tt>true</tt>, the source ID is deemed unique.
     *
     * @param conceptNode The concept node on which <tt>srcId</tt> is unique or not.
     * @param srcId       The source ID for which to determine whether it is unique for
     *                    this concept.
     * @return <tt>true</tt>, if at least one occurrence of <tt>srcId</tt> is
     * marked as unique.
     */
    public static boolean isSourceUnique(Node conceptNode, String srcId) {
        String[] conceptSrcIds = (String[]) conceptNode.getProperty(ConceptConstants.PROP_SRC_IDS);
        boolean[] conceptUniqueSrcIds = (boolean[]) conceptNode.getProperty(ConceptConstants.PROP_UNIQUE_SRC_ID);
        if (conceptSrcIds.length > 0 && conceptSrcIds.length != conceptUniqueSrcIds.length) {
            throw new IllegalStateException("Concept " + NodeUtilities.getNodePropertiesAsString(conceptNode)
                    + " has a differing number of source IDs and unique source ID markers.");
        }
        for (int i = 0; i < conceptSrcIds.length; i++) {
            if (conceptSrcIds[i].equals(srcId) && conceptUniqueSrcIds[i])
                return true;
        }
        return false;
    }

    public static Node mergeConceptNodesWithUniqueSourceId(Transaction tx, String srcId, List<Node> obsoleteNodes) {
        ResourceIterator<Object> conceptNodesIt = FullTextIndexUtils.getNodes(tx, ConceptManager.FULLTEXT_INDEX_CONCEPTS, PROP_SRC_IDS, srcId);
        Node firstNode = null;
        for (Object o : (Iterable<Object>)() -> conceptNodesIt) {
            Node conceptNode = (Node) o;
            if (firstNode == null) {
                firstNode = conceptNode;
                continue;
            }

            // ----- setting preferred name
            String firstNodePrefName = (String) firstNode.getProperty(PROP_PREF_NAME);
            String conceptPrefName = (String) conceptNode.getProperty(PROP_PREF_NAME);
            boolean addConceptPrefToSynonyms = !firstNodePrefName.equals(conceptPrefName);

            // ----- merging of general properties
            PropertyUtilities.mergeEntityIntoEntity(conceptNode, firstNode, ConceptConstants.PROP_LABELS,
                    PROP_SRC_IDS, PROP_SOURCES, PROP_SYNONYMS, RELATIONSHIPS, COORDINATES, PARENT_COORDINATES);

            // ----- merging of source IDs and sources
            // we merge the coordinates (source ID, source) that do not yet exist in first node
            String[] conceptSrcIds = (String[]) conceptNode.getProperty(PROP_SRC_IDS);
            String[] conceptSources = (String[]) conceptNode.getProperty(PROP_SOURCES);
            boolean[] conceptUniqueMarkers = (boolean[]) conceptNode.getProperty(PROP_UNIQUE_SRC_ID);
            for (int i = 0; i < conceptSrcIds.length; i++) {
                String conceptSrcId = conceptSrcIds[i];
                String conceptSource = conceptSources[i];
                boolean conceptUniqueSrcId = conceptUniqueMarkers[i];
                int idIndex = findFirstValueInArrayProperty(firstNode, PROP_SRC_IDS, conceptSrcId);
                int sourceIndex = findFirstValueInArrayProperty(firstNode, PROP_SOURCES, conceptSource);
                // (sourceID, source) coordinate has not been found, create it
                if (!StringUtils.isBlank(srcId) && ((idIndex == -1 && sourceIndex == -1) || (idIndex != sourceIndex))) {
                    addToArrayProperty(firstNode, PROP_SRC_IDS, conceptSrcId, true);
                    addToArrayProperty(firstNode, PROP_SOURCES, conceptSource, true);
                    addToArrayProperty(firstNode, PROP_UNIQUE_SRC_ID, conceptUniqueSrcId, true);
                }
            }

            // ----- merging of synonyms
            String[] conceptSynonyms = (String[]) conceptNode.getProperty(PROP_SYNONYMS);
            mergeArrayProperty(firstNode, ConceptConstants.PROP_SYNONYMS, conceptSynonyms);
            if (addConceptPrefToSynonyms)
                addToArrayProperty(firstNode, PROP_SYNONYMS, conceptPrefName);

            // ----- merging of facets
            String[] conceptFacets = (String[]) conceptNode.getProperty(PROP_FACETS);
            mergeArrayProperty(firstNode, PROP_FACETS, conceptFacets);

            // ----- merging labels
            Iterable<Label> labels = conceptNode.getLabels();
            for (Label label : labels)
                firstNode.addLabel(label);

            obsoleteNodes.add(conceptNode);
        }
        return firstNode;
    }

    /**
     * Returns nodes with the {@link de.julielab.neo4j.plugins.ConceptManager.ConceptLabel#AGGREGATE} label that have
     * the given <tt>node</tt> as an element, which is that the aggregates have a relationship of type
     * {@link EdgeTypes#HAS_ELEMENT} to the passed node.
     *
     * @param node The node whose governing aggregates should be returned.
     * @return The aggregates of which the passed node is an element.
     * @see #getElementNodes(Node)
     */
    public static Set<Node> getAggregatingNodes(Node node) {
        return StreamSupport.stream(node.getRelationships(Direction.INCOMING, EdgeTypes.HAS_ELEMENT).spliterator(), false).map(Relationship::getStartNode).collect(Collectors.toSet());
    }

    /**
     * Returns nodes that have a {@link EdgeTypes#IS_BROADER_THAN} relationship to the passed node.
     *
     * @param node A node for which the taxonomic parents are requested.
     * @return The taxonomic parents of <tt>node</tt>.
     */
    public static Set<Node> getParentNodes(Node node) {
        return StreamSupport.stream(node.getRelationships(Direction.INCOMING, EdgeTypes.IS_BROADER_THAN).spliterator(), false).map(Relationship::getStartNode).collect(Collectors.toSet());
    }

    /**
     * Returns nodes that are an element of the node <tt>aggregate</tt> which means that the aggregate has a relationship of type
     * {@link EdgeTypes#HAS_ELEMENT} to element nodes.
     *
     * @param aggregate The node with the {@link de.julielab.neo4j.plugins.ConceptManager.ConceptLabel#AGGREGATE} label whose element nodes are requested.
     * @return The element nodes of the passed aggregate.
     * @see #getAggregatingNodes(Node)
     */
    public static Set<Node> getElementNodes(Node aggregate) {
        if (!aggregate.hasLabel(ConceptManager.ConceptLabel.AGGREGATE))
            throw new IllegalArgumentException("The given node does not have the AGGREGATE label");
        return StreamSupport.stream(aggregate.getRelationships(Direction.OUTGOING, EdgeTypes.HAS_ELEMENT).spliterator(), false).map(Relationship::getEndNode).collect(Collectors.toSet());
    }
}
