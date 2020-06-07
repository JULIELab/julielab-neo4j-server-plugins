package de.julielab.neo4j.plugins.auxiliaries.semedico;

import de.julielab.neo4j.plugins.FullTextIndexUtils;
import de.julielab.neo4j.plugins.auxiliaries.PropertyUtilities;
import de.julielab.neo4j.plugins.concepts.ConceptEdgeTypes;
import de.julielab.neo4j.plugins.concepts.ConceptLabel;
import de.julielab.neo4j.plugins.concepts.ConceptManager;
import de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants;
import org.apache.commons.lang.StringUtils;
import org.neo4j.graphdb.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static de.julielab.neo4j.plugins.concepts.ConceptLookup.NAME_SOURCE_IDS_SEQUENCE;
import static de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants.*;

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

    public static String[] getSourcesForSourceId(Node conceptNode, String sourceId) {
        int index = getSourceIdIndex(conceptNode, sourceId);
        if (index >= 0) {
            return (String[]) conceptNode.getProperty(PROP_SOURCES + index);
        }
        return new String[0];
    }

    public static String[] getUniqueSourcesForSourceId(Node conceptNode, String sourceId) {
        int index = getSourceIdIndex(conceptNode, sourceId);
        if (index >= 0) {
            return (String[]) conceptNode.getProperty(PROP_UNIQUE_SRC_ID + index);
        }
        return new String[0];
    }

    public static int getSourceIdIndex(Node concept, String sourceId) {
        int i = 0;
        String sourceIdProperty = PROP_SRC_IDS + i;
        while (concept.hasProperty(sourceIdProperty)) {
            String currentSourceId = (String) concept.getProperty(sourceIdProperty);
            if (currentSourceId.equals(sourceId))
                return i;
            sourceIdProperty = PROP_SRC_IDS + ++i;
        }
        return -1;
    }

    public static String[] getSourceIdArray(Node concept) {
        return getSourceIds(concept).toArray(String[]::new);
    }

    public static List<String> getSourceIds(Node concept) {
        int i = 0;
        String sourceIdProperty = PROP_SRC_IDS + i;
        List<String> sourceIds = new ArrayList<>();
        while (concept.hasProperty(sourceIdProperty)) {
            sourceIds.add((String) concept.getProperty(sourceIdProperty));
            sourceIdProperty = PROP_SRC_IDS + ++i;
        }
        return sourceIds;
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
        String[] conceptSrcIds = ((String) conceptNode.getProperty(ConceptConstants.PROP_SRC_IDS)).split("\\s+");
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
        for (Object o : (Iterable<Object>) () -> conceptNodesIt) {
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
     * Returns nodes with the {@link ConceptLabel#AGGREGATE} label that have
     * the given <tt>node</tt> as an element, which is that the aggregates have a relationship of type
     * {@link ConceptEdgeTypes#HAS_ELEMENT} to the passed node.
     *
     * @param node The node whose governing aggregates should be returned.
     * @return The aggregates of which the passed node is an element.
     * @see #getElementNodes(Node)
     */
    public static Set<Node> getAggregatingNodes(Node node) {
        return StreamSupport.stream(node.getRelationships(Direction.INCOMING, ConceptEdgeTypes.HAS_ELEMENT).spliterator(), false).map(Relationship::getStartNode).collect(Collectors.toSet());
    }

    /**
     * Returns nodes that have a {@link ConceptEdgeTypes#IS_BROADER_THAN} relationship to the passed node.
     *
     * @param node A node for which the taxonomic parents are requested.
     * @return The taxonomic parents of <tt>node</tt>.
     */
    public static Set<Node> getParentNodes(Node node) {
        return StreamSupport.stream(node.getRelationships(Direction.INCOMING, ConceptEdgeTypes.IS_BROADER_THAN).spliterator(), false).map(Relationship::getStartNode).collect(Collectors.toSet());
    }

    /**
     * Returns nodes that are an element of the node <tt>aggregate</tt> which means that the aggregate has a relationship of type
     * {@link ConceptEdgeTypes#HAS_ELEMENT} to element nodes.
     *
     * @param aggregate The node with the {@link ConceptLabel#AGGREGATE} label whose element nodes are requested.
     * @return The element nodes of the passed aggregate.
     * @see #getAggregatingNodes(Node)
     */
    public static Set<Node> getElementNodes(Node aggregate) {
        if (!aggregate.hasLabel(ConceptLabel.AGGREGATE))
            throw new IllegalArgumentException("The given node does not have the AGGREGATE label");
        return StreamSupport.stream(aggregate.getRelationships(Direction.OUTGOING, ConceptEdgeTypes.HAS_ELEMENT).spliterator(), false).map(Relationship::getEndNode).collect(Collectors.toSet());
    }

    public static void mergeSourceId(Transaction tx, Node concept, String srcId, String source, boolean uniqueSourceId) {
        int sourceIdIndex = NodeUtilities.getSourceIdIndex(concept, srcId);
        if (sourceIdIndex >= 0)  {
            String sourceProp = PROP_SOURCES + sourceIdIndex;
            String uniqueSourceProp = PROP_UNIQUE_SRC_ID + sourceIdIndex;
            String[] presentSources = (String[]) concept.getProperty(sourceProp);
            int index = Arrays.binarySearch(presentSources, source);
            if (index < 0) {
                int insertionPoint = -1 * (index + 1);
                String[] newSources = new String[presentSources.length + 1];
                newSources[insertionPoint] = source;
                System.arraycopy(presentSources, 0, newSources, 0, insertionPoint);
                System.arraycopy(presentSources, insertionPoint, newSources, insertionPoint+1, presentSources.length-insertionPoint);
                concept.setProperty(sourceProp, newSources);

                boolean[] presentUniqueSources = (boolean[]) concept.getProperty(uniqueSourceProp);
                boolean[] newUniqueSources = new boolean[presentSources.length + 1];
                newUniqueSources[insertionPoint] = uniqueSourceId;
                System.arraycopy(presentUniqueSources, 0, newUniqueSources, 0, insertionPoint);
                System.arraycopy(presentUniqueSources, insertionPoint, newUniqueSources, insertionPoint+1, presentUniqueSources.length-insertionPoint);
                concept.setProperty(uniqueSourceProp, newUniqueSources);
            }
        } else {
            // New source ID for this concept
            int sourcePropNum = SequenceManager.getNextSequenceValue(tx, NAME_SOURCE_IDS_SEQUENCE);
            String sourceIdProperty = PROP_SRC_IDS + sourcePropNum;
            String sourceProperty = PROP_SOURCES + sourcePropNum;
            String uniqueSourceProperty = PROP_UNIQUE_SRC_ID + sourcePropNum;
            concept.setProperty(sourceIdProperty, srcId);
            concept.setProperty(sourceProperty, new String[] {source});
            concept.setProperty(uniqueSourceProperty, new boolean[]{uniqueSourceId});
        }
    }
}
