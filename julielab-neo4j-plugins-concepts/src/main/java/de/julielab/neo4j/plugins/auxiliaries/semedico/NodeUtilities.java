package de.julielab.neo4j.plugins.auxiliaries.semedico;

import static de.julielab.neo4j.plugins.constants.semedico.ConceptConstants.PARENT_COORDINATES;
import static de.julielab.neo4j.plugins.constants.semedico.ConceptConstants.PROP_COORDINATES;
import static de.julielab.neo4j.plugins.constants.semedico.ConceptConstants.PROP_FACETS;
import static de.julielab.neo4j.plugins.constants.semedico.ConceptConstants.PROP_PREF_NAME;
import static de.julielab.neo4j.plugins.constants.semedico.ConceptConstants.PROP_SOURCES;
import static de.julielab.neo4j.plugins.constants.semedico.ConceptConstants.PROP_SRC_IDS;
import static de.julielab.neo4j.plugins.constants.semedico.ConceptConstants.PROP_SYNONYMS;
import static de.julielab.neo4j.plugins.constants.semedico.ConceptConstants.PROP_UNIQUE_SRC_ID;
import static de.julielab.neo4j.plugins.constants.semedico.ConceptConstants.RELATIONSHIPS;
import static de.julielab.neo4j.plugins.constants.semedico.NodeConstants.PROP_ID;
import static de.julielab.neo4j.plugins.constants.semedico.NodeConstants.PROP_NAME;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;

import de.julielab.neo4j.plugins.auxiliaries.PropertyUtilities;
import de.julielab.neo4j.plugins.constants.semedico.ConceptConstants;

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
	 * @param conceptNode
	 *            The concept node on which <tt>srcId</tt> is unique or not.
	 * @param srcId
	 *            The source ID for which to determine whether it is unique for
	 *            this concept.
	 * @return <tt>true</tt>, if at least one occurrence of <tt>srcId</tt> is
	 *         marked as unique.
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

	public static Node mergeConceptNodesWithUniqueSourceId(String srcId, Index<Node> termIndex, List<Node> obsoleteNodes) {
		IndexHits<Node> conceptNodes = termIndex.get(PROP_SRC_IDS, srcId);
		Node firstNode = null;
		for (Node conceptNode : conceptNodes) {
			if (firstNode == null) {
				firstNode = conceptNode;
				continue;
			}
			
			// ----- setting preferred name
			String firstNodePrefName = (String) firstNode.getProperty(PROP_PREF_NAME);
			String conceptPrefName = (String) conceptNode.getProperty(PROP_PREF_NAME);
			boolean addConceptPrefToSynonyms = !firstNodePrefName.equals(conceptPrefName);
			
			// ----- merging of general properties
			PropertyUtilities.mergePropertyContainerIntoPropertyContainer(conceptNode, firstNode, ConceptConstants.PROP_GENERAL_LABELS,
					PROP_SRC_IDS, PROP_SOURCES, PROP_SYNONYMS, RELATIONSHIPS, PROP_COORDINATES, PARENT_COORDINATES);
			
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

}
