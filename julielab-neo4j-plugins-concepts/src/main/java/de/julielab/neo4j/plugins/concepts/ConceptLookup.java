package de.julielab.neo4j.plugins.concepts;

import de.julielab.neo4j.plugins.auxiliaries.PropertyUtilities;
import de.julielab.neo4j.plugins.auxiliaries.semedico.NodeUtilities;
import de.julielab.neo4j.plugins.auxiliaries.semedico.SequenceManager;
import de.julielab.neo4j.plugins.datarepresentation.ConceptCoordinates;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static de.julielab.neo4j.plugins.concepts.ConceptLabel.CONCEPT;
import static de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants.*;

public class ConceptLookup {
    public static final String SYSPROP_ID_CACHE_ENABLED = "de.julielab.neo4j.plugins.conceptlookup.nodeidcache.enabled";
    public static final String NAME_SOURCE_IDS_SEQUENCE = "num_sourceids_sequence";
    private final static Logger log = LoggerFactory.getLogger(ConceptLookup.class);

    /**
     * RULE: Two concepts are equal, iff they have the same original source ID
     * assigned from the same original source or both have no contradicting original
     * ID and original source but the same source ID and source. Contradicting means
     * two non-null values that are not equal.
     *
     * @param coordinates The coordinates of the concept to find.
     * @return The node corresponding to the given coordinates or null, if none is found.
     */
    public static Node lookupConcept(Transaction tx, ConceptCoordinates coordinates) {
        String orgId = coordinates.originalId;
        String orgSource = coordinates.originalSource;
        String srcId = coordinates.sourceId;
        String source = coordinates.source;
        boolean uniqueSourceId = coordinates.uniqueSourceId;
        log.trace("Looking up concept via original ID and source ({}, {}) and source ID and source ({}, {}).", orgId,
                orgSource, srcId, source);
        if ((null == orgId || null == orgSource) && (null == srcId || null == source)) {
            // no source information is complete, per definition we cannot find
            // an equal concept
            log.debug("Neither original ID and original source nor source ID and source were given, returning null.");
            return null;
        }
        Node concept = null;
        // Do we know the original ID?
        ResourceIterator<Node> concepts = null;
        if (orgId != null) {
            concepts = tx.findNodes(CONCEPT, PROP_ORG_ID, orgId);
        }
        if (concepts != null && concepts.hasNext()) {
            log.trace("Found concept by original ID {}", orgId);
            // 1. Check if there is a concept with the given original ID and a matching
            // original source.
            while (concepts.hasNext()) {
                Node foundConcept = concepts.next();
                if (!PropertyUtilities.hasSamePropertyValue(foundConcept, PROP_ORG_SRC, orgSource)) {
                    log.trace("Original source doesn't match; requested: {}, found concept has: {}", orgSource,
                            NodeUtilities.getString(foundConcept, PROP_ORG_SRC));
                } else {
                    log.trace("Found existing concept for original ID {} and original source {}", orgId, orgSource);
                    concept = foundConcept;
                    concepts.close();
                    break;
                }
            }
        }
        // 2. If we couldn't find the concept via original ID, check for a concept with the same source
        // ID and source and a non-contradicting original ID.
        if (null == concept && null != srcId) {
            concept = lookupConceptBySourceId(tx, srcId, source, uniqueSourceId);
            if (null != concept) {
                // check for an original ID contradiction
                Object existingOrgId = NodeUtilities.getNonNullNodeProperty(concept, PROP_ORG_ID);
                Object existingOrgSrc = NodeUtilities.getNonNullNodeProperty(concept, PROP_ORG_SRC);
                if (null != existingOrgId && null != existingOrgSrc && null != orgId && null != orgSource) {
                    if (!existingOrgId.equals(orgId) || !existingOrgSrc.equals(orgSource)) {
                        throw new IllegalStateException(String.format(
                                "Inconsistent data: A newly imported concept has original ID, original source (%s, %s) "
                                        + "and source ID, source (%s, %s); the latter matches the found concept with ID %s "
                                        + "but a this concept has an original ID and source (%s, %s)",
                                orgId, orgSource, srcId, source, NodeUtilities.getNonNullNodeProperty(concept, PROP_ID),
                                existingOrgId, existingOrgSrc));
                    }
                }
            }
        }
        if (null == concept)
            log.trace(
                    "    Did not find an existing concept with original ID and source ({}, {}) or source ID and source ({}, {}).",
                    orgId, orgSource, srcId, source);
        return concept;
    }

    /**
     * Returns the concept node with source ID <tt>srcId</tt> given from source
     * <tt>source</tt> or <tt>null</tt> if no such node exists.
     *
     * @param tx             The current transaction.
     * @param srcId          The source ID of the requested concept node.
     * @param source         The source in which the concept node should be given
     *                       <tt>srcId</tt> as a source ID.
     * @param uniqueSourceId Whether the ID should be unique, independently from the source.
     *                       This holds, for example, for ontology class IRIs.
     * @return The requested concept node or <tt>null</tt> if no such node is found.
     */
    public static Node lookupConceptBySourceId(Transaction tx, String srcId, String source, boolean uniqueSourceId) {
        if (srcId == null)
            throw new IllegalArgumentException("An aggregate element source ID is null.");
        log.trace("Trying to look up existing concept by source ID and source ({}, {})", srcId, source);
        List<Node> foundNodes = new ArrayList<>();
        int maxNumSourceIds = SequenceManager.getCurrentSequenceValue(tx, NAME_SOURCE_IDS_SEQUENCE);
        Node soughtConcept = null;
        for (int i = 0; i < maxNumSourceIds && soughtConcept == null; i++) {
            ResourceIterator<Node> indexHits = tx.findNodes(CONCEPT, PROP_SRC_IDS + i, srcId);
            try {
                if (!indexHits.hasNext()) {
                    log.trace("    Did not find any concept with source ID {}", srcId);
                    return null;
                }
            } catch (QueryExecutionException e) {
                log.error("Could not find index hits for sourceId {} due to error", srcId, e);
                throw e;
            }
            while (indexHits.hasNext()) {
                Node conceptNode = (Node) indexHits.next();
                foundNodes.add(conceptNode);
            }

            boolean uniqueSourceIdNodeFound = false;
            for (Node conceptNode : foundNodes) {
                if (null != conceptNode) {

                    // The rule goes as follows: Two concepts that share a source ID
                    // which is marked as being unique on both concepts are equal. If
                    // on at least one concept the source ID is not marked as
                    // unique, the concepts are different.
                    if (uniqueSourceId) {
                        boolean uniqueOnConceptNode = NodeUtilities.isSourceUnique(conceptNode, srcId, source);
                        if (uniqueOnConceptNode) {
                            if (soughtConcept == null)
                                soughtConcept = conceptNode;
                            else if (uniqueSourceIdNodeFound)
                                throw new IllegalStateException("There are multiple concept nodes with unique source ID "
                                        + srcId
                                        + ". This means that some sources define the ID as unique and others not. This can lead to an inconsistent database as happened in this case.");
                            log.trace(
                                    "    Found existing concept with unique source ID {} which matches given unique source ID",
                                    srcId);
                            uniqueSourceIdNodeFound = true;
                        }
                    }

                    String[] sources = NodeUtilities.getSourcesForSourceId(conceptNode, srcId);
                    if (Arrays.binarySearch(sources, source) < 0) {
                        log.debug("    Did not find a match for source ID " + srcId + " and source " + source);
                        conceptNode = null;
                    } else {
                        log.debug("    Found existing concept for source ID " + srcId + " and source " + source);
                    }
                    if (soughtConcept == null)
                        soughtConcept = conceptNode;
                        // if soughtConcept is not null, we already found a matching
                        // concept in the last iteration
                    else if (!uniqueSourceIdNodeFound)
                        throw new IllegalStateException(
                                "There are multiple concept nodes with source ID " + srcId + " and source " + source);
                }
            }
        }
        return soughtConcept;
    }

    public static Stream<Node> lookupConceptsBySourceId(Transaction tx, String srcId) {
        int maxNumSourceIds = SequenceManager.getCurrentSequenceValue(tx, NAME_SOURCE_IDS_SEQUENCE);
        Stream<Node> s = Stream.empty();
        for (int i = 0; i < maxNumSourceIds; i++) {
            String srcIdProp = PROP_SRC_IDS + i;
            s = Stream.concat(s, tx.findNodes(CONCEPT, srcIdProp, srcId).stream());
        }
        return s;
    }

    public static Node lookupSingleConceptBySourceId(Transaction tx, String srcId) {
        List<Node> concepts = lookupConceptsBySourceId(tx, srcId).collect(Collectors.toList());
        if (concepts.size() > 1)
            throw new IllegalStateException("Found multiple (" + concepts.size() + ") concepts with source ID '" + srcId + "'.");
        return concepts.isEmpty() ? null : concepts.get(0);
    }


    public static boolean isCorrectNode(Node n, String idProperty, String id, String idSource) {
        return isCorrectNode(n, idProperty, Set.of(id), idSource);
    }

    public static boolean isCorrectNode(Node n, String idProperty, Set<String> ids, String idSource) {
        if (idProperty.equals(PROP_ORG_ID)) {
            String originalId = (String) n.getProperty(PROP_ORG_ID);
            String originalSource = (String) n.getProperty(PROP_ORG_SRC);
            return ids.contains(originalId) && originalSource.equals(idSource);
        } else if (idProperty.equals(PROP_SRC_IDS)) {
            int i = 0;
            String sourceId = (String) n.getProperty(PROP_SRC_IDS + i);
            String[] sources = (String[]) n.getProperty(PROP_SOURCES);
            do {
                if (ids.contains(sourceId) && sources[i].equals(idSource))
                    return true;
                ++i;
                sourceId = (String) n.getProperty(PROP_SRC_IDS + i);
            } while (sourceId != null);
        } else if (idProperty.equals(PROP_ID)) {
            String nodeId = (String) n.getProperty(PROP_ID);
            return ids.contains(nodeId);
        } else
            throw new IllegalArgumentException("Unknown ID property \"" + idProperty + "\".");
        return false;
    }
}
