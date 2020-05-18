package de.julielab.neo4j.plugins.concepts;

import com.google.gson.stream.JsonReader;
import de.julielab.neo4j.plugins.auxiliaries.semedico.TermVariantComparator;
import de.julielab.neo4j.plugins.datarepresentation.constants.MorphoConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.MorphoRelationConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants;
import org.neo4j.graphdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_ID;

public class ConceptTermInsertion {
    private final static Logger log = LoggerFactory.getLogger(ConceptTermInsertion.class);

    /**
     * Expected format:
     *
     * <pre>
     * {"tid1": {
     *         "docID1": {
     *             "variant1": count1,
     *             "variant2": count2,
     *             ...
     *         },
     *         "docID2": {
     *             ...
     *          }
     *     },
     * "tid2": {
     *     ...
     *     }
     * }
     * </pre>
     *
     * @param conceptVariants The concept term variants in the above described JSON format.
     * @param type 'writingVariants' or 'acronyms'. Causes the given variants to appended to the respective {@link @MorphoLabel} nodes.
     */
    static void addConceptVariant(Transaction tx, String conceptVariants, String type) throws IOException {
        Label variantsAggregationLabel;
        Label variantNodeLabel;

        EdgeTypes variantRelationshipType;
        if (type.equals("writingVariants")) {
            variantsAggregationLabel = MorphoLabel.WRITING_VARIANTS;
            variantNodeLabel = MorphoLabel.WRITING_VARIANT;
            variantRelationshipType = EdgeTypes.HAS_VARIANTS;
        } else if (type.equals("acronyms")) {
            variantsAggregationLabel = MorphoLabel.ACRONYMS;
            variantNodeLabel = MorphoLabel.ACRONYM;
            variantRelationshipType = EdgeTypes.HAS_ACRONYMS;
        } else
            throw new IllegalArgumentException("Unknown lexico-morphological type \"" + type + "\".");
        try (StringReader stringReader = new StringReader(conceptVariants)) {
            JsonReader jsonReader = new JsonReader(stringReader);
            // object holding the concept IDs mapped to their respective
            // writing variant object
            jsonReader.beginObject();
            while (jsonReader.hasNext()) {
                String conceptId = jsonReader.nextName();
                // Conceptually, this is a "Map<DocId, Map<Variants,
                // Count>>"
                Map<String, Map<String, Integer>> variantCountsInDocs = new HashMap<>();
                // object mapping document IDs to variant count objects
                jsonReader.beginObject();
                while (jsonReader.hasNext()) {
                    String docId = jsonReader.nextName();

                    // object mapping variants to counts
                    jsonReader.beginObject();
                    TreeMap<String, Integer> variantCounts = new TreeMap<>(new TermVariantComparator());
                    while (jsonReader.hasNext()) {
                        String variant = jsonReader.nextName();
                        int count = jsonReader.nextInt();
                        if (variantCounts.containsKey(variant)) {
                            // may happen if the tree map comparator deems
                            // the
                            // variant equal to another variant (most
                            // probably
                            // due to case normalization)
                            // add the count of the two variants deemed to
                            // be
                            // "equal"
                            Integer currentCount = variantCounts.get(variant);
                            variantCounts.put(variant, currentCount + count);
                        } else {
                            variantCounts.put(variant, count);
                        }
                    }
                    jsonReader.endObject();
                    variantCountsInDocs.put(docId, variantCounts);
                }
                jsonReader.endObject();

                if (variantCountsInDocs.isEmpty()) {
                    log.debug("Concept with ID " + conceptId + " has no writing variants / acronyms attached.");
                    continue;
                }
                Node concept = tx.findNode(ConceptLabel.CONCEPT, PROP_ID, conceptId);
                if (null == concept) {
                    log.warn("Concept with ID " + conceptId
                            + " was not found, cannot add writing variants / acronyms.");
                    continue;
                }

                // If we are this far, we actually got new variants.
                // Get or create a new node representing the variants for
                // the current concept. We need this since we want to store the
                // variants as well as their counts.
                Relationship hasVariantsRel = concept.getSingleRelationship(variantRelationshipType,
                        Direction.OUTGOING);
                if (null == hasVariantsRel) {
                    Node variantsNode = tx.createNode(variantsAggregationLabel);
                    hasVariantsRel = concept.createRelationshipTo(variantsNode, variantRelationshipType);
                }
                Node variantsNode = hasVariantsRel.getEndNode();
                for (String docId : variantCountsInDocs.keySet()) {
                    Map<String, Integer> variantCounts = variantCountsInDocs.get(docId);
                    for (String variant : variantCounts.keySet()) {
                        String normalizedVariant = TermVariantComparator.normalizeVariant(variant);
                        Node variantNode = tx.findNode(variantNodeLabel, MorphoConstants.PROP_ID,
                                normalizedVariant);
                        if (null == variantNode) {
                            variantNode = tx.createNode(variantNodeLabel);
                            variantNode.setProperty(NodeConstants.PROP_ID, normalizedVariant);
                            variantNode.setProperty(MorphoConstants.PROP_NAME, variant);
                        }
                        // with 'specific' we mean the exact relationship
                        // connecting the variant with the variants node
                        // belonging to the current concept (and no other concept
                        // -
                        // ambiguity!)
                        Relationship specificElementRel = null;
                        for (Relationship elementRel : variantNode.getRelationships(Direction.INCOMING,
                                EdgeTypes.HAS_ELEMENT)) {
                            if (elementRel.getStartNode().equals(variantsNode)
                                    && elementRel.getEndNode().equals(variantNode)) {
                                specificElementRel = elementRel;
                                break;
                            }
                        }
                        if (null == specificElementRel) {
                            specificElementRel = variantsNode.createRelationshipTo(variantNode,
                                    EdgeTypes.HAS_ELEMENT);
                            specificElementRel.setProperty(MorphoRelationConstants.PROP_DOCS, new String[0]);
                            specificElementRel.setProperty(MorphoRelationConstants.PROP_COUNTS, new int[0]);
                        }
                        String[] documents = (String[]) specificElementRel
                                .getProperty(MorphoRelationConstants.PROP_DOCS);
                        int[] counts = (int[]) specificElementRel.getProperty(MorphoRelationConstants.PROP_COUNTS);
                        int docIndex = Arrays.binarySearch(documents, docId);
                        Integer count = variantCounts.get(variant);

                        // found the document, we can just set the new value
                        if (docIndex >= 0) {
                            counts[docIndex] = count;
                        } else {
                            int insertionPoint = -1 * (docIndex + 1);
                            // we don't have a record for this document
                            String[] newDocuments = new String[documents.length + 1];
                            int[] newCounts = new int[newDocuments.length];

                            if (insertionPoint > 0) {
                                // copy existing values before the new
                                // documents entry
                                System.arraycopy(documents, 0, newDocuments, 0, insertionPoint);
                                System.arraycopy(counts, 0, newCounts, 0, insertionPoint);
                            }
                            newDocuments[insertionPoint] = docId;
                            newCounts[insertionPoint] = count;
                            if (insertionPoint < documents.length) {
                                // copy existing values after the new
                                // document entry
                                System.arraycopy(documents, insertionPoint, newDocuments, insertionPoint + 1,
                                        documents.length - insertionPoint);
                                System.arraycopy(counts, insertionPoint, newCounts, insertionPoint + 1,
                                        counts.length - insertionPoint);
                            }
                            specificElementRel.setProperty(MorphoRelationConstants.PROP_DOCS, newDocuments);
                            specificElementRel.setProperty(MorphoRelationConstants.PROP_COUNTS, newCounts);
                        }
                    }
                }
            }
            jsonReader.endObject();
            jsonReader.close();
        }
    }

}
