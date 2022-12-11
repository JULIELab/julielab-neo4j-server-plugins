package de.julielab.neo4j.plugins.concepts;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multiset.Entry;
import de.julielab.neo4j.plugins.auxiliaries.JulieNeo4jUtilities;
import de.julielab.neo4j.plugins.auxiliaries.semedico.CoordinatesMap;
import de.julielab.neo4j.plugins.auxiliaries.semedico.NodeUtilities;
import de.julielab.neo4j.plugins.auxiliaries.semedico.SequenceManager;
import de.julielab.neo4j.plugins.constants.semedico.SequenceConstants;
import de.julielab.neo4j.plugins.datarepresentation.ConceptCoordinates;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcept;
import de.julielab.neo4j.plugins.datarepresentation.ImportOptions;
import de.julielab.neo4j.plugins.datarepresentation.constants.AggregateConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.ConceptRelationConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.NodeIDPrefixConstants;
import de.julielab.neo4j.plugins.util.AggregateConceptInsertionException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;

import javax.ws.rs.Path;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static de.julielab.neo4j.plugins.auxiliaries.PropertyUtilities.getNonNullNodeProperty;
import static de.julielab.neo4j.plugins.auxiliaries.PropertyUtilities.mergeArrayValue;
import static de.julielab.neo4j.plugins.concepts.ConceptInsertion.registerNewHollowConceptNode;
import static de.julielab.neo4j.plugins.concepts.ConceptLabel.AGGREGATE;
import static de.julielab.neo4j.plugins.concepts.ConceptLabel.*;
import static de.julielab.neo4j.plugins.concepts.ConceptLookup.lookupConcept;
import static de.julielab.neo4j.plugins.concepts.ConceptLookup.lookupConceptBySourceId;
import static de.julielab.neo4j.plugins.concepts.ConceptManager.UNKNOWN_CONCEPT_SOURCE;
import static de.julielab.neo4j.plugins.concepts.ConceptManager.getErrorResponse;
import static de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants.*;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;



@Path("/" + ConceptAggregateManager.CAM_REST_ENDPOINT)
public class ConceptAggregateManager {

    public static final String COPY_AGGREGATE_PROPERTIES = "copy_aggregate_properties";
    public static final String BUILD_AGGREGATES_BY_PREFERRED_NAME = "build_aggregates_by_preferred_name";
    public static final String BUILD_AGGREGATES_BY_MAPPINGS = "build_aggregates_by_mappings";
    public static final String DELETE_AGGREGATES = "delete_aggregates";
    public static final String CAM_REST_ENDPOINT = "concept_aggregate_manager";
    public static final String KEY_LABEL = "label";
    public static final String KEY_AGGREGATED_LABEL = "aggregated_label";
    public static final String KEY_AGGREGATED_LABELS = "aggregated_labels";
    public static final String KEY_SKIP_EXISTING_PROPERTIES = "skip_existing_properties";
    public static final String KEY_ALLOWED_MAPPING_TYPES = "allowedMappingTypes";
    public static final String KEY_COPY_PROPERTIES = "copy_properties";
    public static final String KEY_NAME_PROPERTY = "name_property";
    public static final String RET_KEY_NUM_AGGREGATES = "numAggregates";
    public static final String RET_KEY_NUM_ELEMENTS = "numElements";
    public static final String RET_KEY_NUM_PROPERTIES = "numProperties";

    private final DatabaseManagementService dbms;

    public ConceptAggregateManager(@Context DatabaseManagementService dbms) {
        this.dbms = dbms;
    }

    /**
     * Adds an aggregate concept. An aggregate concept is a concept of the following
     * input form:<br/>
     *
     * <pre>
     * {
     *     'aggregate':true,
     *     'elementSrcIds':['id4','id29','id41']
     *     'sources':['NCBI Gene'],
     *     'copyProperties':['prefName','synonyms']
     * }
     * </pre>
     * <p>
     * I.e. a representative concept that has no distinct properties on its own. It
     * will get links to the concept source IDs given in <code>elementSrcIds</code>
     * with respect to <code>source</code>. The <code>copyProperties</code> property
     * contains the properties of element concepts that should be copied into the
     * aggregate and does not have to be present in which case nothing will be
     * copied. The copy process will NOT be done in this method call but must be
     * triggered manually via
     * {@link #copyAggregateProperties()}.
     *
     * @param tx                 The current transaction.
     * @param jsonConcept        The aggregate encoded into JSON format.
     * @param nodesByCoordinates The currently imported nodes.
     * @param insertionReport    The current insertion report.
     * @param importOptions      The current import options.
     * @throws AggregateConceptInsertionException If the aggregate could not be added.
     */
    static void insertAggregateConcept(Transaction tx, ImportConcept jsonConcept,
                                       CoordinatesMap nodesByCoordinates, InsertionReport insertionReport, ImportOptions importOptions, Log log)
            throws AggregateConceptInsertionException {
        try {
            ConceptCoordinates aggCoordinates = jsonConcept.coordinates != null ? jsonConcept.coordinates
                    : new ConceptCoordinates();
            String aggPrefName = jsonConcept.prefName;
            String aggOrgId = aggCoordinates.originalId;
            String aggOrgSource = aggCoordinates.originalSource;
            String aggSrcId = aggCoordinates.sourceId;
            String aggSource = aggCoordinates.source;
            if (null == aggSource)
                aggSource = UNKNOWN_CONCEPT_SOURCE;
            Node aggregate = lookupConcept(tx, aggCoordinates);
            if (null != aggregate) {
                if (!aggregate.hasLabel(HOLLOW))
                    return;
                // remove the HOLLOW label, we have to aggregate information now and
                // will set it to the node in the following
                aggregate.removeLabel(HOLLOW);
                aggregate.addLabel(AGGREGATE);
            }
            if (aggregate == null) {
                aggregate = tx.createNode(AGGREGATE);
            }
            boolean includeAggreationInHierarchy = jsonConcept.aggregateIncludeInHierarchy;
            // If the aggregate is to be included into the hierarchy, it also should
            // be a CONCEPT for path creation
            if (includeAggreationInHierarchy)
                aggregate.addLabel(CONCEPT);
            List<ConceptCoordinates> elementCoords = jsonConcept.elementCoordinates;
            for (ConceptCoordinates elementCoordinates : elementCoords) {
                String elementSource = elementCoordinates.source;
                if (null == elementCoordinates.source)
                    elementSource = UNKNOWN_CONCEPT_SOURCE;
                Node element = nodesByCoordinates.get(elementCoordinates);
                if (null != element) {
                    // If the source ID matches but not the sources then this is
                    // the wrong node.
                    String[] sourcesForSourceId = NodeUtilities.getSourcesForSourceId(element, elementCoordinates.sourceId);
                    if (Arrays.binarySearch(sourcesForSourceId, elementSource) < 0)
                        element = null;
                }
                log.debug("Looking up element by source ID %s and source %s", elementCoordinates.sourceId, elementSource);
                if (null == element)
                    element = lookupConceptBySourceId(tx, elementCoordinates.sourceId, elementSource, false);
                log.debug("Found element with source ID %s and source %s", elementCoordinates.sourceId, elementSource);
                if (null == element && importOptions.createHollowAggregateElements) {
                    element = registerNewHollowConceptNode(tx, log, elementCoordinates);
                }
                if (element != null) {
                    aggregate.createRelationshipTo(element, ConceptEdgeTypes.HAS_ELEMENT);
                }
            }

            // Set the aggregate's properties
            if (null != aggPrefName)
                aggregate.setProperty(PROP_PREF_NAME, aggPrefName);
            if (null != aggSrcId) {
                NodeUtilities.mergeSourceId(tx, aggregate, aggSrcId, aggSource, false);
                // if the aggregate has a source ID, add it to the respective
                // map for later access during the relationship insertion phase
                nodesByCoordinates.put(new ConceptCoordinates(aggCoordinates), aggregate);
            }
            if (null != aggOrgId)
                aggregate.setProperty(PROP_ORG_ID, aggOrgId);
            if (null != aggOrgSource)
                aggregate.setProperty(PROP_ORG_SRC, aggOrgSource);
            List<String> copyProperties = jsonConcept.copyProperties;
            if (null != copyProperties && !copyProperties.isEmpty())
                aggregate.setProperty(PROP_COPY_PROPERTIES, copyProperties.toArray(new String[0]));

            List<String> generalLabels = jsonConcept.generalLabels;
            for (int i = 0; null != generalLabels && i < generalLabels.size(); i++) {
                aggregate.addLabel(Label.label(generalLabels.get(i)));
            }

            String aggregateId = NodeIDPrefixConstants.AGGREGATE_TERM
                    + SequenceManager.getNextSequenceValue(tx, SequenceConstants.SEQ_AGGREGATE_TERM);
            aggregate.setProperty(PROP_ID, aggregateId);

            insertionReport.numConcepts++;
        } catch (Exception e) {
            throw new AggregateConceptInsertionException(
                    "Aggregate concept creation failed for aggregate " + jsonConcept, e);
        }
    }

    /**
     * Aggregates terms that have equal preferred name and synonyms, after some
     * minor normalization.
     *
     * @param graphDb               The graph database to work on.
     * @param nodeLabels
     * @param nameProperty
     * @param aggregatedLabels
     * @param copyProperties
     * @return
     */
    public static int buildAggregatesForEqualNames(GraphDatabaseService graphDb, List<Label> nodeLabels, String nameProperty, List<Label> aggregatedLabels, String[] copyProperties, Log log) {
        int createdAggregates = 0;
        Comparator<Node> nodeNameComparator = Comparator.comparing(n -> ((String) n.getProperty(nameProperty)));

        // Sort the nodes with the target according to their preferred name
        log.info("Acquiring all non-aggregate nodes with labels %s", nodeLabels);
        List<Long> nodeIds;
        try (final Transaction tx = graphDb.beginTx()) {
            List<Node> nodes = new ArrayList<>();
            for (Label nodeLabel : nodeLabels) {
                ResourceIterable<Node> nodeIterable = () -> tx.findNodes(nodeLabel);
                for (Node node : nodeIterable) {
                    if (!node.hasLabel(AGGREGATE))
                        nodes.add(node);
                }
            }
            log.info("Found %s nodes with labels %s", nodes.size(), nodeLabels);
            log.info("Sorting %s nodes with labels %s by name", nodes.size(), nodeLabels);
            nodes.sort(nodeNameComparator);
            log.info("Sorting of nodes by name is done.");
            nodeIds = nodes.stream().map(Node::getId).collect(Collectors.toList());
        }


        List<Node> equalNameNodes = new ArrayList<>();
        log.info("Creating equal-name aggregates for labels %s with labels %s", nodeLabels, aggregatedLabels);
        Label[] aggregatedLabelsArray = aggregatedLabels.toArray(Label[]::new);
        final Iterator<Long> nodeitId = nodeIds.iterator();
        int batchsize = 1000;
        List<Long> currentBatch = new ArrayList<>(batchsize);
        while (nodeitId.hasNext()) {
            currentBatch.add(nodeitId.next());
            if (currentBatch.size() == batchsize || !nodeitId.hasNext()) {
                try (final Transaction tx = graphDb.beginTx()) {
                    for (long nodeId : currentBatch) {
                        Node n = tx.getNodeById(nodeId);
                        boolean equalTerm = 0 == equalNameNodes.size()
                                || 0 == nodeNameComparator.compare(equalNameNodes.get(equalNameNodes.size() - 1), n);
                        if (equalTerm) {
                            equalNameNodes.add(n);
                        } else if (equalNameNodes.size() > 1) {
                            createAggregate(tx, copyProperties, new HashSet<>(equalNameNodes),
                                    new String[]{AGGREGATE_EQUAL_NAMES.toString()},
                                    aggregatedLabelsArray);
                            ++createdAggregates;
                            if (createdAggregates % 10000 == 0)
                                log.info("Created %s equal-name aggregates for labels %s with labels %s.", createdAggregates, nodeLabels, aggregatedLabels);
                            equalNameNodes.clear();
                            equalNameNodes.add(n);
                        } else {
                            equalNameNodes.clear();
                            equalNameNodes.add(n);
                        }
                    }
                    // handle the last set of equal nodes of the last batch
                    if (!nodeitId.hasNext() && equalNameNodes.size() > 1) {
                        createAggregate(tx, copyProperties, new HashSet<>(equalNameNodes),
                                new String[]{AGGREGATE_EQUAL_NAMES.toString()},
                                aggregatedLabelsArray);
                        ++createdAggregates;
                    }

                    tx.commit();
                    currentBatch.clear();
                    // Clear the nodes that belong to the aggregates that were just committed but also
                    // those that have not yet been processed. We cannot transfer them to another transaction.
                    // Thus, we put their IDs back to the list
                    equalNameNodes.clear();
                    equalNameNodes.stream().map(Node::getId).forEach(currentBatch::add);
                }
            }
        }
        log.info("%s equal-name aggregates were created.", createdAggregates);
        return createdAggregates;
    }

    private static int addUniqueNameLabels(Transaction tx, Label nodeLabel, Log log) {
        ResourceIterable<Node> nodeIterable = () -> tx.findNodes(nodeLabel);
        int count = 0;
        for (Node n : nodeIterable) {
            // omit aggregate nodes
            if (n.hasLabel(AGGREGATE))
                continue;
            // omit nodes that are element of an equal-name aggregate
            if (StreamSupport.stream(n.getRelationships(Direction.INCOMING, ConceptEdgeTypes.HAS_ELEMENT).spliterator(), false).anyMatch(r -> r.getOtherNode(n).hasLabel(AGGREGATE_EQUAL_NAMES)))
                continue;
            n.addLabel(AGGREGATE_EQUAL_NAMES);
            ++count;
            if (count % 1000000 == 0)
                log.info("Added %s label to %s unique-named nodes that are not actually an aggregate.", AGGREGATE_EQUAL_NAMES, count);
        }
        return count;
    }

    public static Map<String, Long> deleteAggregatesBatchWise(GraphDatabaseService graphDb, List<Label> aggregateLabels, Log log) {
        log.info("Removing all nodes with label %s", aggregateLabels);
        long numNodes = 0;
        long numLabels = 0;
        long numRel = 0;
        long numNodesInThisBatch;
        long numRelInThisBatch;
        for (Label aggregateLabel : aggregateLabels) {
            log.info("Deleting aggregate nodes with label %s", aggregateLabel);
            do {
                try (Transaction tx = graphDb.beginTx()) {
                    numNodesInThisBatch = 0;
                    numRelInThisBatch = 0;
                    Iterator<Node> aggregates = tx.findNodes(aggregateLabel);
                    // Delete the nodes in batches. Otherwise, we get very large transactions and, in consequence,
                    // memory issues.
                    while (aggregates.hasNext() && numNodesInThisBatch < 10000) {
                        Node aggregate = aggregates.next();
                        if (!aggregate.hasLabel(AGGREGATE)) {
                            // For terms that are not really aggregates, we just remove
                            // the label - we want to keep the term
                            // itself.
                            aggregate.removeLabel(aggregateLabel);
                            ++numLabels;
                            continue;
                        }
                        // Delete the aggregate.
                        for (Relationship rel : aggregate.getRelationships()) {
                            rel.delete();
                            ++numRelInThisBatch;
                        }
                        aggregate.delete();
                        ++numNodesInThisBatch;
                    }
                    numNodes += numNodesInThisBatch;
                    numRel += numRelInThisBatch;
                    tx.commit();
                    log.info("Deleted %s nodes", numNodes);
                }
            } while (numNodesInThisBatch > 0);
        }
        log.info("Finished deleting %s edges and %s nodes with label %s and removed %s %s labels from non-aggregate nodes", numRel, numNodes, aggregateLabels, numLabels, aggregateLabels);
        return Map.of("numNodes", numNodes, "numRelationships", numRel);
    }

    public static void deleteAggregates(Transaction tx, Label aggregateLabel, Log log) {
        log.info("Removing all nodes with label %s", aggregateLabel.name());
        long numNodes = 0;
        long numRel = 0;
        ResourceIterable<Node> aggregates = () -> tx.findNodes(aggregateLabel);
        for (Node aggregate : aggregates) {
            if (!aggregate.hasLabel(AGGREGATE)) {
                // For terms that are not really aggregates, we just remove
                // the label - we want to keep the term
                // itself.
                aggregate.removeLabel(aggregateLabel);
                continue;
            }
            // Delete the aggregate.
            for (Relationship rel : aggregate.getRelationships()) {
                rel.delete();
                ++numRel;
            }
            aggregate.delete();
            ++numNodes;
            if (numNodes % 10000 == 0)
                log.info("Deleted %s nodes", numNodes);
        }
        log.info("Finished deleting %s edges and %s nodes with label %s", numRel, numNodes, aggregateLabel.name());
    }

    /**
     * @param allowedMappingTypes  The mapping types that will be used to build aggregates. This
     *                             relates to the property of mapping relationships that exposes
     *                             the type of mapping the relationships is part of. All edges
     *                             that are of at least one mapping type delivered by this
     *                             parameter will be traversed to build an aggregate. That means,
     *                             the more mapping types are allowed, the bigger the aggregates
     *                             become. Also, each distinguished set of allowed mapping types
     *                             defines a particular set of aggregations.
     * @param allowedTermLabel     Label to restrict the terms for which aggregates are built.
     * @param aggregatedTermsLabel Label for terms that have been processed by the aggregation
     *                             algorithm. Such terms can be aggregate terms (with the label
     *                             {@link ConceptLabel#AGGREGATE}) or just plain terms (with the label
     *                             {@link ConceptLabel#CONCEPT}) that are not an element of an aggregate.
     * @return
     */
    public static int buildAggregatesForMappings(Transaction tx, Set<String> allowedMappingTypes,
                                                 Label allowedTermLabel, Label aggregatedTermsLabel, Log log) {
        log.info("Building aggregates for mappings " + allowedMappingTypes + " and terms with label "
                + allowedTermLabel);
        int numCreatedAggregates = 0;
        String[] copyProperties = new String[]{PROP_PREF_NAME, PROP_SYNONYMS,
                PROP_WRITING_VARIANTS, PROP_DESCRIPTIONS, PROP_FACETS};
        // At first, delete all mapping aggregates since they will be built
        // again afterwards.
        deleteAggregates(tx, aggregatedTermsLabel, log);

        // Iterate through terms, look for mappings and generate mapping
        // aggregates
        Label label = null == allowedTermLabel ? CONCEPT : allowedTermLabel;
        ResourceIterable<Node> termIterable = () -> tx.findNodes(label);
        for (Node term : termIterable) {
            // Determine recursively other nodes with which a new aggregate
            // should be created.

            // First collect all mapping aggregates with the correct mapping
            // types this term already is element of.
            // We use this for duplicate avoidance.
            Set<Node> aggregateNodes = getMatchingAggregates(term, allowedMappingTypes, aggregatedTermsLabel);
            if (aggregateNodes.size() > 1)
                throw new IllegalStateException("Term with ID " + term.getProperty(PROP_ID)
                        + " is part of multiple aggregates of the same type, thus duplicates. The aggregate nodes are: "
                        + aggregateNodes);
            if (aggregateNodes.size() == 1)
                // This term already is part of an aggregate of correct
                // type, continue to the next term
                continue;

            // This set will be used to recursively collect all terms that
            // are mapped directory or indirectly to the
            // current term, thus traversing the whole "mapped-to subgraph"
            // of this term. I.e. the "mapped-to"
            // relation is an equivalence relation to us.
            Set<Node> elements = new HashSet<>();
            Set<Node> visited = new HashSet<>();
            determineMappedSubgraph(allowedMappingTypes, allowedTermLabel, term, elements, visited);

            if (elements.size() > 1) {
                createAggregate(tx, copyProperties, elements,
                        allowedMappingTypes.toArray(new String[0]),
                        // TermLabel.AGGREGATE_MAPPING);
                        aggregatedTermsLabel);
                ++numCreatedAggregates;

                // aggregate could be null if we have a term that just has
                // no mappings
                // aggregate.addLabel(TermLabel.AGGREGATE_MAPPING);
                // aggregate.addLabel(aggregatedTermsLabel);
            } else {
                // The current is not mapped to other terms, at least not
                // with one of the allowed mapping types. So
                // it is "its own" aggregate.
                term.addLabel(aggregatedTermsLabel);
            }
        }
        return numCreatedAggregates;
    }

    /**
     * Concatenates the values of the elements of <tt>aggregate</tt> and returns
     * them as an array.
     *
     * @param aggregate The aggregate for whose elements properties are requested.
     * @param property  The requested property.
     * @return The values of property <tt>property</tt> in the elements of
     * <tt>aggregate</tt>
     */
    public static String[] getPropertyValueOfElements(Node aggregate, String property) {
        if (!aggregate.hasLabel(AGGREGATE))
            throw new IllegalArgumentException(
                    "Node " + NodeUtilities.getNodePropertiesAsString(aggregate) + " is not an aggregate.");
        Iterable<Relationship> elementRels = aggregate.getRelationships(Direction.OUTGOING, ConceptEdgeTypes.HAS_ELEMENT);
        List<String> elementValues = new ArrayList<>();
        for (Relationship elementRel : elementRels) {
            String[] value = NodeUtilities.getNodePropertyAsStringArrayValue(elementRel.getEndNode(), property);
            for (int i = 0; value != null && i < value.length; i++)
                elementValues.add(value[i]);
        }
        return elementValues.isEmpty() ? null : elementValues.toArray(new String[0]);
    }

    protected static void determineMappedSubgraph(Set<String> allowedMappingTypes, Label allowedTermLabel, Node term,
                                                  Set<Node> elements, Set<Node> visited) {
        if (visited.contains(term))
            return;
        visited.add(term);
        Iterable<Relationship> mappings = term.getRelationships(ConceptEdgeTypes.IS_MAPPED_TO);
        for (Relationship mapping : mappings) {
            if (!mapping.hasProperty(ConceptRelationConstants.PROP_MAPPING_TYPE))
                throw new IllegalStateException("The mapping relationship " + mapping + " does not specify its type.");
            // We must check whether the found mapping is of a type that should
            // be aggregated in this call.
            String[] mappingTypes = (String[]) mapping.getProperty(ConceptRelationConstants.PROP_MAPPING_TYPE);
            for (String mappingType : mappingTypes) {
                if (allowedMappingTypes.contains(mappingType)) {
                    // There is at least one allowed mapping and thus an
                    // aggregate has to be created. Of course, the
                    // current term itself will also be an element of the
                    // aggregate.
                    if (null == allowedTermLabel || term.hasLabel(allowedTermLabel))
                        elements.add(term);
                    // This is an allowed mapping of the current term. Thus, all
                    // directly mapped terms to this
                    // term should be elements of the resulting aggregate.
                    Node otherTerm = mapping.getOtherNode(term);
                    if (!elements.contains(otherTerm)) {
                        if (null == allowedTermLabel || otherTerm.hasLabel(allowedTermLabel))
                            elements.add(otherTerm);
                        determineMappedSubgraph(allowedMappingTypes, allowedTermLabel, otherTerm, elements, visited);
                    }
                }
            }
        }
    }

    /**
     * Returns all the aggregate nodes where <tt>conceptNode</tt> is an element of and
     * where the aggregate is exactly of the mapping types specified in
     * <tt>allowedMappingTypes</tt>.
     *
     * @param conceptNode         The concept node that is an element of the sought aggregates.
     * @param allowedMappingTypes The mapping type for which aggregates are requested.
     * @param aggregateLabel      The aggregate label for requested aggregate nodes.
     * @return Matching aggregates for which <tt>conceptNode</tt> is an element of.
     */
    protected static Set<Node> getMatchingAggregates(Node conceptNode, Set<String> allowedMappingTypes, Label aggregateLabel) {
        Set<Node> aggregateNodes;
        aggregateNodes = new HashSet<>();
        Iterable<Relationship> elementRelationships = conceptNode.getRelationships(ConceptEdgeTypes.HAS_ELEMENT);
        for (Relationship elementRelationship : elementRelationships) {
            Node aggregate = elementRelationship.getOtherNode(conceptNode);
            if (aggregate.hasLabel(aggregateLabel) && aggregate.hasLabel(AGGREGATE) && aggregate.hasProperty(PROP_MAPPING_TYPE)) {
                String[] mappingTypes = (String[]) aggregate.getProperty(PROP_MAPPING_TYPE);
                List<String> mappingTypesList = Arrays.asList(mappingTypes);
                boolean correctMappingTypes = true;
                for (String mappingType : mappingTypesList) {
                    if (!allowedMappingTypes.contains(mappingType)) {
                        correctMappingTypes = false;
                        break;
                    }
                }
                for (String mappingType : allowedMappingTypes) {
                    if (!mappingTypesList.contains(mappingType)) {
                        correctMappingTypes = false;
                        break;
                    }
                }
                if (correctMappingTypes)
                    aggregateNodes.add(aggregate);
            }
        }
        return aggregateNodes;
    }

    private static void createAggregate(Transaction tx, String[] copyProperties, Set<Node> elementTerms,
                                        String[] mappingTypes, Label... labels) {
        if (elementTerms.isEmpty())
            return;
        Node aggregate = tx.createNode(labels);
        aggregate.addLabel(AGGREGATE);
        aggregate.setProperty(PROP_COPY_PROPERTIES, copyProperties);
        aggregate.setProperty(PROP_MAPPING_TYPE, mappingTypes);
        for (Label termLabel : labels) {
            aggregate.addLabel(termLabel);
        }
        for (Node elementTerm : elementTerms) {
            aggregate.createRelationshipTo(elementTerm, ConceptEdgeTypes.HAS_ELEMENT);
        }
        String aggregateId = NodeIDPrefixConstants.AGGREGATE_TERM
                + SequenceManager.getNextSequenceValue(tx, SequenceConstants.SEQ_AGGREGATE_TERM);
        aggregate.setProperty(PROP_ID, aggregateId);
    }

    /**
     * Fills <tt>aggregate</tt> with property values from its elements.
     * Currently only called separately after all aggregates have been
     * determined to save performance. During the process of aggregate creation,
     * sometimes aggregations are merged. We don't copy all properties again but
     * only merge the elements and compute the property values from the final
     * elements after the aggregation creation process has finished. This has to
     * be done explicitly and is not done automatically.
     *
     * @param aggregate              The aggregate node to assembly element properties to.
     * @param skipExistingProperties
     * @param copyProperties         The properties that should be copied into the aggregate.
     * @param copyStats              An object to collect statistics over the copy process.
     */
    public static void copyAggregateProperties(Node aggregate, boolean skipExistingProperties, String[] copyProperties,
                                               CopyAggregatePropertiesStatistics copyStats) {
        String[] unskippedProperties = copyProperties;
        // first, clear the properties be copied in case we make a refresh
        if (skipExistingProperties) {
            unskippedProperties = Arrays.stream(copyProperties).filter(Predicate.not(aggregate::hasProperty)).toArray(String[]::new);
        }
        for (String copyProperty : unskippedProperties) {
            aggregate.removeProperty(copyProperty);
        }
        Iterable<Relationship> elementRels = aggregate.getRelationships(ConceptEdgeTypes.HAS_ELEMENT);
        // List of properties to copy that are no array properties - and thus
        // cannot be merged - but have different
        // values.
        Set<String> divergentProperties = new HashSet<>();
        // For each element...
        Map<String, Object> newAggregateProperties = new HashMap<>();
        // Collect existing property values on the aggregate
        for (String copyProperty : unskippedProperties) {
            if (aggregate.hasProperty(copyProperty))
                newAggregateProperties.put(copyProperty, aggregate.getProperties(copyProperty));
        }
        for (Relationship elementRel : elementRels) {
            Node term = elementRel.getEndNode();
            if (null != copyStats)
                copyStats.numElements++;
            // Copy each specified property.
            // Array properties are merged.
            // Non-array properties are set, if non existent. If a non-array
            // property has multiple, different values
            // among the elements, this property is subject to the majority vote
            // after this loop.
            for (String copyProperty : unskippedProperties) {
                if (term.hasProperty(copyProperty)) {
                    if (null != copyStats)
                        copyStats.numProperties++;
                    Object property = term.getProperty(copyProperty);
                    if (property.getClass().isArray()) {
                        final Object[] mergedValue = mergeArrayValue(newAggregateProperties.getOrDefault(copyProperty, null), JulieNeo4jUtilities.convertArray(property));
                        // TODO
//                        aggregate.setProperty(copyProperty, mergedValue);
                        newAggregateProperties.put(copyProperty, mergedValue);
                    } else {
                        if (!newAggregateProperties.containsKey(copyProperty))
                            newAggregateProperties.put(copyProperty, property);
//                        setNonNullNodeProperty(aggregate, copyProperty, property);
                        Object aggregateProperty = newAggregateProperties.get(copyProperty);
                        if (!aggregateProperty.equals(property)) {
                            divergentProperties.add(copyProperty);
                        }
                    }
                }
            }
        }
        // We now make majority votes on the values of divergent properties. The
        // minority property values are then
        // stored in a special property with the suffix
        // AggregateConstants.SUFFIX_DIVERGENT_ELEMENT_ROPERTY

        for (String divergentProperty : divergentProperties) {
            Multiset<Object> propertyValues = HashMultiset.create();
            elementRels = aggregate.getRelationships(ConceptEdgeTypes.HAS_ELEMENT);
            for (Relationship elementRel : elementRels) {
                Node term = elementRel.getEndNode();
                Object propertyValue = getNonNullNodeProperty(term, divergentProperty);
                if (null != propertyValue)
                    propertyValues.add(propertyValue);
            }
            // Determine the majority value.
            Object majorityValue = null;
            int maxCount = 0;
            for (Entry<Object> entry : propertyValues.entrySet()) {
                if (entry.getCount() > maxCount) {
                    majorityValue = entry.getElement();
                    maxCount = entry.getCount();
                }
            }

            // Set the majority value to the aggregate.
//            aggregate.setProperty(divergentProperty, majorityValue);
            newAggregateProperties.put(divergentProperty, majorityValue);
            // Set the minority values to the aggregate as a special property.
            for (Object propertyValue : propertyValues.elementSet()) {
                if (!propertyValue.equals(majorityValue)) {
                    Object[] convert = JulieNeo4jUtilities.convertElementsIntoArray(propertyValue.getClass(),
                            propertyValue);
                    final String divergentKey = divergentProperty + AggregateConstants.SUFFIX_DIVERGENT_ELEMENT_ROPERTY;
                    final Object[] mergedValue = mergeArrayValue(newAggregateProperties.getOrDefault(divergentKey, null), convert);
                    newAggregateProperties.put(divergentKey, mergedValue);
//                    mergeArrayProperty(aggregate,
//                            divergentKey, convert);
                }
            }
        }

        // The aggregate could have a conflict on the preferred name. This is
        // already resolved by a majority
        // vote above. We now additionally merge the minority names to the
        // synonyms.
        final String divergentPrefnameKey = PROP_PREF_NAME + AggregateConstants.SUFFIX_DIVERGENT_ELEMENT_ROPERTY;
        final Object[] synonymsWithDivergentPrefNames = mergeArrayValue(newAggregateProperties.getOrDefault(PROP_SYNONYMS, null), (Object[]) newAggregateProperties.get(divergentPrefnameKey));
        if (synonymsWithDivergentPrefNames != null)
            newAggregateProperties.put(PROP_SYNONYMS, synonymsWithDivergentPrefNames);
//        mergeArrayProperty(aggregate, PROP_SYNONYMS,
//                (Object[]) getNonNullNodeProperty(aggregate,
//                        divergentPrefnameKey));

        // As a last step, remove duplicate synonyms, case ignored
        if (newAggregateProperties.containsKey(PROP_SYNONYMS)) {
            String[] synonyms = (String[]) newAggregateProperties.get(PROP_SYNONYMS);
            Set<String> lowerCaseSynonyms = new HashSet<>();
            List<String> acceptedSynonyms = new ArrayList<>();
            for (String synonym : synonyms) {
                String lowerCaseSynonym = synonym.toLowerCase();
                if (!lowerCaseSynonyms.contains(lowerCaseSynonym)) {
                    lowerCaseSynonyms.add(lowerCaseSynonym);
                    acceptedSynonyms.add(synonym);
                }
            }
            Collections.sort(acceptedSynonyms);
            newAggregateProperties.put(PROP_SYNONYMS,
                    acceptedSynonyms.toArray(new String[0]));
        }

        for (String copyProperty : newAggregateProperties.keySet()) {
            aggregate.setProperty(copyProperty, newAggregateProperties.get(copyProperty));
        }
    }

    /**
     * <p>
     * Retrieves all nodes that are connected to <tt>aggregate</tt> directly or indirectly through
     * {@link ConceptEdgeTypes#HAS_ELEMENT} edges. Thus, the "leaves" are collected.
     * </p>
     * <p>
     * Note that <tt>aggregate</tt> might already be a leaf in the above sense. In this case, the input node is returned itself.
     * </p>
     *
     * @param aggregate The start node to resolve non-aggregate elements for. May be a non-aggregate itself.
     * @return All element nodes (i.e. nodes that are connected to an aggregate node via {@link ConceptEdgeTypes#HAS_ELEMENT} and are not aggregates themselves) reachable from <tt>aggregate</tt>.
     */
    public static List<Node> getNonAggregateElements(Node aggregate) {
        List<Node> elements = new ArrayList<>();
        getNonAggregateElements(aggregate, elements);
        return elements;
    }

    public static void getNonAggregateElements(Node node, List<Node> elements) {
        if (!node.hasLabel(AGGREGATE)) {
            elements.add(node);
            // This node is not an aggregate, there are no further elements below.
            return;
        }
        Iterable<Relationship> hasElements = node.getRelationships(Direction.OUTGOING, ConceptEdgeTypes.HAS_ELEMENT);
        for (Relationship hasElement : hasElements) {
            Node endNode = hasElement.getEndNode();
            if (endNode.hasLabel(AGGREGATE))
                getNonAggregateElements(endNode, elements);
            else
                elements.add(endNode);
        }
    }

    /**
     * <ul>
     *  <li>{@link #KEY_LABEL}:Label to restrict the concepts to that are considered for aggregation creation. </li>
     * </ul>
     *
     * @param jsonParameterObject The parameter JSON object.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Path(BUILD_AGGREGATES_BY_PREFERRED_NAME)
    public Response buildAggregatesByPreferredName(String jsonParameterObject, @Context Log log) {
        try {
            ObjectMapper om = new ObjectMapper();
            var parameterMap = om.readValue(jsonParameterObject, Map.class);
            if (!parameterMap.containsKey(KEY_LABELS))
                throw new IllegalArgumentException("Parameter '" + KEY_LABELS + "' not specified.");
            List<Label> aggregatedLabels = List.of(AGGREGATE_EQUAL_NAMES);
            String nameProperty = PROP_PREF_NAME;
            if (parameterMap.containsKey(KEY_NAME_PROPERTY))
                nameProperty = (String) parameterMap.get(KEY_NAME_PROPERTY);
            if (parameterMap.containsKey(KEY_AGGREGATED_LABELS))
                aggregatedLabels = ((List<String>) parameterMap.get(KEY_AGGREGATED_LABELS)).stream().map(Label::label).collect(Collectors.toList());
            List<String> copyProperties = List.of(PROP_PREF_NAME, PROP_SYNONYMS);
            if (parameterMap.containsKey(KEY_COPY_PROPERTIES))
                copyProperties = ((List<String>) parameterMap.get(KEY_COPY_PROPERTIES));
            List<Label> targetLabels = ((List<String>) parameterMap.get(KEY_LABELS)).stream().map(Label::label).collect(Collectors.toList());
            log.info("Creating equal-name-aggregates regarding the '%s' property for concepts with label %s and assigning them label %s", nameProperty, targetLabels, aggregatedLabels);
            GraphDatabaseService graphDb = dbms.database(DEFAULT_DATABASE_NAME);
            int createdAggregates;
            log.info("Beginning transaction for the creation of equal-name aggregates.");
            createdAggregates = ConceptAggregateManager.buildAggregatesForEqualNames(graphDb, targetLabels, nameProperty, aggregatedLabels, copyProperties.toArray(String[]::new), log);
            log.info("Process for the creation of equal-name aggregates has finished.");
            return Response.ok(createdAggregates).build();
        } catch (IOException e) {
            return getErrorResponse(e);
        }
    }

    /**
     * <ul>
     *  <li>{@link #KEY_ALLOWED_MAPPING_TYPES}: The allowed types for IS_MAPPED_TO relationships to be included in aggregation building.</li>
     *  <li>{@link #KEY_AGGREGATED_LABEL}: Label for concepts that have been processed by the aggregation algorithm. Such concepts
     *  can be aggregate concepts (with the label AGGREGATE) or just plain concepts (with the label CONCEPT) that are not an element of an aggregate.</li>
     *  <li>{@link #KEY_LABEL}:Label to restrict the concepts to that are considered for aggregation creation. </li>
     * </ul>
     *
     * @param jsonParameterObject The parameter JSON object.
     */
    @SuppressWarnings("unchecked")
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Path(BUILD_AGGREGATES_BY_MAPPINGS)
    public Response buildAggregatesByMappings(String jsonParameterObject, @Context Log log) {
        try {
            ObjectMapper om = new ObjectMapper();
            var parameterMap = om.readValue(jsonParameterObject, Map.class);
            final Set<String> allowedMappingTypes = new HashSet<>((List<String>) parameterMap.get(KEY_ALLOWED_MAPPING_TYPES));
            Label aggregatedConceptsLabel = Label.label((String) parameterMap.get(KEY_AGGREGATED_LABEL));
            Label allowedConceptLabel = parameterMap.containsKey(KEY_LABEL) ? Label.label((String) parameterMap.get(KEY_LABEL))
                    : null;
            log.info("Creating mapping aggregates for concepts with label {} and mapping types {}", allowedConceptLabel,
                    allowedMappingTypes);
            GraphDatabaseService graphDb = dbms.database(DEFAULT_DATABASE_NAME);
            int createdAggregates;
            try (Transaction tx = graphDb.beginTx()) {
                createdAggregates = ConceptAggregateManager.buildAggregatesForMappings(tx, allowedMappingTypes, allowedConceptLabel,
                        aggregatedConceptsLabel, log);
                tx.commit();
            }
            return Response.ok(createdAggregates).build();
        } catch (Throwable t) {
            return getErrorResponse(t);
        }
    }

    /**
     * <ul>
     *     <li>{@link #KEY_AGGREGATED_LABEL}: Label for concepts that have been processed by the aggregation algorithm.
     *     Such concepts can be aggregate concepts (with the label AGGREGATE) or just plain concepts
     *     (with the label CONCEPT) that are not an element of an aggregate.</li>
     * </ul>
     *
     * @param aggregatedConceptsLabelString The aggregate node label for which to delete the aggregate nodes.
     */
    @DELETE
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path(DELETE_AGGREGATES)
    public Response deleteAggregatesByMappings(@QueryParam(KEY_AGGREGATED_LABELS) String aggregatedConceptsLabelString, @Context Log log) {
        try {
            ObjectMapper om = new ObjectMapper();
            var labelList = om.readValue(aggregatedConceptsLabelString, String[].class);
            List<Label> aggregatedConceptsLabels = Arrays.stream(labelList).map(Label::label).collect(Collectors.toList());
            GraphDatabaseService graphDb = dbms.database(DEFAULT_DATABASE_NAME);
            final Map<String, Long> deletionCounts = ConceptAggregateManager.deleteAggregatesBatchWise(graphDb, aggregatedConceptsLabels, log);
            return Response.ok(deletionCounts).build();
        } catch (Throwable t) {
            return getErrorResponse(t);
        }
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path(COPY_AGGREGATE_PROPERTIES)
    public Object copyAggregateProperties(String jsonParameterObject, @Context Log log) {
        try {
            ObjectMapper om = new ObjectMapper();
            var parameterMap = jsonParameterObject != null && !jsonParameterObject.isBlank() ? om.readValue(jsonParameterObject, Map.class) : Collections.emptyMap();
            boolean skipExistingProperties = Boolean.parseBoolean((String) parameterMap.getOrDefault(KEY_SKIP_EXISTING_PROPERTIES, "true"));
            List<Label> aggregateLabels = ((List<String>) parameterMap.getOrDefault(KEY_AGGREGATED_LABELS, List.of(AGGREGATE.name()))).stream().map(Label::label).collect(Collectors.toList());
            int batchSize = 100;
            int numAggregates = 0;
            log.info("Copying properties of aggregates with labels %s.", aggregateLabels);
            CopyAggregatePropertiesStatistics copyStats = new CopyAggregatePropertiesStatistics();
            GraphDatabaseService graphDb = dbms.database(DEFAULT_DATABASE_NAME);
            for (Label aggregateLabel : aggregateLabels) {
                Queue<String> aggregateIds = new ArrayDeque<>();
                try (Transaction tx = graphDb.beginTx()) {
                    try (ResourceIterator<Node> aggregateIt = tx.findNodes(aggregateLabel)) {
                        while (aggregateIt.hasNext()) {
                            Node aggregate = aggregateIt.next();
                            final String aggregateId = (String) aggregate.getProperty(PROP_ID);
                            if (aggregateId == null)
                                throw new IllegalStateException("There are aggregate nodes without an ID.");
                            aggregateIds.add(aggregateId);
                        }
                    }
                }
                log.info("Retrieved %s aggregates. Now copying properties.", aggregateIds.size());
                int numAggregatesProcessed = 0;
                do {
                    final Set<String> alreadySeenIds = new HashSet<>();
                    try (Transaction tx = graphDb.beginTx()) {
                        log.info("Processing next batch of %s aggregates. Already seen aggregates through recursion are skipped.", batchSize);
                        for (int i = 0; i < batchSize && !aggregateIds.isEmpty(); i++) {
                            final String aggregateId = aggregateIds.poll();
                            final Node aggregate = tx.findNode(AGGREGATE, PROP_ID, aggregateId);
                            numAggregates += copyAggregatePropertiesRecursively(aggregate, skipExistingProperties, copyStats, alreadySeenIds);
                            ++numAggregatesProcessed;
                        }
                        log.info("Processed %s aggregates", numAggregatesProcessed);
                        log.info("Committing aggregate property copy transaction.");
                        tx.commit();
                    }
                } while (!aggregateIds.isEmpty());
            }
            log.info("Finished the copying of properties for %s aggregate nodes.", numAggregates);
            Map<String, Object> reportMap = new HashMap<>();
            reportMap.put(RET_KEY_NUM_AGGREGATES, numAggregates);
            reportMap.put(RET_KEY_NUM_ELEMENTS, copyStats.numElements);
            reportMap.put(RET_KEY_NUM_PROPERTIES, copyStats.numProperties);
            return Response.ok(reportMap).build();
        } catch (Throwable t) {
            return getErrorResponse(t);
        }
    }

    private int copyAggregatePropertiesRecursively(Node aggregate, boolean skipExistingProperties, CopyAggregatePropertiesStatistics copyStats,
                                                   Set<String> alreadySeen) {
        int startSize = alreadySeen.size();
        final String aggregateId = (String) aggregate.getProperty(PROP_ID);
        if (alreadySeen.contains(aggregateId))
            return 0;
        List<Node> elementAggregates = new ArrayList<>();
        // If the current aggregate has other aggregates as elements, those need to obtain their copied properties
        // first, so we can then use them for this aggregate.
        Iterable<Relationship> elementRels = aggregate.getRelationships(Direction.OUTGOING, ConceptEdgeTypes.HAS_ELEMENT);
        for (Relationship elementRel : elementRels) {
            Node endNode = elementRel.getEndNode();
            if (endNode.hasLabel(AGGREGATE) && !alreadySeen.contains(endNode))
                elementAggregates.add(endNode);
        }
        for (Node elementAggregate : elementAggregates) {
            copyAggregatePropertiesRecursively(elementAggregate, skipExistingProperties, copyStats, alreadySeen);
        }
        if (aggregate.hasProperty(PROP_COPY_PROPERTIES)) {
            String[] copyProperties = (String[]) aggregate.getProperty(PROP_COPY_PROPERTIES);
            ConceptAggregateManager.copyAggregateProperties(aggregate, skipExistingProperties, copyProperties, copyStats);
        }
        alreadySeen.add(aggregateId);
        return alreadySeen.size() - startSize;
    }

    public static class CopyAggregatePropertiesStatistics {
        public int numProperties = 0;
        public int numElements = 0;

        @Override
        public String toString() {
            return "CopyAggregatePropertiesStatistics [numProperties=" + numProperties + ", numElements=" + numElements
                    + "]";
        }

    }
}
