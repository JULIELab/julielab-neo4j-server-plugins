package de.julielab.neo4j.plugins;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.google.gson.stream.JsonReader;
import de.julielab.neo4j.plugins.FacetManager.FacetLabel;
import de.julielab.neo4j.plugins.auxiliaries.PropertyUtilities;
import de.julielab.neo4j.plugins.auxiliaries.semedico.*;
import de.julielab.neo4j.plugins.auxiliaries.semedico.ConceptAggregateBuilder.CopyAggregatePropertiesStatistics;
import de.julielab.neo4j.plugins.constants.semedico.SequenceConstants;
import de.julielab.neo4j.plugins.datarepresentation.*;
import de.julielab.neo4j.plugins.datarepresentation.constants.*;
import de.julielab.neo4j.plugins.datarepresentation.util.ConceptsJsonSerializer;
import de.julielab.neo4j.plugins.util.AggregateConceptInsertionException;
import de.julielab.neo4j.plugins.util.ConceptInsertionException;
import org.apache.commons.lang.StringUtils;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.*;
import org.neo4j.server.rest.repr.MappingRepresentation;
import org.neo4j.server.rest.repr.RecursiveMappingRepresentation;
import org.neo4j.server.rest.repr.Representation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static de.julielab.neo4j.plugins.ConceptManager.CM_REST_ENDPOINT;
import static de.julielab.neo4j.plugins.auxiliaries.PropertyUtilities.*;
import static de.julielab.neo4j.plugins.auxiliaries.semedico.NodeUtilities.getSourceIds;
import static de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants.*;
import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_ID;
import static java.util.stream.Collectors.joining;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@javax.ws.rs.Path("/" + CM_REST_ENDPOINT)
public class ConceptManager {

    public static final String CM_REST_ENDPOINT = "concept_manager";

    public static final String INSERT_MAPPINGS = "insert_mappings";
    public static final String BUILD_AGGREGATES_BY_NAME_AND_SYNONYMS = "build_aggregates_by_name_and_synonyms";
    public static final String BUILD_AGGREGATES_BY_MAPPINGS = "build_aggregates_by_mappings";
    public static final String DELETE_AGGREGATES = "delete_aggregates";
    public static final String COPY_AGGREGATE_PROPERTIES = "copy_aggregate_properties";
    public static final String GET_CHILDREN_OF_CONCEPTS = "get_children_of_concepts";
    public static final String GET_NUM_CONCEPTS = "get_num_concepts";
    public static final String GET_PATHS_FROM_FACETROOTS = "get_paths_to_facetroots";
    public static final String INSERT_CONCEPTS = "insert_concepts";
    public static final String GET_FACET_ROOTS = "get_facet_roots";
    public static final String ADD_CONCEPT_TERM = "add_concept_term";
    public static final String KEY_AMOUNT = "amount";
    public static final String KEY_CREATE_HOLLOW_PARENTS = "createHollowParents";
    public static final String KEY_FACET = "facet";
    public static final String KEY_FACET_ID = "facetId";
    public static final String KEY_FACET_IDS = "facetIds";
    public static final String KEY_FACET_PROP_KEY = "propertyKey";
    public static final String KEY_FACET_PROP_VALUE = "propertyValue";
    public static final String KEY_ID_PROPERTY = "id_property";
    public static final String KEY_RETURN_ID_PROPERTY = "return_id_property";
    public static final String KEY_IMPORT_OPTIONS = "importOptions";
    public static final String KEY_LABEL = "label";
    public static final String KEY_SORT_RESULT = "sortResult";
    public static final String KEY_CONCEPT_IDS = "conceptIds";
    public static final String KEY_MAX_ROOTS = "maxRoots";
    public static final String KEY_CONCEPT_PROP_KEY = "conceptPropertyKey";
    public static final String KEY_CONCEPT_PROP_VALUE = "conceptPropertyValue";
    public static final String KEY_CONCEPT_PROP_VALUES = "conceptPropertyValues";
    public static final String KEY_CONCEPT_PUSH_CMD = "conceptPushCommand";
    public static final String KEY_AGGREGATED_LABEL = "aggregatedLabel";
    public static final String KEY_ALLOWED_MAPPING_TYPES = "allowedMappingTypes";
    public static final String KEY_CONCEPT_TERMS = "conceptTerms";
    public static final String KEY_CONCEPT_ACRONYMS = "conceptAcronyms";
    /**
     * The key of the map to send to the {@link #INSERT_CONCEPTS} endpoint.
     */
    public static final String KEY_CONCEPTS = "concepts";
    public static final String KEY_TIME = "time";
    public static final String KEY_MAPPINGS = "mappings";
    public static final String POP_CONCEPTS_FROM_SET = "pop_concepts_from_set";
    public static final String PUSH_CONCEPTS_TO_SET = "push_concepts_to_set";
    public static final String RET_KEY_CHILDREN = "children";
    public static final String RET_KEY_NUM_AGGREGATES = "numAggregates";
    public static final String RET_KEY_NUM_CREATED_RELS = "numCreatedRelationships";
    public static final String RET_KEY_NUM_CREATED_CONCEPTS = "numCreatedConcepts";
    public static final String RET_KEY_NUM_ELEMENTS = "numElements";
    public static final String RET_KEY_NUM_PROPERTIES = "numProperties";
    public static final String RET_KEY_PATHS = "paths";
    public static final String RET_KEY_RELTYPES = "reltypes";
    public static final String RET_KEY_CONCEPTS = "concepts";

    public static final String FULLTEXT_INDEX_CONCEPTS = "concepts";

    public static final String UPDATE_CHILD_INFORMATION = "update_children_information";
    private static final String UNKNOWN_CONCEPT_SOURCE = "<unknown>";
    private static final Logger log = LoggerFactory.getLogger(ConceptManager.class);
    private static final int CONCEPT_INSERT_BATCH_SIZE = 10000;

    private static final String INDEX_SRC_IDS = "index_src_ids";

    private final DatabaseManagementService dbms;

    public ConceptManager(@Context DatabaseManagementService dbms) {
        this.dbms = dbms;
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
        if (!aggregate.hasLabel(ConceptLabel.AGGREGATE))
            throw new IllegalArgumentException(
                    "Node " + NodeUtilities.getNodePropertiesAsString(aggregate) + " is not an aggregate.");
        Iterable<Relationship> elementRels = aggregate.getRelationships(Direction.OUTGOING, EdgeTypes.HAS_ELEMENT);
        List<String> elementValues = new ArrayList<>();
        for (Relationship elementRel : elementRels) {
            String[] value = NodeUtilities.getNodePropertyAsStringArrayValue(elementRel.getEndNode(), property);
            for (int i = 0; value != null && i < value.length; i++)
                elementValues.add(value[i]);
        }
        return elementValues.isEmpty() ? null : elementValues.toArray(new String[elementValues.size()]);
    }

    public static void createIndexes(Transaction tx) {
        Indexes.createSinglePropertyIndexIfAbsent(tx, ConceptLabel.CONCEPT, true, ConceptConstants.PROP_ID);
        // The org ID can actually be duplicated. Only the composite (orgId,orgSource) should be unique but this isn't supported
        // by schema indexes it seems
        Indexes.createSinglePropertyIndexIfAbsent(tx, ConceptLabel.CONCEPT, false, ConceptConstants.PROP_ORG_ID);
        Indexes.createSinglePropertyIndexIfAbsent(tx, NodeConstants.Labels.ROOT, true, NodeConstants.PROP_NAME);
        FullTextIndexUtils.createTextIndex(tx, FULLTEXT_INDEX_CONCEPTS, Map.of("analyzer", "whitespace"), new Label[]{ConceptLabel.CONCEPT, ConceptLabel.HOLLOW}, new String[]{PROP_SRC_IDS});
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
     * @throws IOException When the JSON parameter cannot be read.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @javax.ws.rs.Path(BUILD_AGGREGATES_BY_MAPPINGS)
    public void buildAggregatesByMappings(String jsonParameterObject)
            throws IOException {
        ObjectMapper om = new ObjectMapper();
        Map<String, Object> parameterMap = om.readValue(jsonParameterObject, Map.class);
        final Set<String> allowedMappingTypes = new HashSet<>((List<String>) parameterMap.get(KEY_ALLOWED_MAPPING_TYPES));
        Label aggregatedConceptsLabel = Label.label((String) parameterMap.get(KEY_AGGREGATED_LABEL));
        Label allowedConceptLabel = parameterMap.containsKey(KEY_LABEL) ? Label.label((String) parameterMap.get(KEY_LABEL))
                : null;
        log.info("Creating mapping aggregates for concepts with label {} and mapping types {}", allowedConceptLabel,
                allowedMappingTypes);
        GraphDatabaseService graphDb = dbms.database(DEFAULT_DATABASE_NAME);
        try (Transaction tx = graphDb.beginTx()) {
            ConceptAggregateBuilder.buildAggregatesForMappings(tx, allowedMappingTypes, allowedConceptLabel,
                    aggregatedConceptsLabel);
            tx.commit();
        }
    }

    /**
     * <ul>
     *     <li>{@link #KEY_AGGREGATED_LABEL}: Label for concepts that have been processed by the aggregation algorithm.
     *     Such concepts can be aggregate concepts (with the label AGGREGATE) or just plain concepts
     *     (with the label CONCEPT) that are not an element of an aggregate.</li>
     * </ul>
     *
     * @param aggregatedConceptsLabelString
     */
    @DELETE
    @Consumes(MediaType.TEXT_PLAIN)
    @javax.ws.rs.Path(DELETE_AGGREGATES)
    public void deleteAggregatesByMappings(@QueryParam(KEY_AGGREGATED_LABEL) String aggregatedConceptsLabelString) {
        Label aggregatedConceptsLabel = Label.label(aggregatedConceptsLabelString);
        GraphDatabaseService graphDb = dbms.database(DEFAULT_DATABASE_NAME);
        try (Transaction tx = graphDb.beginTx()) {
            ConceptAggregateBuilder.deleteAggregates(tx, aggregatedConceptsLabel);
            tx.commit();
        }
    }

    @PUT
    @Produces(MediaType.APPLICATION_JSON)
    @javax.ws.rs.Path(COPY_AGGREGATE_PROPERTIES)
    public Representation copyAggregateProperties() {
        int numAggregates = 0;
        CopyAggregatePropertiesStatistics copyStats = new CopyAggregatePropertiesStatistics();
        GraphDatabaseService graphDb = dbms.database(DEFAULT_DATABASE_NAME);
        try (Transaction tx = graphDb.beginTx()) {
            try (ResourceIterator<Node> aggregateIt = tx.findNodes(ConceptLabel.AGGREGATE)) {
                while (aggregateIt.hasNext()) {
                    Node aggregate = aggregateIt.next();
                    numAggregates += copyAggregatePropertiesRecursively(aggregate, copyStats, new HashSet<>());
                }
            }
            tx.commit();
        }
        Map<String, Object> reportMap = new HashMap<>();
        reportMap.put(RET_KEY_NUM_AGGREGATES, numAggregates);
        reportMap.put(RET_KEY_NUM_ELEMENTS, copyStats.numElements);
        reportMap.put(RET_KEY_NUM_PROPERTIES, copyStats.numProperties);
        return new RecursiveMappingRepresentation(Representation.MAP, reportMap);
    }

    private int copyAggregatePropertiesRecursively(Node aggregate, CopyAggregatePropertiesStatistics copyStats,
                                                   Set<Node> alreadySeen) {
        if (alreadySeen.contains(aggregate))
            return 0;
        List<Node> elementAggregates = new ArrayList<>();
        Iterable<Relationship> elementRels = aggregate.getRelationships(Direction.OUTGOING, EdgeTypes.HAS_ELEMENT);
        for (Relationship elementRel : elementRels) {
            Node endNode = elementRel.getEndNode();
            if (endNode.hasLabel(ConceptLabel.AGGREGATE) && !alreadySeen.contains(endNode))
                elementAggregates.add(endNode);
        }
        for (Node elementAggregate : elementAggregates) {
            copyAggregatePropertiesRecursively(elementAggregate, copyStats, alreadySeen);
        }
        if (aggregate.hasProperty(PROP_COPY_PROPERTIES)) {
            String[] copyProperties = (String[]) aggregate.getProperty(PROP_COPY_PROPERTIES);
            ConceptAggregateBuilder.copyAggregateProperties(aggregate, copyProperties, copyStats);
        }
        alreadySeen.add(aggregate);
        return alreadySeen.size();
    }

    private void createRelationships(Transaction tx, List<ImportConcept> jsonConcepts, Node facet,
                                     CoordinatesMap nodesByCoordinates, ImportOptions importOptions, InsertionReport insertionReport) {
        log.info("Creating relationship between inserted concepts.");
//        Index<Node> idIndex = graphDb.index().forNodes(ConceptConstants.INDEX_NAME);
        String facetId = null;
        if (null != facet)
            facetId = (String) facet.getProperty(FacetConstants.PROP_ID);
        RelationshipType relBroaderThanInFacet = null;
        if (null != facet)
            relBroaderThanInFacet = RelationshipType.withName(EdgeTypes.IS_BROADER_THAN.toString() + "_" + facetId);
        AddToNonFacetGroupCommand noFacetCmd = importOptions.noFacetCmd;
        Node noFacet = null;
        int quarter = jsonConcepts.size() / 4;
        int numQuarter = 1;
        long totalTime = 0;
        long relCreationTime = 0;
        for (int i = 0; i < jsonConcepts.size(); i++) {
            long time = System.currentTimeMillis();
            ImportConcept jsonConcept = jsonConcepts.get(i);
            // aggregates may be included into the taxonomy, but by default they
            // are not
            if (jsonConcept.aggregate
                    && !jsonConcept.aggregateIncludeInHierarchy)
                continue;
            ConceptCoordinates coordinates = jsonConcept.coordinates;
            // Every concept must have a source ID...
            String srcId = coordinates.sourceId;
            // ...but it is not required to have a parent in its source.
            // Then, it's a facet root.
            Node concept = nodesByCoordinates.get(new ConceptCoordinates(coordinates));
            // Perhaps the concept was omitted on purpose?
            if (null == concept && insertionReport.omittedConcepts.contains(srcId))
                continue;
            if (null == concept) {
                throw new IllegalStateException("No node for source ID " + srcId
                        + " was created but the respective concept is included into the data for import and it is unknown why no node instance was created.");
            }
            // Default-relationships (taxonomical).
            {
                final List<ConceptCoordinates> parentCoordinateList = jsonConcept.parentCoordinates;
                if (parentCoordinateList != null && !parentCoordinateList.isEmpty()) {
                    for (ConceptCoordinates parentCoordinates : parentCoordinateList) {

                        String parentSrcId = parentCoordinates.sourceId;
                        if (importOptions.cutParents.contains(parentSrcId)) {
                            log.debug("Concept node " + coordinates
                                    + " has a parent that is marked to be cut away. Concept will be a facet root.");
                            createRelationshipIfNotExists(facet, concept, EdgeTypes.HAS_ROOT_CONCEPT, insertionReport);
                            continue;
                        }

                        // The concept has another concept as parent. Connect
                        // them. First check if the parent was included in the
                        // current import data
                        Node parent = nodesByCoordinates.get(parentCoordinates);
                        if (null == parent)
                            throw new IllegalStateException("The parent node of concept " + coordinates
                                    + " should have been created in the insertConcepts method before, but it is null. The parent coordinates are "
                                    + parentCoordinates);

                        // The parent was not in the imported data; check if it
                        // already exists in the database
                        // if (parent == null) {
                        // log.debug("Searching for parent concept to create
                        // hierarchical relationships");
                        // parent = lookupConcept(parentCoordinates, idIndex);
                        // }

                        // if (null != parent) {
                        if (insertionReport.importedCoordinates.contains(parentCoordinates)
                                || insertionReport.existingConcepts.contains(parent)) {
                            log.trace("Parent with " + parentCoordinates + " was found by source ID for concept "
                                    + coordinates + ".");
                            long creationTime = System.currentTimeMillis();
                            createRelationshipIfNotExists(parent, concept, EdgeTypes.IS_BROADER_THAN, insertionReport);
                            // Since a concept may appear in multiple facets, we
                            // connect concepts with a general taxonomic
                            // relation as well as a special relation only
                            // relevant to the
                            // particular structure‚ of the current facet.
                            createRelationshipIfNotExists(parent, concept, relBroaderThanInFacet, insertionReport);
                            relCreationTime += System.currentTimeMillis() - creationTime;
                        } else {
                            // If the parent is not found in nodesBySrcId it
                            // does not exist in the currently imported data nor
                            // in the database. If it would have existed in the
                            // database, we would have added it to the map in
                            // insertFacetConcept().
                            // TODO this approach fails completely with ontology
                            // imports: Imported classes are defined within the
                            // ontology neither is it clear, what the defining
                            // ontology will have as an ID (BioPortal). However,
                            // class IRIs have to be unique anyway. We need a
                            // mechanism where sources may be ignored. Think
                            // this
                            // through: When do we really know the source(s)?
                            // Are there cases where sourceIds are unique and
                            // other cases where they aren't? Then perhaps we
                            // need an option to allow "source-less" lookup
                            // explicitly.
                            log.debug("Concept with source ID \"" + srcId
                                    + "\" referenced the concept with source ID \"" + parentSrcId
                                    + "\" as its parent. However, that parent node does not exist.");

                            if (!importOptions.doNotCreateHollowParents) {
                                log.debug(
                                        "Creating hollow parents is switched on. The parent will be created with the label \""
                                                + ConceptLabel.HOLLOW + "\" and be connected to the facet root.");
                                // We create the parent as a "hollow" concept and
                                // connect it to the facet root. The latter
                                // is the only thing we can do because we can't
                                // get to know the
                                // parent's parent since it is not included in
                                // the data.
                                // Node hollowParent =
                                // registerNewHollowConceptNode(graphDb,
                                // parentCoordinates, idIndex,
                                // ConceptLabel.CONCEPT);
                                parent.addLabel(ConceptLabel.CONCEPT);
                                // nodesByCoordinates.put(parentCoordinates,
                                // hollowParent);
                                // insertionReport.numConcepts++;
                                createRelationshipIfNotExists(parent, concept, EdgeTypes.IS_BROADER_THAN,
                                        insertionReport);
                                createRelationshipIfNotExists(parent, concept, relBroaderThanInFacet, insertionReport);
                                createRelationshipIfNotExists(facet, parent, EdgeTypes.HAS_ROOT_CONCEPT,
                                        insertionReport);
                            } else {
                                log.warn(
                                        "Creating hollow parents is switched off. Hence the concept will be added as root concept for its facet (\""
                                                + facet.getProperty(FacetConstants.PROP_NAME) + "\").");
                                // Connect the concept as a root, it's the best we
                                // can
                                // do.
                                createRelationshipIfNotExists(facet, concept, EdgeTypes.HAS_ROOT_CONCEPT,
                                        insertionReport);
                            }
                        }
                        if (parent.hasLabel(ConceptLabel.AGGREGATE) && !parent.hasLabel(ConceptLabel.CONCEPT))
                            throw new IllegalArgumentException("Concept with source ID " + srcId
                                    + " specifies source ID " + parentSrcId
                                    + " as parent. This node is an aggregate but not a CONCEPT itself and thus is not included in the hierarchy and cannot be the conceptual parent of other concepts. To achieve this, import the aggregate with the property "
                                    + ConceptConstants.AGGREGATE_INCLUDE_IN_HIERARCHY
                                    + " set to true or build the aggregates in a way that assignes the CONCEPT label to them. The parent is "
                                    + NodeUtilities.getNodePropertiesAsString(parent)
                                    + " and has the following labels: "
                                    + StreamSupport.stream(parent.getLabels().spliterator(), false).map(Label::name)
                                    .collect(joining(", ")));
                    }

                } else {
                    if (noFacetCmd != null && noFacetCmd.getParentCriteria()
                            .contains(AddToNonFacetGroupCommand.ParentCriterium.NO_PARENT)) {
                        if (null == noFacet) {
                            noFacet = FacetManager.getNoFacet(tx, (String) facet.getProperty(PROP_ID));
                        }

                        createRelationshipIfNotExists(noFacet, concept, EdgeTypes.HAS_ROOT_CONCEPT, insertionReport);
                    } else if (null != facet) {
                        // This concept does not have a concept parent. It is a facet
                        // root,
                        // thus connect it to the facet node.
                        log.trace("Installing concept with source ID " + srcId + " (ID: " + concept.getProperty(PROP_ID)
                                + ") as root for facet " + facet.getProperty(NodeConstants.PROP_NAME) + "(ID: "
                                + facet.getProperty(PROP_ID) + ")");
                        createRelationshipIfNotExists(facet, concept, EdgeTypes.HAS_ROOT_CONCEPT, insertionReport);
                    }
                    // else: nothing, because the concept already existed, we are
                    // merely merging here.
                }
            }
            // Explicitly specified relationships (has-same-name-as,
            // is-mapped-to,
            // whatever...)
            {
                if (jsonConcept.relationships != null) {
                    log.info("Adding explicitly specified relationships");
                    for (ImportConceptRelationship jsonRelationship : jsonConcept.relationships) {
                        String rsTypeStr = jsonRelationship.type;
                        final ConceptCoordinates targetCoordinates = jsonRelationship.targetCoordinates;
                        Node target = lookupConcept(tx, targetCoordinates);
                        if (null == target) {
                            log.debug("Creating hollow relationship target with orig Id/orig source " + targetCoordinates);
                            target = registerNewHollowConceptNode(tx, targetCoordinates);
                        }
                        EdgeTypes type = EdgeTypes.valueOf(rsTypeStr);
                        Object[] properties = null;
                        if (jsonRelationship.properties != null) {
                            Set<String> propNames = jsonRelationship.properties.keySet();
                            properties = new Object[propNames.size() * 2];
                            int k = 0;
                            for (String propName : propNames) {
                                Object propValue = jsonRelationship.properties.get(propName);
                                properties[2 * k] = propName;
                                properties[2 * k + 1] = propValue;
                                ++k;
                            }
                        }
                        createRelationShipIfNotExists(concept, target, type, insertionReport, Direction.OUTGOING,
                                properties);
                        // concept.createRelationshipTo(target, type);
                        insertionReport.numRelationships++;
                    }
                }
            }
            totalTime += System.currentTimeMillis() - time;
            if (i >= numQuarter * quarter) {
                log.info("Finished " + (25 * numQuarter) + "% of concepts for relationship creation.");
                log.info("Relationship creation took " + relCreationTime + "ms.");
                log.info("Total time consumption for creation of " + insertionReport.numRelationships
                        + " relationships until now: " + totalTime + "ms.");
                numQuarter++;
            }
        }
        log.info("Finished 100% of concepts for relationship creation.");
    }

    /**
     * Creates a relationship of type <tt>type</tt> from <tt>source</tt> to
     * <tt>target</tt>, if this relationship does not already exist.
     *
     * @param source
     * @param target
     * @param type
     * @param insertionReport
     * @return
     */
    private Relationship createRelationshipIfNotExists(Node source, Node target, RelationshipType type,
                                                       InsertionReport insertionReport) {
        return createRelationShipIfNotExists(source, target, type, insertionReport, Direction.OUTGOING);
    }

    /**
     * Creates a relationship of type <tt>type</tt> from <tt>source</tt> to
     * <tt>target</tt>, if this relationship does not already exist.
     * <p>
     * The parameter <tt>direction</tt> may be used to deconceptine for which
     * direction there should be checked for an existing relationship, outgoing,
     * incoming or both. Note that the new relationship will <em>always</em> be
     * created from <tt>source</tt> to <tt>target</tt>, no matter for which
     * direction existing relationships should be checked.
     * </p>
     * <p>
     * If a relationship of type <tt>type</tt> already exists but has different
     * properties than specified by <tt>properties</tt>, it will be tried to merge
     * the properties instead of creating a new relationship.
     * </p>
     *
     * @param source
     * @param target
     * @param type
     * @param insertionReport
     * @param direction
     * @param properties      A sequence of property key and property values. These properties
     *                        will be used to deconceptine whether a relationship - with those
     *                        properties - already exists.
     * @return
     */
    private Relationship createRelationShipIfNotExists(Node source, Node target, RelationshipType type,
                                                       InsertionReport insertionReport, Direction direction, Object... properties) {
        if (null != properties && properties.length % 2 != 0)
            throw new IllegalArgumentException("Property list must contain of key/value pairs but its length was odd.");

        boolean relationShipExists = false;
        Relationship createdRelationship = null;
        if (insertionReport.relationshipAlreadyWasCreated(source, target, type)) {
            relationShipExists = true;
        } else if (insertionReport.existingConcepts.contains(source)
                && insertionReport.existingConcepts.contains(target)) {
            // Both concepts existing before the current processing call to
            // insert_concepts. Thus, we have to check whether
            // the relation already exists and cannot just use
            // insertionReport.relationshipAlreadyWasCreated()
            Iterable<Relationship> relationships = source.getRelationships(direction, type);
            for (Relationship relationship : relationships) {
                if (relationship.getEndNode().equals(target)) {
                    relationShipExists = true;
                    if (!PropertyUtilities.mergeProperties(relationship, properties))
                        relationShipExists = false;
                }
            }
        }
        if (!relationShipExists) {
            // The relationship does not exist. Create it.
            createdRelationship = source.createRelationshipTo(target, type);
            // Add the properties.
            for (int i = 0; null != properties && i < properties.length; i += 2) {
                String key = (String) properties[i];
                Object value = properties[i + 1];
                createdRelationship.setProperty(key, value);
            }
            insertionReport.addCreatedRelationship(source, target, type);
            insertionReport.numRelationships++;
        }
        return createdRelationship;
    }

    /**
     * <p>
     * Returns all non-hollow children of concepts identified via the  KEY_CONCEPT_IDS
     * parameter. The return format is a map from the children's id
     * to respective child concept. This endpoint has been created due
     * to performance reasons. All tried Cypher queries to achieve
     * the same behaviour were less performant (tested for version 2.0.0 M3).
     * </p>
     * <p>
     * Parameters:
     *     <ul>
     *         <li>{@link #KEY_CONCEPT_IDS}: Comma-separated list of concept IDs for which to return their children.</li>
     *         <li>{@link #KEY_LABEL}: The label against which the given concept IDs are resolved. Defaults to 'CONCEPT'.</li>
     *     </ul>
     * </p>
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @javax.ws.rs.Path(GET_CHILDREN_OF_CONCEPTS)
    public MappingRepresentation getChildrenOfConcepts(@QueryParam(KEY_CONCEPT_IDS) String conceptIdsCsv, @QueryParam(KEY_LABEL) String labelString) throws IOException {
        Label label = labelString != null ? Label.label(labelString) : ConceptLabel.CONCEPT;
        final List<String> conceptIds = Arrays.asList(conceptIdsCsv.split(","));
        GraphDatabaseService graphDb = dbms.database(DEFAULT_DATABASE_NAME);
        try (Transaction tx = graphDb.beginTx()) {
            Map<String, Object> childrenByConceptId = getChildrenOfConcepts(tx, conceptIds, label);
            return new RecursiveMappingRepresentation(Representation.MAP, childrenByConceptId);
        }
    }

    public Map<String, Object> getChildrenOfConcepts(Transaction tx, List<String> conceptIds, Label label) throws IOException {
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

    /**
     * Parameters:
     * <ul>
     *     <li>{@link #KEY_CONCEPT_IDS}: Array of root concept IDs to retrieve the paths from.</li>
     *     <li>{@link #KEY_SORT_RESULT}: Boolean indicator if the result paths should be sorted by length.</li>
     *     <li>{@link #KEY_FACET_ID}: Optional. The facet ID to restrict the root nodes to.</li>
     * </ul>
     *
     * @param conceptIdsCsv The concept Ids, separated with commas.
     * @param sort          Whether or not to sort the result paths by length.
     * @param facetId       Optional. A facet ID to restrict all paths to.
     * @return Paths from facet root concept nodes to the given concept nodes.
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @javax.ws.rs.Path(GET_PATHS_FROM_FACETROOTS)
    public Representation getPathsFromFacetRoots(@QueryParam(KEY_CONCEPT_IDS) String conceptIdsCsv, @QueryParam(KEY_ID_PROPERTY) String idProperty, @QueryParam(KEY_RETURN_ID_PROPERTY) String returnIdProperty, @QueryParam(KEY_SORT_RESULT) boolean sort, @QueryParam(KEY_FACET_ID) String facetId) {
        final List<String> conceptIds = Arrays.asList(conceptIdsCsv.split(","));

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
        GraphDatabaseService graphDb = dbms.database(DEFAULT_DATABASE_NAME);
        try (Transaction tx = graphDb.beginTx()) {
            RelationshipType relType = StringUtils.isBlank(facetId) ? ConceptManager.EdgeTypes.IS_BROADER_THAN
                    : RelationshipType.withName(ConceptManager.EdgeTypes.IS_BROADER_THAN.name() + "_" + facetId);
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
            return new RecursiveMappingRepresentation(Representation.MAP, pathsWrappedInMap);
        }
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
    private void insertAggregateConcept(Transaction tx, ImportConcept jsonConcept,
                                        CoordinatesMap nodesByCoordinates, InsertionReport insertionReport, ImportOptions importOptions)
            throws AggregateConceptInsertionException {
        try {
            ConceptCoordinates aggCoordinates = jsonConcept.coordinates != null ? jsonConcept.coordinates
                    : new ConceptCoordinates();
            String aggOrgId = aggCoordinates.originalId;
            String aggOrgSource = aggCoordinates.originalSource;
            String aggSrcId = aggCoordinates.sourceId;
            String aggSource = aggCoordinates.source;
            if (null == aggSource)
                aggSource = UNKNOWN_CONCEPT_SOURCE;
            log.trace("Looking up aggregate ({}, {}) / ({}, {}), original/source coordinates.", aggOrgId, aggOrgSource,
                    aggSrcId, aggSource);
            Node aggregate = lookupConcept(tx, aggCoordinates);
            if (null != aggregate) {
                String isHollowMessage = "";
                if (aggregate.hasLabel(ConceptLabel.HOLLOW))
                    isHollowMessage = ", however it is hollow and its properties will be set now.";
                log.trace("    aggregate does already exist {}", isHollowMessage);
                if (!aggregate.hasLabel(ConceptLabel.HOLLOW))
                    return;
                // remove the HOLLOW label, we have to aggregate information now and
                // will set it to the node in the following
                aggregate.removeLabel(ConceptLabel.HOLLOW);
                aggregate.addLabel(ConceptLabel.AGGREGATE);
            }
            if (aggregate == null) {
                log.trace("    aggregate is being created");
                aggregate = tx.createNode(ConceptLabel.AGGREGATE);
            }
            boolean includeAggreationInHierarchy = jsonConcept.aggregateIncludeInHierarchy;
            // If the aggregate is to be included into the hierarchy, it also should
            // be a CONCEPT for path creation
            if (includeAggreationInHierarchy)
                aggregate.addLabel(ConceptLabel.CONCEPT);
            List<ConceptCoordinates> elementCoords = jsonConcept.elementCoordinates;
            log.trace("    looking up aggregate elements");
            for (ConceptCoordinates elementCoordinates : elementCoords) {
                String elementSource = elementCoordinates.source;
                if (null == elementCoordinates.source)
                    elementSource = UNKNOWN_CONCEPT_SOURCE;
                Node element = nodesByCoordinates.get(elementCoordinates);
                if (null != element) {
                    String[] srcIds = getSourceIds(element);
                    String[] sources = element.hasProperty(PROP_SOURCES) ? (String[]) element.getProperty(PROP_SOURCES)
                            : new String[0];
                    for (int j = 0; j < srcIds.length; j++) {
                        String srcId = srcIds[j];
                        String source = sources.length > j ? sources[j] : null;
                        // If the source ID matches but not the sources then this is
                        // the wrong node.
                        if (srcId.equals(elementCoordinates.sourceId)
                                && !((elementSource == null && source == null) || (elementSource.equals(source))))
                            element = null;
                        else
                            break;
                    }
                    if (null != element)
                        log.trace("\tFound element with source ID and source ({}, {}) in in-memory map.", elementCoordinates.sourceId,
                                elementSource);
                }
                if (null == element)
                    element = lookupConceptBySourceId(tx, elementCoordinates.sourceId, elementSource, false);
                if (null == element && importOptions.createHollowAggregateElements) {
                    element = registerNewHollowConceptNode(tx, elementCoordinates);
                    log.trace("    Creating HOLLOW element with source coordinates ({}, {})", elementCoordinates.sourceId,
                            elementSource);
                }
                if (element != null) {
                    aggregate.createRelationshipTo(element, EdgeTypes.HAS_ELEMENT);
                }
            }

            // Set the aggregate's properties
            if (null != aggSrcId) {
                int idIndex = aggregate.hasProperty(PROP_SRC_IDS) ? Arrays.asList(getSourceIds(aggregate)).indexOf(aggSrcId) : -1;
                int sourceIndex = findFirstValueInArrayProperty(aggregate, PROP_SOURCES, aggSource);
                if (!StringUtils.isBlank(aggSrcId)
                        && ((idIndex == -1 && sourceIndex == -1) || (idIndex != sourceIndex))) {
                    String newSourceIdString = aggregate.hasProperty(PROP_SRC_IDS) ? aggregate.getProperty(PROP_SRC_IDS) + " " + aggSrcId : aggSrcId;
                    aggregate.setProperty(PROP_SRC_IDS, newSourceIdString);
                    addToArrayProperty(aggregate, PROP_SOURCES, aggSource, true);
                }
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
                aggregate.setProperty(ConceptConstants.PROP_COPY_PROPERTIES, copyProperties.toArray(new String[0]));

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

    private void insertConcept(Transaction tx, String facetId,
                               ImportConcept jsonConcept, CoordinatesMap nodesByCoordinates, InsertionReport insertionReport,
                               ImportOptions importOptions) {
        // Name is mandatory, thus we don't use the
        // null-convenience method here.
        String prefName = jsonConcept.prefName;
        List<String> synonyms = jsonConcept.synonyms;
        List<String> generalLabels = jsonConcept.generalLabels;

        ConceptCoordinates coordinates = jsonConcept.coordinates;

        if (coordinates == null)
            throw new IllegalArgumentException(
                    "The concept " + jsonConcept + " does not specify coordinates.");

        // Source ID is mandatory if we have a real concept import and not just a
        // merging operation.
        if (!importOptions.merge && coordinates.sourceId == null)
            throw new IllegalArgumentException("The concept " + jsonConcept + " does not specify a source ID.");
        // The other properties may have values or not, make it
        // null-proof.
        String srcId = coordinates.sourceId;
        String orgId = coordinates.originalId;
        String source = coordinates.source;
        String orgSource = coordinates.originalSource;
        boolean uniqueSourceId = coordinates.uniqueSourceId;

        boolean srcIduniqueMarkerChanged = false;

        if (StringUtils.isBlank(srcId) && !StringUtils.isBlank(orgId)
                && ((StringUtils.isBlank(source) && !StringUtils.isBlank(orgSource)) || source.equals(orgSource))) {
            srcId = orgId;
            source = orgSource;
        }

        if (StringUtils.isBlank(source))
            source = UNKNOWN_CONCEPT_SOURCE;

        if (StringUtils.isBlank(orgId) ^ StringUtils.isBlank(orgSource))
            throw new IllegalArgumentException(
                    "Concept to be inserted defines only its original ID or its original source but not both. This is not allowed. The concept data was: "
                            + jsonConcept);
        if (importOptions.merge && jsonConcept.parentCoordinates != null && !jsonConcept.parentCoordinates.isEmpty())
            // The problem is that we use the nodeBySrcId map to check whether
            // relationships have to be created or not.
            // Thus, for relationships we need source IDs. Could be adapted in
            // the future to switch to original IDs if
            // concepts do not come with a source ID.
            throw new IllegalArgumentException("Concept " + jsonConcept
                    + " is supposed to be merged with an existing database concept but defines parents. This is currently not supported in merging mode.");

        // The concept node does already exist by now, it has either been
        // retrieved from the database or created HOLLOW by the concept
        // insertion method calling this method
        Node concept = nodesByCoordinates.get(coordinates);
        if (concept == null && !importOptions.merge)
            throw new IllegalStateException("No concept node was found or created for import concept with coordinates "
                    + coordinates + " and this is not a merging operation.");
        else if (concept == null)
            // we are in merging mode, for nodes we don't know we just do
            // nothing
            return;
        if (concept.hasLabel(ConceptLabel.HOLLOW)) {
            log.trace("Got HOLLOW concept node with coordinates " + coordinates + " and will create full concept.");
            concept.removeLabel(ConceptLabel.HOLLOW);
            concept.addLabel(ConceptLabel.CONCEPT);
            Iterable<Relationship> relationships = concept.getRelationships(EdgeTypes.HAS_ROOT_CONCEPT);
            for (Relationship rel : relationships) {
                Node startNode = rel.getStartNode();
                if (startNode.hasLabel(FacetManager.FacetLabel.FACET))
                    rel.delete();
            }
            String conceptId = NodeIDPrefixConstants.TERM
                    + SequenceManager.getNextSequenceValue(tx, SequenceConstants.SEQ_TERM);
            concept.setProperty(PROP_ID, conceptId);
        }

        // Merge the new or an already existing concept with what we
        // already have, perhaps the stored information
        // and the new information is complementary to each other
        // (if there is any information already stored, the concept could be
        // fresh and empty).
        // Currently, just do the following: For non-array property
        // values, set those properties which are currently non
        // existent. For array, merge the arrays.
        if (!StringUtils.isBlank(coordinates.originalId) && !concept.hasProperty(PROP_ORG_ID)) {
            concept.setProperty(PROP_ORG_ID, coordinates.originalId);
            concept.setProperty(PROP_ORG_SRC, coordinates.originalSource);
        }
        PropertyUtilities.setNonNullNodeProperty(concept, PROP_PREF_NAME, jsonConcept.prefName);
        PropertyUtilities.mergeArrayProperty(concept, PROP_DESCRIPTIONS, () -> jsonConcept.descriptions.toArray(new String[0]));
        PropertyUtilities.mergeArrayProperty(concept, PROP_WRITING_VARIANTS, () -> jsonConcept.writingVariants.toArray(new String[0]));
        PropertyUtilities.mergeArrayProperty(concept, PROP_COPY_PROPERTIES, () -> jsonConcept.copyProperties.toArray(new String[0]));
        mergeArrayProperty(concept, ConceptConstants.PROP_SYNONYMS, synonyms.stream().filter(s -> !s.equals(prefName)).toArray());
        addToArrayProperty(concept, PROP_FACETS, facetId);

        // There could be multiple sources containing a concept. For
        // now, we just note that facet (if these sources give the same original
        // ID, otherwise we won't notice) but don't do anything about
        // it. In the future, it could be interesting to link back to the
        // different sources, but this requires quite some more modeling. At
        // least parallel arrays of source IDs and addresses of sources
        // themselves (in a property
        // of their own). Or the sources will be nodes and have
        // relationships to the concepts they contain.
        // Check, if the parallel pair of source ID and source already exists.
        // If not, insert it. Unless a source ID
        // wasn't specified.

        // The source IDs are stored as whitespace-delimited string. The reason is that this allows us to use a
        // full text index on the source ID property.
        List<String> presentSourceIds = Arrays.asList(getSourceIds(concept));
        int sourceIndex = findFirstValueInArrayProperty(concept, PROP_SOURCES, source);
        int idIndex = presentSourceIds.indexOf(srcId);
        if (!StringUtils.isBlank(srcId) && ((idIndex == -1 && sourceIndex == -1) || (idIndex != sourceIndex))) {
            // on first creation, no concept node has a source ID at this point
            if (concept.hasProperty(PROP_SRC_IDS))
                srcIduniqueMarkerChanged = checkUniqueIdMarkerClash(concept, srcId, uniqueSourceId);
//            addToArrayProperty(concept, PROP_SRC_IDS, srcId, true);
            concept.setProperty(PROP_SRC_IDS, concept.getProperty(PROP_SRC_IDS) + " " + srcId);
            addToArrayProperty(concept, PROP_SOURCES, source, true);
            addToArrayProperty(concept, PROP_UNIQUE_SRC_ID, uniqueSourceId, true);
        }

        for (int i = 0; null != generalLabels && i < generalLabels.size(); i++) {
            concept.addLabel(Label.label(generalLabels.get(i)));
        }

        if (srcIduniqueMarkerChanged) {
            log.warn("Merging concept nodes with unique source ID " + srcId
                    + " because on concept with this source ID and source " + source
                    + " the ID was declared non-unique in the past but unique now. Properties from all nodes are merged together and relationships are moved from obsolete nodes to the single remaining node. This is experimental and might lead to errors.");
            List<Node> obsoleteNodes = new ArrayList<>();
            Node mergedNode = NodeUtilities.mergeConceptNodesWithUniqueSourceId(tx, srcId, obsoleteNodes);
            // now move the relationships of all nodes to be removed to the
            // merged node
            for (Node obsoleteNode : obsoleteNodes) {
                Iterable<Relationship> relationships = obsoleteNode.getRelationships();
                for (Relationship rel : relationships) {
                    Node startNode = rel.getStartNode();
                    Node endNode = rel.getEndNode();
                    // replace the obsolete node by the merged node for the new
                    // relationships
                    if (startNode.getId() == obsoleteNode.getId())
                        startNode = mergedNode;
                    if (endNode.getId() == obsoleteNode.getId())
                        endNode = mergedNode;
                    // create the new relationship between the merged node and
                    // the other nodes the obsolete node is connected with
                    createRelationShipIfNotExists(startNode, endNode, rel.getType(), insertionReport,
                            Direction.OUTGOING, rel.getAllProperties());
                    // delete the original relationship
                    rel.delete();
                    // and finally, delete the obsolete node
                    obsoleteNode.delete();
                }
            }
        }

        if (StringUtils.isBlank(prefName) && !insertionReport.existingConcepts.contains(concept))
            throw new IllegalArgumentException("Concept has no property \"" + PROP_PREF_NAME + "\": " + jsonConcept);

    }

    private boolean checkUniqueIdMarkerClash(Node conceptNode, String srcId, boolean uniqueSourceId) {
        boolean uniqueOnConcept = NodeUtilities.isSourceUnique(conceptNode, srcId);
        // case: the source ID was already set on this concept node and
        // uniqueSourceId was
        // false; then, other concepts might have been inserted with
        // the same source ID marked as unique, but would not have been merged
        // since this concept marks its source ID as not unique (the rule says
        // that then the concept differ). But now the
        // same source ID will be marked as unique which would cause an
        // inconsistent database state because then, the formerly imported
        // concepts with the same unique source ID should have been merged
        if (!uniqueOnConcept && uniqueOnConcept != uniqueSourceId)
            return true;
        return false;
    }

    /**
     * A few things to realize:
     * <ul>
     * <li>Referenced concepts - parents, elements of aggregates, targets of
     * explicitly specified concept nodes - are not required to be included in the
     * same import data as the referencing concept. Then, the referee will be
     * realized as a HOLLOW node.</li>
     * <li>For non-aggregate concepts, we use the
     * {@link #createRelationShipIfNotExists(Node, Node, RelationshipType, InsertionReport, Direction, Object...)}
     * method that is sped up by knowing if the two input nodes for the relationship
     * did exist before the current import. Because if not, then they cannot have
     * had a relationship before. The method will make errors if this information is
     * wrong, causing missing relationships</li>
     * <li>Thus, all concept nodes that might be used in this method and that
     * existed before the current import, must be set so in the
     * <code>importOptions</code> parameter.</li>
     * <li>These concept nodes are:
     * <ul>
     * <li>The imported concept nodes themselves</li>
     * <li>Their parents</li>
     * </ul>
     * </li>
     *
     * </ul>
     *
     * @param tx
     * @param jsonConcepts
     * @param facetId
     * @param nodesByCoordinates
     * @param importOptions
     * @return
     * @throws AggregateConceptInsertionException
     */
    private InsertionReport insertConcepts(Transaction tx, List<ImportConcept> jsonConcepts, String facetId,
                                           CoordinatesMap nodesByCoordinates, ImportOptions importOptions) throws ConceptInsertionException {
        long time = System.currentTimeMillis();
        InsertionReport insertionReport = new InsertionReport();
        // Idea: First create all nodes and just store which Node has which
        // parent. Then, after all nodes have been created, do the actual
        // connection.

        // this MUST be a TreeSort or at least some collection using the
        // Comparable interface because ConceptCoordinates are rather
        // complicated regarding equality
        CoordinatesSet toBeCreated = new CoordinatesSet();
        // First, iterate through all concepts and check if their parents
        // already exist, before any nodes are created (for more efficient
        // relationship creation).

        // When merging, we don't care about parents.
        if (!importOptions.merge) {
            for (ImportConcept jsonConcept : jsonConcepts) {
                if (jsonConcept.parentCoordinates != null) {
                    for (ConceptCoordinates parentCoordinates : jsonConcept.parentCoordinates) {
                        Node parentNode = lookupConcept(tx, parentCoordinates);
                        if (parentNode != null) {
                            insertionReport.addExistingConcept(parentNode);
                            nodesByCoordinates.put(parentCoordinates, parentNode);
                        } else {
                            toBeCreated.add(parentCoordinates);
                        }
                    }
                }
            }
        }
        // Finished finding parents

        // When merging, we remove those import concepts that are not known in
        // the database from the input data
        List<Integer> importConceptsToRemove = new ArrayList<>();
        // Second, iterate through all concepts to be imported and check if
        // they already exist themselves or not. Not existing nodes will be
        // created as
        // HOLLOW nodes.
        // The following methods can then just access the nodes by their source
        // Id which ought to be unique for each import.
        for (int i = 0; i < jsonConcepts.size(); i++) {
            ImportConcept jsonConcept = jsonConcepts.get(i);
            ConceptCoordinates coordinates;
            if (jsonConcept.coordinates != null) {
                coordinates = jsonConcept.coordinates;
                insertionReport.addImportedCoordinates(coordinates);
            } else if (!jsonConcept.aggregate) {
                throw new IllegalArgumentException("Concept " + jsonConcept + " does not define concept coordinates.");
            } else {
                continue;
            }
            // many nodes will actually already have been seen as parents
            // above
            if (nodesByCoordinates.containsKey(coordinates) || toBeCreated.contains(coordinates, true))
                continue;
            Node conceptNode = lookupConcept(tx, coordinates);
            if (conceptNode != null) {
                insertionReport.addExistingConcept(conceptNode);
                nodesByCoordinates.put(coordinates, conceptNode);
            } else if (!importOptions.merge) {
                // When merging, we don't create new concepts

                // The concept coordinates are not yet known, create an
                // empty
                // concept node with its coordinates.
                // Node newConcept = registerNewHollowConceptNode(graphDb,
                // coordinates, conceptIndex);
                toBeCreated.add(coordinates);

                // conceptNode = newConcept;
            } else {
                // We are in merging mode and requested concept is not in the
                // database; mark it for removal from the input data and
                // continue
                importConceptsToRemove.add(i);
                continue;
            }

        }
        // Finished getting existing nodes and creating HOLLOW nodes
        for (ConceptCoordinates coordinates : toBeCreated) {
            Node conceptNode = registerNewHollowConceptNode(tx, coordinates);
            ++insertionReport.numConcepts;

            nodesByCoordinates.put(coordinates, conceptNode);
        }

        if (!importConceptsToRemove.isEmpty())
            log.info("removing " + importConceptsToRemove.size()
                    + " input concepts that should be omitted because we are merging and don't have them in the database");
        for (int index = importConceptsToRemove.size() - 1; index >= 0; --index)
            jsonConcepts.remove(importConceptsToRemove.get(index));

        log.info("Starting to insert " + jsonConcepts.size() + " concepts.");
        for (ImportConcept jsonConcept : jsonConcepts) {
            boolean isAggregate = jsonConcept.aggregate;
            if (isAggregate) {
                insertAggregateConcept(tx, jsonConcept, nodesByCoordinates, insertionReport,
                        importOptions);
            } else {
                insertConcept(tx, facetId, jsonConcept, nodesByCoordinates, insertionReport,
                        importOptions);
            }
        }
        log.debug(jsonConcepts.size() + " concepts inserted.");
        time = System.currentTimeMillis() - time;
        log.info(insertionReport.numConcepts
                + " new concepts - but not yet relationships - have been inserted. This took " + time + " ms ("
                + (time / 1000) + " s)");
        return insertionReport;
    }

    /**
     * Creates a node with the {@link ConceptLabel#HOLLOW} label, sets the given
     * coordinates and adds the node to the index.
     *
     * @param tx
     * @param coordinates
     * @return
     */
    private Node registerNewHollowConceptNode(Transaction tx, ConceptCoordinates coordinates,
                                              Label... additionalLabels) {
        Node node = tx.createNode(ConceptLabel.HOLLOW);
        for (Label label : additionalLabels) {
            node.addLabel(label);
        }
        log.trace("Created new HOLLOW concept node for coordinates {}", coordinates);
        if (!StringUtils.isBlank(coordinates.originalId)) {
            node.setProperty(PROP_ORG_ID, coordinates.originalId);
            node.setProperty(PROP_ORG_SRC, coordinates.originalSource);
        }
        node.setProperty(PROP_SRC_IDS, coordinates.sourceId);
        node.setProperty(PROP_SOURCES, new String[]{coordinates.source});
        node.setProperty(PROP_UNIQUE_SRC_ID, new boolean[]{coordinates.uniqueSourceId});

        return node;
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @javax.ws.rs.Path(INSERT_CONCEPTS)
    public Object insertConcepts(String jsonParameterObject)
            throws ConceptInsertionException, IOException {
        try {
            log.info("{} was called", INSERT_CONCEPTS);
            long time = System.currentTimeMillis();

            final ImportConcepts importConcepts = ConceptsJsonSerializer.fromJson(jsonParameterObject, ImportConcepts.class);
            ImportFacet jsonFacet = importConcepts.getFacet();
            List<ImportConcept> jsonConcepts = importConcepts.getConcepts();
            log.info("Got {} input concepts for import.", jsonConcepts != null ? jsonConcepts.size() : 0);
            ImportOptions importOptions = importConcepts.getImportOptions() != null ? importConcepts.getImportOptions() : new ImportOptions();

            Map<String, Object> report = new HashMap<>();
            InsertionReport insertionReport = new InsertionReport();
            log.debug("Beginning processing of concept insertion.");
            GraphDatabaseService graphDb = dbms.database(DEFAULT_DATABASE_NAME);
            try (Transaction tx = graphDb.beginTx()) {
                Node facet = null;
                String facetId = null;
                // The facet Id will be added to the facets-property of the concept
                // nodes.
                log.debug("Handling import of facet.");
                if (null != jsonFacet && jsonFacet.getId() != null) {
                    facetId = jsonFacet.getId();
                    log.info("Facet ID {} has been given to add the concepts to.", facetId);
                    boolean isNoFacet = jsonFacet.isNoFacet();
                    if (isNoFacet)
                        facet = FacetManager.getNoFacet(tx, facetId);
                    else
                        facet = FacetManager.getFacetNode(tx, facetId);
                    if (null == facet)
                        throw new IllegalArgumentException("The facet with ID \"" + facetId
                                + "\" was not found. You must pass the ID of an existing facet or deliver all information required to create the facet from scratch. Then, the facetId must not be included in the request, it will be created dynamically.");
                } else if (null != jsonFacet && jsonFacet.getName() != null) {
                    ResourceIterator<Node> facetIterator = tx.findNodes(FacetLabel.FACET);
                    while (facetIterator.hasNext()) {
                        facet = facetIterator.next();
                        if (facet.getProperty(FacetConstants.PROP_NAME)
                                .equals(jsonFacet.getName()))
                            break;
                        facet = null;
                    }

                }
                if (null != jsonFacet && null == facet) {
                    // No existing ID is given, create a new facet.
                    facet = FacetManager.createFacet(tx, jsonFacet);
                }
                if (null != facet) {
                    facetId = (String) facet.getProperty(PROP_ID);
                    log.debug("Facet {} was successfully created or determined by ID.", facetId);
                } else {
                    log.debug(
                            "No facet was specified for this import. This is currently equivalent to specifying the merge import option, i.e. concept properties will be merged but no new nodes or relationships will be created.");
                    importOptions.merge = true;
                }

                if (null != jsonConcepts) {
                    log.debug("Beginning to create concept nodes and relationships.");
                    CoordinatesMap nodesByCoordinates = new CoordinatesMap();
                    insertionReport = insertConcepts(tx, jsonConcepts, facetId, nodesByCoordinates, importOptions);
                    // If the nodesBySrcId map is empty we either have no concepts or
                    // at least no concepts with a source ID. Then,
                    // relationship creation is currently not supported.
                    if (!nodesByCoordinates.isEmpty() && !importOptions.merge)
                        createRelationships(tx, jsonConcepts, facet, nodesByCoordinates, importOptions,
                                insertionReport);
                    else
                        log.info("This is a property merging import, no relationships are created.");
                    report.put(RET_KEY_NUM_CREATED_CONCEPTS, insertionReport.numConcepts);
                    report.put(RET_KEY_NUM_CREATED_RELS, insertionReport.numRelationships);
                    log.debug("Done creating concepts and relationships.");
                } else {
                    log.info("No concepts were included in the request.");
                }

                time = System.currentTimeMillis() - time;
                report.put(KEY_TIME, time);
                report.put(KEY_FACET_ID, facetId);
                tx.commit();
            }
            log.info("Concept insertion complete.");
            log.info(INSERT_CONCEPTS + " is finished processing after " + time + " ms. " + insertionReport.numConcepts
                    + " concepts and " + insertionReport.numRelationships + " relationships have been created.");
            return Response.ok(report).build();
        } catch (Throwable throwable) {
            return Response.serverError().entity(throwable.getMessage() != null ? throwable.getMessage() : throwable).build();
        }
    }

    /**
     * RULE: Two concepts are equal, iff they have the same original source ID
     * assigned from the same original source or both have no contradicting original
     * ID and original source but the same source ID and source. Contradicting means
     * two non-null values that are not equal.
     *
     * @param coordinates The coordinates of the concept to find.
     * @return The node corresponding to the given coordinates or null, if none is found.
     */
    private Node lookupConcept(Transaction tx, ConceptCoordinates coordinates) {
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
        Node concept;
        // Do we know the original ID?
        concept = null != orgId ? tx.findNode(ConceptLabel.CONCEPT, PROP_ORG_ID, orgId) : null;
        if (concept != null)
            log.trace("Found concept by original ID {}", orgId);
        // 1. Check if there is a concept with the given original ID and a matching
        // original source.
        if (null != concept) {
            if (!PropertyUtilities.hasSamePropertyValue(concept, PROP_ORG_SRC, orgSource)) {
                log.trace("Original source doesn't match; requested: {}, found concept has: {}", orgSource,
                        NodeUtilities.getString(concept, PROP_ORG_SRC));
                concept = null;
            } else {
                log.trace("Found existing concept for original ID {} and original source {}", orgId, orgSource);
            }
        }
        // 2. If there was no original ID, check for a concept with the same source
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
     * @param tx
     * @param srcId          The source ID of the requested concept node.
     * @param source         The source in which the concept node should be given
     *                       <tt>srcId</tt> as a source ID.
     * @param uniqueSourceId Whether the ID should be unique, independently from the source.
     *                       This holds, for example, for ontology class IRIs.
     * @return The requested concept node or <tt>null</tt> if no such node is found.
     */
    private Node lookupConceptBySourceId(Transaction tx, String srcId, String source, boolean uniqueSourceId) {
        log.trace("Trying to look up existing concept by source ID and source ({}, {})", srcId, source);
        ResourceIterator<Object> indexHits = FullTextIndexUtils.getNodes(tx, FULLTEXT_INDEX_CONCEPTS, PROP_SRC_IDS, srcId);
        if (!indexHits.hasNext())
            log.trace("    Did not find any concept with source ID {}", srcId);

        Node soughtConcept = null;
        boolean uniqueSourceIdNodeFound = false;

        while (indexHits.hasNext()) {
            Node conceptNode = (Node) indexHits.next();
            if (null != conceptNode) {
                // The rule goes as follows: Two concepts that share a source ID
                // which is marked as being unique on both concepts are equal. If
                // on at least one concept the source ID is not marked as
                // unique, the concepts are different.
                if (uniqueSourceId) {
                    boolean uniqueOnConceptNode = NodeUtilities.isSourceUnique(conceptNode, srcId);
                    if (uniqueOnConceptNode) {
                        if (soughtConcept == null)
                            soughtConcept = conceptNode;
                        else if (uniqueSourceIdNodeFound == true)
                            throw new IllegalStateException("There are multiple concept nodes with unique source ID "
                                    + srcId
                                    + ". This means that some sources define the ID as unique and others not. This can lead to an inconsistent database as happened in this case.");
                        log.trace(
                                "    Found existing concept with unique source ID {} which matches given unique source ID",
                                srcId);
                        uniqueSourceIdNodeFound = true;
                    }
                }

                Set<String> sources = NodeUtilities.getSourcesForSourceId(conceptNode, srcId);
                if (!sources.contains(source)) {
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
        return soughtConcept;
    }


    /**
     * Updates - or creates - the information which concept has children in which facets.
     * This information is used in Semedico to either render an 'opening' arrow next to
     * a concept to display its children, or no 'drill-down' option depending on whether
     * the concept in question has children in the facet it is shown in or not.
     */
    @POST
    @javax.ws.rs.Path(UPDATE_CHILD_INFORMATION)
    public void updateChildrenInformation() {
        GraphDatabaseService graphDb = dbms.database(DEFAULT_DATABASE_NAME);
        try (Transaction tx = graphDb.beginTx()) {
            ResourceIterator<Node> conceptIt = tx.findNodes(ConceptLabel.CONCEPT);
            while (conceptIt.hasNext()) {
                Node concept = conceptIt.next();
                Iterator<Relationship> relIt = concept.getRelationships(Direction.OUTGOING).iterator();
                Set<String> facetsContainingChildren = new HashSet<>();
                while (relIt.hasNext()) {
                    Relationship rel = relIt.next();
                    String type = rel.getType().name();
                    if (type.startsWith(EdgeTypes.IS_BROADER_THAN.toString())) {
                        String[] typeNameParts = type.split("_");
                        String lastPart = typeNameParts[typeNameParts.length - 1];
                        if (lastPart.startsWith(NodeIDPrefixConstants.FACET)) {
                            facetsContainingChildren.add(lastPart);
                        }
                    }
                }
                if (facetsContainingChildren.size() == 0 && concept.hasProperty(PROP_CHILDREN_IN_FACETS))
                    concept.removeProperty(PROP_CHILDREN_IN_FACETS);
                else if (facetsContainingChildren.size() > 0)
                    concept.setProperty(PROP_CHILDREN_IN_FACETS,
                            facetsContainingChildren.toArray(new String[facetsContainingChildren.size()]));
            }
            tx.commit();
        }
    }

    /**
     * <p>
     * Adds a set of concept mappings to the database. Here, a 'mapping'
     * between two concepts means that those concepts are 'similar' to one
     * another. The actual similarity - e.g. 'equal' or 'related' - is
     * defined by the type of the mapping. Here, all mappings are interpreted
     * as being symmetric. That does not mean that two relationships are created
     * but that reading commands don't care about the relationship direction.
     * </p>
     * <p>
     * Parameter: {@link #KEY_MAPPINGS}: An array of mappings in JSON format. Each mapping is an object with the keys for "id1", "id2" and "mappingType", respectively.
     * </p>
     *
     * @param mappingsJson The mappings in JSON format.
     * @return The number of insertes mappings.
     * @throws IOException If the input JSON cannot be read.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @javax.ws.rs.Path(INSERT_MAPPINGS)
    public int insertMappings(String mappingsJson) throws IOException {
        final ObjectMapper om = new ObjectMapper();
        final List<Map<String, String>> mappings = om.readValue(mappingsJson, new TypeReference<List<Map<String, String>>>() {
        });
        log.info("Starting to insert " + mappings.size() + " mappings.");
        GraphDatabaseService graphDb = dbms.database(DEFAULT_DATABASE_NAME);
        try (Transaction tx = graphDb.beginTx()) {
            Map<String, Node> nodesBySrcId = new HashMap<>(mappings.size());
            InsertionReport insertionReport = new InsertionReport();

            for (Map<String, String> mapping : mappings) {
                String id1 = mapping.get("id1");
                String id2 = mapping.get("id2");
                String mappingType = mapping.get("mappingType");

                log.debug("Inserting mapping " + id1 + " -" + mappingType + "- " + id2);

                if (StringUtils.isBlank(id1))
                    throw new IllegalArgumentException("id1 in mapping \"" + mapping + "\" is missing.");
                if (StringUtils.isBlank(id2))
                    throw new IllegalArgumentException("id2 in mapping \"" + mapping + "\" is missing.");
                if (StringUtils.isBlank(mappingType))
                    throw new IllegalArgumentException("mappingType in mapping \"" + mapping + "\" is missing.");

                Node n1 = nodesBySrcId.get(id1);
                if (null == n1) {

                    ResourceIterator<Object> indexHits = FullTextIndexUtils.getNodes(tx, FULLTEXT_INDEX_CONCEPTS, PROP_SRC_IDS, id1);
                    if (indexHits.hasNext())
                        n1 = (Node) indexHits.next();
                    if (indexHits.hasNext()) {
                        log.error("More than one node for source ID {}", id1);
                        while (indexHits.hasNext())
                            log.error(NodeUtilities.getNodePropertiesAsString((Entity) indexHits.next()));
                        throw new IllegalStateException("More than one node for source ID " + id1);
                    }
                    if (null == n1) {
                        log.warn("There is no concept with source ID \"" + id1 + "\" as required by the mapping \""
                                + mapping + "\" Mapping is skipped.");
                        continue;
                    }
                    nodesBySrcId.put(id1, n1);
                }
                Node n2 = nodesBySrcId.get(id2);
                if (null == n2) {
                    n2 = FullTextIndexUtils.getNode(tx, FULLTEXT_INDEX_CONCEPTS, PROP_SRC_IDS, id2);
                    if (null == n2) {
                        log.warn("There is no concept with source ID \"" + id2 + "\" as required by the mapping \""
                                + mapping + "\" Mapping is skipped.");
                        continue;
                    }
                    nodesBySrcId.put(id2, n2);
                }
                if (mappingType.equalsIgnoreCase("LOOM")) {
                    // Exclude mappings that map classes within the same
                    // ontology. LOOM as delivered from BioPortal does this but
                    // all I saw were errors.
                    String[] n1Facets = (String[]) n1.getProperty(PROP_FACETS);
                    String[] n2Facets = (String[]) n2.getProperty(PROP_FACETS);
                    Set<String> n1FacetSet = new HashSet<>();
                    Set<String> n2FacetSet = new HashSet<>();
                    n1FacetSet.addAll(Arrays.asList(n1Facets));
                    Collections.addAll(n2FacetSet, n2Facets);
                    if (!Sets.intersection(n1FacetSet, n2FacetSet).isEmpty()) {
                        // Of course an ontology might contain two equivalent
                        // classes; possible they are even asserted to be equal.
                        // But this is nothing LOOM would detect.
                        log.debug("Omitting LOOM mapping between " + id1 + " and " + id2
                                + " because both concepts appear in the same conceptinology. We assume that the conceptinology does not have two equal concepts and that LOOM is wrong here.");
                        continue;
                    }
                }
                insertionReport.addExistingConcept(n1);
                insertionReport.addExistingConcept(n2);
                createRelationShipIfNotExists(n1, n2, EdgeTypes.IS_MAPPED_TO, insertionReport, Direction.BOTH,
                        ConceptRelationConstants.PROP_MAPPING_TYPE, new String[]{mappingType});
            }
            tx.commit();
            log.info(insertionReport.numRelationships + " of " + mappings.size()
                    + " new mappings successfully added.");
            return insertionReport.numRelationships;
        }
    }

    /**
     * <p>
     * Returns root concepts for the facets with specified IDs. Can also be restricted to particular roots which is useful for facets that have a lot of roots.
     * </p>
     * <p>
     * Parameters:
     *     <ul>
     *         <li>{@link #KEY_FACET_IDS}: "An array of facet IDs in CSV format.</li>
     *         <li>{@link #KEY_CONCEPT_IDS}: "An array of concept IDs to restrict the retrieval to in CSV format.</li>
     *         <li>{@link #KEY_MAX_ROOTS}: "Restricts the facets to those that have at most the specified number of roots.</li>
     *     </ul>
     * </p>
     *
     * @return
     * @throws IOException
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @javax.ws.rs.Path(GET_FACET_ROOTS)
    public MappingRepresentation getFacetRoots(@Context UriInfo uriInfo) {
        GraphDatabaseService graphDb = dbms.database(DEFAULT_DATABASE_NAME);
        try (Transaction tx = graphDb.beginTx()) {
            Set<String> requestedFacetId = new HashSet<>();
            MultivaluedMap<String, String> queryParameters = uriInfo.getQueryParameters();
            Map<String, Set<String>> requestedConceptIds = new HashMap<>();
            int maxRoots = 0;
            for (String param : queryParameters.keySet()) {
                if (param.equals(KEY_FACET_IDS))
                    Stream.of(queryParameters.getFirst(param).split(",")).forEach(requestedFacetId::add);
                else if (param.equals(KEY_MAX_ROOTS))
                    maxRoots = Integer.parseInt(queryParameters.getFirst(param));
                else {
                    requestedFacetId.add(param);
                    requestedConceptIds.put(param, Set.of(queryParameters.getFirst(param).split(",")));
                }
            }
            Map<String, List<Node>> facetRoots = getFacetRoots(tx, requestedFacetId, requestedConceptIds, maxRoots);
            return new RecursiveMappingRepresentation(Representation.MAP, facetRoots);
        }
    }

    /**
     * Convenience method for programmatical access.
     *
     * @param requestedFacetId    A set of facet IDs for which to return their root concepts.
     * @param requestedConceptIds Optional. Groups concept root IDs to the facet they should be returned for.
     * @return
     * @see #getFacetRoots(UriInfo)
     */
    public Map<String, List<Node>> getFacetRoots(Transaction tx, Set<String> requestedFacetId, Map<String, Set<String>> requestedConceptIds, int maxRoots) {

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

    /**
     * <p>
     * Allows to add writing variants and acronyms to concepts in the database. For each type of data
     * (variants and acronyms) there is a parameter of its own. It is allowed to omit a parameter value. The expected format is"
     * {'tid1': {'docID1': {'variant1': count1, 'variant2': count2, ...}, 'docID2': {...}}, 'tid2':...} for both variants and acronyms."
     * </p>
     * <p>
     * Parameters encapsulated into a JSON object:
     *     <ul>
     *         <li>{@link #KEY_CONCEPT_TERMS}: A JSON object mapping concept IDs to an array of writing variants to add to the existing writing variants.</li>
     *         <li>{@link #KEY_CONCEPT_ACRONYMS}: A JSON object mapping concept IDs to an array of acronyms to add to the existing concept acronyms.</li>
     *     </ul>
     * </p>
     *
     * @throws IOException If the JSON input cannot be read.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @javax.ws.rs.Path(ADD_CONCEPT_TERM)
    public void addWritingVariants(String jsonParameterObject) throws IOException {
        ObjectMapper om = new ObjectMapper();
        Map<String, String> parameterMap = om.readValue(jsonParameterObject, Map.class);
        String conceptVariants = parameterMap.get(KEY_CONCEPT_TERMS);
        String conceptAcronyms = parameterMap.get(KEY_CONCEPT_ACRONYMS);
        if (null != conceptVariants)
            addConceptVariant(conceptVariants, "writingVariants");
        if (null != conceptAcronyms)
            addConceptVariant(conceptAcronyms, "acronyms");
    }

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
     * @param conceptVariants
     * @param type
     */
    private void addConceptVariant(String conceptVariants, String type) {
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
            GraphDatabaseService graphDb = dbms.database(DEFAULT_DATABASE_NAME);
            try (Transaction tx = graphDb.beginTx()) {
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
                tx.commit();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public enum EdgeTypes implements RelationshipType {
        /**
         * Relationship type for connecting aggregate classes with their element
         * concepts.
         */
        HAS_ELEMENT, HAS_ROOT_CONCEPT,
        /**
         * Relationship type to express that two concepts seem to be identical regarding
         * preferred label and synonyms.
         */
        HAS_SAME_NAMES, IS_BROADER_THAN,
        /**
         * A concept mapping that expresses some similarity between to concepts, e.g.
         * 'equal' or 'related'. The actual type of relatedness should be added as a
         * property to the relationship.
         */
        IS_MAPPED_TO,
        /**
         * Concept writing variants and their frequencies are stored in a special kind
         * of node, connected to the respective concept with this relationship type.
         */
        HAS_VARIANTS, HAS_ACRONYMS
    }

    public enum ConceptLabel implements Label {
        /**
         * Label to indicate a node is not an actual concept but an aggregate concept.
         * Such concepts have {@link EdgeTypes#HAS_ELEMENT} relationships to concepts,
         * deconceptining the set of concepts the aggregate represents.
         */
        AGGREGATE,
        /**
         * A particular type of {@link #AGGREGATE} node.
         */
        AGGREGATE_EQUAL_NAMES,
        /**
         * Label for nodes that are referenced by at least one other concept in imported
         * data, but are not included in the imported data themselves. Such concepts
         * know their source ID (given by the reference of another concept) and will be
         * made un-HOLLOW as soon at a concept with this source ID occurs in imported
         * data.
         */
        HOLLOW, CONCEPT
    }

    /**
     * Labels for nodes representing lexico-morphological variations of concepts.
     */
    public enum MorphoLabel implements Label {
        WRITING_VARIANTS, ACRONYMS, WRITING_VARIANT, ACRONYM
    }

    private class InsertionReport {
        /**
         * A temporary storage to keep track over relationships created during a single
         * concept insertion batch. It is used for deconceptination whether existing
         * relationships between two concepts must be checked or not. This is required
         * for the case that a concept is inserted multiple times in a single insertion
         * batch. Then, the "concept already existing" method does not work anymore.
         */
        public Set<String> createdRelationshipsCache = new HashSet<>();
        /**
         * The concept nodes that already existed before they should have been inserted
         * again (duplicate detection). This is used to deconceptine whether a check
         * about already existing relationships between two nodes is necessary. If at
         * least one of two concepts between which a relationships should be created did
         * not exist before, no check is necessary: A concept that did not exist could
         * not have had any relationships.
         */
        public Set<Node> existingConcepts = new HashSet<>();
        /**
         * The source IDs of concepts that have been omitted from the data for -
         * hopefully - good reasons. The first (and perhaps only) use case were
         * aggregates which had a single elements and were thus omitted but should also
         * be included into the concept hierarchy and have other concepts referring to
         * them as a parent. This set serves as a lookup in this case so we know there
         * is not an error.
         */
        public Set<String> omittedConcepts = new HashSet<>();
        /**
         * The coordinates of all concepts that are being imported. This information is
         * used by the relationship creation method to know if a parent is included in
         * the imported data or not.
         */
        public CoordinatesSet importedCoordinates = new CoordinatesSet();
        public int numRelationships = 0;
        public int numConcepts = 0;

        public void addCreatedRelationship(Node source, Node target, RelationshipType type) {
            createdRelationshipsCache.add(getRelationshipIdentifier(source, target, type));
        }

        public void addExistingConcept(Node concept) {
            existingConcepts.add(concept);
        }

        private String getRelationshipIdentifier(Node source, Node target, RelationshipType type) {
            return source.getId() + type.name() + target.getId();
        }

        public boolean relationshipAlreadyWasCreated(Node source, Node target, RelationshipType type) {
            return createdRelationshipsCache.contains(getRelationshipIdentifier(source, target, type));
        }

        public void addImportedCoordinates(ConceptCoordinates coordinates) {
            importedCoordinates.add(coordinates);
        }
    }
}
