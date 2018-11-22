package de.julielab.neo4j.plugins;

import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import de.julielab.neo4j.plugins.FacetManager.FacetLabel;
import de.julielab.neo4j.plugins.auxiliaries.JSON;
import de.julielab.neo4j.plugins.auxiliaries.PropertyUtilities;
import de.julielab.neo4j.plugins.auxiliaries.semedico.*;
import de.julielab.neo4j.plugins.auxiliaries.semedico.ConceptAggregateBuilder.CopyAggregatePropertiesStatistics;
import de.julielab.neo4j.plugins.constants.semedico.SequenceConstants;
import de.julielab.neo4j.plugins.datarepresentation.AddToNonFacetGroupCommand;
import de.julielab.neo4j.plugins.datarepresentation.ConceptCoordinates;
import de.julielab.neo4j.plugins.datarepresentation.ImportOptions;
import de.julielab.neo4j.plugins.datarepresentation.PushConceptsToSetCommand;
import de.julielab.neo4j.plugins.datarepresentation.PushConceptsToSetCommand.ConceptSelectionDefinition;
import de.julielab.neo4j.plugins.datarepresentation.constants.*;
import de.julielab.neo4j.plugins.util.AggregateConceptInsertionException;
import de.julielab.neo4j.plugins.util.ConceptInsertionException;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.graphdb.traversal.*;
import org.neo4j.server.plugins.*;
import org.neo4j.server.rest.repr.MappingRepresentation;
import org.neo4j.server.rest.repr.RecursiveMappingRepresentation;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.shell.util.json.JSONArray;
import org.neo4j.shell.util.json.JSONException;
import org.neo4j.shell.util.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.stream.StreamSupport;

import static de.julielab.neo4j.plugins.auxiliaries.PropertyUtilities.*;
import static de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants.*;
import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_ID;
import static java.util.stream.Collectors.joining;

@Description("This plugin discloses special operation for efficient access to the FacetConcepts for Semedico.")
public class ConceptManager extends ServerPlugin {

    public static final String INSERT_MAPPINGS = "insert_mappings";
    public static final String BUILD_AGGREGATES_BY_NAME_AND_SYNONYMS = "build_aggregates_by_name_and_synonyms";
    public static final String BUILD_AGGREGATES_BY_MAPPINGS = "build_aggregates_by_mappings";
    public static final String DELETE_AGGREGATES = "delete_aggregates";
    public static final String COPY_AGGREGATE_PROPERTIES = "copy_aggregate_properties";
    public static final String CREATE_SCHEMA_INDEXES = "create_schema_indexes";
    public static final String GET_CHILDREN_OF_CONCEPTS = "get_children_of_concepts";
    public static final String GET_NUM_CONCEPTS = "get_num_concepts";
    public static final String GET_PATHS_FROM_FACETROOTS = "get_paths_to_facetroots";
    public static final String INSERT_CONCEPTS = "insert_concepts";
    public static final String GET_FACET_ROOTS = "get_facet_roots";
    public static final String ADD_CONCEPT_CONCEPT = "add_concept_term";
    public static final String KEY_AMOUNT = "amount";
    public static final String KEY_CREATE_HOLLOW_PARENTS = "createHollowParents";
    public static final String KEY_FACET = "facet";
    public static final String KEY_FACET_ID = "facetId";
    public static final String KEY_FACET_IDS = "facetIds";
    public static final String KEY_FACET_PROP_KEY = "propertyKey";
    public static final String KEY_FACET_PROP_VALUE = "propertyValue";
    public static final String KEY_ID_TYPE = "idType";
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
    /**
     * The REST context path to this plugin. This is for convenience for usage from
     * external programs that make use of the plugin.
     */
    public static final String CONCEPT_MANAGER_ENDPOINT = "db/data/ext/" + ConceptManager.class.getSimpleName()
            + "/graphdb/";
    public static final String UPDATE_CHILDREN_INFORMATION = "update_children_information";
    private static final String UNKNOWN_CONCEPT_SOURCE = "<unknown>";
    private static final Logger log = LoggerFactory.getLogger(ConceptManager.class);
    private static final int CONCEPT_INSERT_BATCH_SIZE = 10000;

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

    @Name(BUILD_AGGREGATES_BY_MAPPINGS)
    @Description("Creates concept aggregates with respect to 'IS_MAPPED_TO' relationships.")
    @PluginTarget(GraphDatabaseService.class)
    public void buildAggregatesByMappings(@Source GraphDatabaseService graphDb,
                                          @Description("The allowed types for IS_MAPPED_TO relationships to be included in aggregation building.") @Parameter(name = KEY_ALLOWED_MAPPING_TYPES) String allowedMappingTypesArray,
                                          @Description("Label for concepts that have been processed by the aggregation algorithm. Such concepts"
                                                  + " can be aggregate concepts (with the label AGGREGATE) or just plain concepts"
                                                  + " (with the label CONCEPT) that are not an element of an aggregate.") @Parameter(name = KEY_AGGREGATED_LABEL) String aggregatedConceptsLabelString,
                                          @Description("Label to restrict the concepts to that are considered for aggregation creation.") @Parameter(name = KEY_LABEL, optional = true) String allowedConceptLabelString)
            throws JSONException {
        JSONArray allowedMappingTypesJson = new JSONArray(allowedMappingTypesArray);
        Set<String> allowedMappingTypes = new HashSet<>();
        for (int i = 0; i < allowedMappingTypesJson.length(); i++) {
            allowedMappingTypes.add(allowedMappingTypesJson.getString(i));
        }
        Label aggregatedConceptsLabel = Label.label(aggregatedConceptsLabelString);
        Label allowedConceptLabel = StringUtils.isBlank(allowedConceptLabelString) ? null
                : Label.label(allowedConceptLabelString);
        log.info("Creating mapping aggregates for concepts with label {} and mapping types {}", allowedConceptLabel,
                allowedMappingTypesJson);
        ConceptAggregateBuilder.buildAggregatesForMappings(graphDb, allowedMappingTypes, allowedConceptLabel,
                aggregatedConceptsLabel);
    }

    @Name(DELETE_AGGREGATES)
    @Description("Deletes aggregates with respect to a specific aggregate label. Only real aggregates are actually deleted, plain concepts that are 'their own' aggregates just lose the label.")
    @PluginTarget(GraphDatabaseService.class)
    public void deleteAggregatesByMappigs(@Source GraphDatabaseService graphDb,
                                          @Description("Label for concepts that have been processed by the aggregation algorithm. Such concepts"
                                                  + " can be aggregate concepts (with the label AGGREGATE) or just plain concepts"
                                                  + " (with the label CONCEPT) that are not an element of an aggregate.") @Parameter(name = KEY_AGGREGATED_LABEL) String aggregatedConceptsLabelString)
            throws JSONException {
        Label aggregatedConceptsLabel = Label.label(aggregatedConceptsLabelString);
        ConceptAggregateBuilder.deleteAggregates(graphDb, aggregatedConceptsLabel);
    }

    @Name(BUILD_AGGREGATES_BY_NAME_AND_SYNONYMS)
    // TODO
    @Description("TODO")
    @PluginTarget(GraphDatabaseService.class)
    public void buildAggregatesByNameAndSynonyms(@Source GraphDatabaseService graphDb,
                                                 @Description("TODO") @Parameter(name = KEY_CONCEPT_PROP_KEY) String conceptPropertyKey,
                                                 @Description("TODO") @Parameter(name = KEY_CONCEPT_PROP_VALUES) String propertyValues)
            throws JSONException {
        JSONArray jsonPropertyValues = new JSONArray(propertyValues);
        ConceptAggregateBuilder.buildAggregatesForEqualNames(graphDb, conceptPropertyKey, jsonPropertyValues);
    }

    @Name(COPY_AGGREGATE_PROPERTIES)
    // TODO
    @Description("TODO")
    @PluginTarget(GraphDatabaseService.class)
    public Representation copyAggregateProperties(@Source GraphDatabaseService graphDb) {
        int numAggregates = 0;
        CopyAggregatePropertiesStatistics copyStats = new CopyAggregatePropertiesStatistics();
        try (Transaction tx = graphDb.beginTx()) {
            try (ResourceIterator<Node> aggregateIt = graphDb.findNodes(ConceptLabel.AGGREGATE)) {
                while (aggregateIt.hasNext()) {
                    Node aggregate = aggregateIt.next();
                    numAggregates += copyAggregatePropertiesRecursively(aggregate, copyStats, new HashSet<Node>());
                }
            }
            tx.success();
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

    private void createRelationships(GraphDatabaseService graphDb, JSONArray jsonConcepts, Node facet,
                                     CoordinatesMap nodesByCoordinates, ImportOptions importOptions, InsertionReport insertionReport)
            throws JSONException {
        log.info("Creating relationship between inserted concepts.");
        Index<Node> idIndex = graphDb.index().forNodes(ConceptConstants.INDEX_NAME);
        String facetId = null;
        if (null != facet)
            facetId = (String) facet.getProperty(FacetConstants.PROP_ID);
        RelationshipType relBroaderThanInFacet = null;
        if (null != facet)
            relBroaderThanInFacet = RelationshipType.withName(EdgeTypes.IS_BROADER_THAN.toString() + "_" + facetId);
        AddToNonFacetGroupCommand noFacetCmd = importOptions.noFacetCmd;
        Node noFacet = null;
        int quarter = jsonConcepts.length() / 4;
        int numQuarter = 1;
        long totalTime = 0;
        long relCreationTime = 0;
        for (int i = 0; i < jsonConcepts.length(); i++) {
            long time = System.currentTimeMillis();
            JSONObject jsonConcept = jsonConcepts.getJSONObject(i);
            // aggregates may be included into the taxonomy, but by default they
            // are not
            if (JSON.getBoolean(jsonConcept, ConceptConstants.AGGREGATE)
                    && !JSON.getBoolean(jsonConcept, ConceptConstants.AGGREGATE_INCLUDE_IN_HIERARCHY))
                continue;
            JSONObject coordinates = jsonConcept.getJSONObject(COORDINATES);
            // Every concept must have a source ID...
            String srcId = coordinates.getString(CoordinateConstants.SOURCE_ID);
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
                if (jsonConcept.has(PARENT_COORDINATES) && jsonConcept.getJSONArray(PARENT_COORDINATES).length() > 0) {
                    JSONArray parentCoordinateArray = jsonConcept.getJSONArray(PARENT_COORDINATES);
                    for (int j = 0; j < parentCoordinateArray.length(); j++) {
                        ConceptCoordinates parentCoordinates = new ConceptCoordinates(
                                parentCoordinateArray.getJSONObject(j));

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
                        if (null != parent && parent.hasLabel(ConceptLabel.AGGREGATE)
                                && !parent.hasLabel(ConceptLabel.CONCEPT))
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
                        if (null != noFacetCmd && null == noFacet) {
                            noFacet = FacetManager.getNoFacet(graphDb, (String) facet.getProperty(PROP_ID));
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
                if (jsonConcept.has(ConceptConstants.RELATIONSHIPS)) {
                    log.info("Adding explicitly specified relationships");
                    JSONArray jsonRelationships = jsonConcept.getJSONArray(ConceptConstants.RELATIONSHIPS);
                    for (int j = 0; j < jsonRelationships.length(); j++) {
                        JSONObject jsonRelationship = jsonRelationships.getJSONObject(j);
                        String rsTypeStr = jsonRelationship.getString(ConceptConstants.RS_TYPE);
                        final JSONObject targetCoordinateJson = jsonRelationship.getJSONObject(ConceptConstants.RS_TARGET_COORDINATES);
                        final ConceptCoordinates targetCoordinates = new ConceptCoordinates(targetCoordinateJson);
                        Node target = lookupConcept(targetCoordinates, idIndex);
                        if (null == target) {
                            log.debug("Creating hollow relationship target with orig Id/orig source " + targetCoordinates);
                            target = registerNewHollowConceptNode(graphDb, targetCoordinates, idIndex);
                        }
                        EdgeTypes type = EdgeTypes.valueOf(rsTypeStr);
                        Object[] properties = null;
                        if (jsonRelationship.has(ConceptConstants.RS_PROPS)) {
                            JSONObject relProps = jsonRelationship.getJSONObject(ConceptConstants.RS_PROPS);
                            JSONArray propNames = relProps.names();
                            properties = new Object[propNames.length() * 2];
                            for (int k = 0; k < propNames.length(); ++k) {
                                String propName = propNames.getString(k);
                                Object propValue = relProps.get(propName);
                                properties[2 * k] = propName;
                                properties[2 * k + 1] = propValue;
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
     * Checks whether an automatic index for the <tt>label</tt> exists on the
     * {@link ConceptConstants#PROP_ID} property and creates it, if not.
     *
     * @param graphDb
     * @param label
     */
    private void createIndexIfAbsent(GraphDatabaseService graphDb, Label label, String key, boolean unique) {
        try (Transaction tx = graphDb.beginTx()) {
            Schema schema = graphDb.schema();
            boolean indexExists = false;
            for (IndexDefinition id : schema.getIndexes(label)) {
                for (String propertyKey : id.getPropertyKeys()) {
                    if (propertyKey.equals(key))
                        indexExists = true;
                }
            }
            if (!indexExists) {
                log.info("Creating index for label " + label + " on property " + key + " (unique: " + unique + ").");
                // IndexDefinition indexDefinition;
                // indexDefinition = schema.indexFor(label).on(key).create();
                schema.constraintFor(label).assertPropertyIsUnique(key).create();
                // schema.awaitIndexOnline(indexDefinition, 15,
                // TimeUnit.MINUTES);
                tx.success();
            }
        }
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

    @Name(CREATE_SCHEMA_INDEXES)
    @Description("Creates uniqueness constraints (and thus, indexes), on the following label / property combinations: CONCEPT / "
            + ConceptConstants.PROP_ID + "; CONCEPT / " + ConceptConstants.PROP_ORG_ID + "; FACET / "
            + FacetConstants.PROP_ID + "; NO_FACET / " + FacetConstants.PROP_ID + "; ROOT / " + NodeConstants.PROP_NAME
            + ". This should be done after the main initial import because node insertion with uniqueness switched on costs significant insertion performance.")
    @PluginTarget(GraphDatabaseService.class)
    public void createSchemaIndexes(@Source GraphDatabaseService graphDb) {
        createIndexIfAbsent(graphDb, ConceptLabel.CONCEPT, ConceptConstants.PROP_ID, true);
        createIndexIfAbsent(graphDb, ConceptLabel.CONCEPT, ConceptConstants.PROP_ORG_ID, true);
        createIndexIfAbsent(graphDb, FacetLabel.FACET, FacetConstants.PROP_ID, true);
        createIndexIfAbsent(graphDb, FacetLabel.NO_FACET, FacetConstants.PROP_ID, true);
        createIndexIfAbsent(graphDb, NodeConstants.Labels.ROOT, NodeConstants.PROP_NAME, true);
    }

    @Name(GET_CHILDREN_OF_CONCEPTS)
    @Description("Returns all non-hollow children of concepts identified via the " + KEY_CONCEPT_IDS
            + " parameter. The return format is a map from the children's id"
            + " to respective child concept. This endpoint has been created due"
            + " to performance reasons. All tried Cypher queries to achieve"
            + " the same behaviour were less performant (tested for version 2.0.0 M3).")
    @PluginTarget(GraphDatabaseService.class)
    public MappingRepresentation getChildrenOfConcepts(@Source GraphDatabaseService graphDb,
                                                       @Description("JSON array of concept IDs for which to return their children.") @Parameter(name = KEY_CONCEPT_IDS) String conceptIdArray,
                                                       @Description("The label agsinst which the given concept IDs are resolved. Defaults to 'CONCEPT'.") @Parameter(name = KEY_LABEL, optional = true) String labelString)
            throws JSONException {
        Label label = ConceptLabel.CONCEPT;
        if (!StringUtils.isBlank(labelString))
            label = Label.label(labelString);
        JSONArray conceptIds = new JSONArray(conceptIdArray);
        try (Transaction tx = graphDb.beginTx()) {
            Map<String, Object> childrenByConceptId = new HashMap<>();
            for (int i = 0; i < conceptIds.length(); i++) {
                Map<String, List<String>> reltypesByNodeId = new HashMap<>();
                Set<Node> childList = new HashSet<>();
                String conceptId = conceptIds.getString(i);
                Node concept = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, label, PROP_ID, conceptId);
                if (null != concept) {
                    Iterator<Relationship> rels = concept.getRelationships(Direction.OUTGOING).iterator();
                    while (rels.hasNext()) {
                        Relationship rel = rels.next();
                        String reltype = rel.getType().name();
                        Node child = rel.getEndNode();
                        boolean isHollow = false;
                        for (Label l : child.getLabels())
                            if (l.equals(ConceptLabel.HOLLOW))
                                isHollow = true;
                        if (isHollow)
                            continue;
                        String childId = (String) child.getProperty(PROP_ID);
                        List<String> reltypeList = reltypesByNodeId.get(childId);
                        if (null == reltypeList) {
                            reltypeList = new ArrayList<>();
                            reltypesByNodeId.put(childId, reltypeList);
                        }
                        reltypeList.add(reltype);
                        childList.add(child);
                    }
                    Map<String, Object> childrenAndReltypes = new HashMap<>();
                    childrenAndReltypes.put(RET_KEY_CHILDREN, childList);
                    childrenAndReltypes.put(RET_KEY_RELTYPES, reltypesByNodeId);
                    childrenByConceptId.put(conceptId, childrenAndReltypes);
                }
            }
            return new RecursiveMappingRepresentation(Representation.MAP, childrenByConceptId);
        }
    }

    @Name(GET_NUM_CONCEPTS)
    @Description("Returns the number of concepts in the database, i.e. the number of nodes with the \"CONCEPT\" label.")
    @PluginTarget(GraphDatabaseService.class)
    public long getNumConcepts(@Source GraphDatabaseService graphDb) {
        try (Transaction tx = graphDb.beginTx()) {
            ResourceIterable<Node> concepts = () -> graphDb.findNodes(ConceptLabel.CONCEPT);
            long count = 0;
            for (@SuppressWarnings("unused")
                    Node concept : concepts) {
                count++;
            }
            return count;
        }
    }

    @Name(GET_PATHS_FROM_FACETROOTS)
    @Description("TODO")
    @PluginTarget(GraphDatabaseService.class)
    public Representation getPathsFromFacetroots(@Source GraphDatabaseService graphDb,
                                                 @Description("TODO") @Parameter(name = KEY_CONCEPT_IDS) String conceptIdsJsonString,
                                                 @Description("TODO") @Parameter(name = KEY_ID_TYPE) String idType,
                                                 @Description("TODO") @Parameter(name = KEY_SORT_RESULT) boolean sort,
                                                 @Description("TODO") @Parameter(name = KEY_FACET_ID) final String facetId) throws JSONException {
        JSONArray conceptIds = new JSONArray(conceptIdsJsonString);

        Evaluator rootConceptEvaluator = new Evaluator() {

            @Override
            public Evaluation evaluate(Path path) {
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
            }

        };
        RelationshipType relType = StringUtils.isBlank(facetId) ? ConceptManager.EdgeTypes.IS_BROADER_THAN
                : RelationshipType.withName(ConceptManager.EdgeTypes.IS_BROADER_THAN.name() + "_" + facetId);
        TraversalDescription td = graphDb.traversalDescription().uniqueness(Uniqueness.NODE_PATH).depthFirst()
                .relationships(relType, Direction.INCOMING).evaluator(rootConceptEvaluator);

        try (Transaction tx = graphDb.beginTx()) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < conceptIds.length(); i++) {
                String conceptId = conceptIds.getString(i);
                // escape the conceptId for the Lucene lookup
                conceptId = QueryParser.escape(conceptId);
                sb.append(idType).append(":").append(conceptId);
                if (i < conceptIds.length() - 1)
                    sb.append(" ");
            }
            String conceptQuery = sb.toString();

            Index<Node> conceptIndex = graphDb.index().forNodes(INDEX_NAME);
            IndexHits<Node> indexHits = conceptIndex.query(conceptQuery);
            ResourceIterator<Node> startNodeIterator = indexHits.iterator();
            Node[] startNodes = new Node[indexHits.size()];
            for (int i = 0; i < indexHits.size(); i++) {
                if (startNodeIterator.hasNext()) {
                    startNodes[i] = startNodeIterator.next();
                } else
                    throw new IllegalStateException(indexHits.size()
                            + " index hits for start nodes expected but iterator expired unexpectedly.");
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
                    if (!n.hasProperty(PROP_ID)) {
                        log.warn("Came across the concept " + n + " (" + NodeUtilities.getNodePropertiesAsString(n)
                                + ") when computing root paths. But this concept does not have an ID.");
                        error = true;
                        break;
                    }
                    pathConceptIds[i] = (String) n.getProperty(PROP_ID);
                }
                if (!error)
                    pathsConceptIds.add(pathConceptIds);
            }
            if (sort)
                Collections.sort(pathsConceptIds, new Comparator<String[]>() {
                    @Override
                    public int compare(String[] o1, String[] o2) {
                        return o1.length - o2.length;
                    }
                });
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
     * {@link #copyAggregateProperties(GraphDatabaseService)}.
     *
     * @param graphDb
     * @param conceptIndex
     * @param jsonConcept
     * @param nodesByCoordinates
     * @param insertionReport
     * @param importOptions
     * @return
     * @throws JSONException
     * @throws AggregateConceptInsertionException
     */
    private void insertAggregateConcept(GraphDatabaseService graphDb, Index<Node> conceptIndex, JSONObject jsonConcept,
                                        CoordinatesMap nodesByCoordinates, InsertionReport insertionReport, ImportOptions importOptions)
            throws AggregateConceptInsertionException {
        try {
            JSONObject aggCoordinates = jsonConcept.has(COORDINATES) ? jsonConcept.getJSONObject(COORDINATES)
                    : new JSONObject();
            String aggOrgId = JSON.getString(aggCoordinates, CoordinateConstants.ORIGINAL_ID);
            String aggOrgSource = JSON.getString(aggCoordinates, CoordinateConstants.ORIGINAL_SOURCE);
            String aggSrcId = JSON.getString(aggCoordinates, CoordinateConstants.SOURCE_ID);
            String aggSource = JSON.getString(aggCoordinates, CoordinateConstants.SOURCE);
            if (null == aggSource)
                aggSource = UNKNOWN_CONCEPT_SOURCE;
            log.trace("Looking up aggregate ({}, {}) / ({}, {}), original/source coordinates.", aggOrgId, aggOrgSource,
                    aggSrcId, aggSource);
            Node aggregate = lookupConcept(new ConceptCoordinates(aggCoordinates, false), conceptIndex);
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
                aggregate = graphDb.createNode(ConceptLabel.AGGREGATE);
            }
            boolean includeAggreationInHierarchy = jsonConcept.has(AGGREGATE_INCLUDE_IN_HIERARCHY)
                    && jsonConcept.getBoolean(AGGREGATE_INCLUDE_IN_HIERARCHY);
            // If the aggregate is to be included into the hierarchy, it also should
            // be a CONCEPT for path creation
            if (includeAggreationInHierarchy)
                aggregate.addLabel(ConceptLabel.CONCEPT);
            JSONArray elementCoords = jsonConcept.getJSONArray(ConceptConstants.ELEMENT_COORDINATES);
            log.trace("    looking up aggregate elements");
            for (int i = 0; i < elementCoords.length(); i++) {
                JSONObject elementCoord = elementCoords.getJSONObject(i);
                final ConceptCoordinates elementCoordinates = new ConceptCoordinates(elementCoord);
                String elementSource = elementCoordinates.source;
                if (null == elementCoordinates.source)
                    elementSource = UNKNOWN_CONCEPT_SOURCE;
                Node element = nodesByCoordinates.get(elementCoordinates);
                if (null != element) {
                    String[] srcIds = (String[]) element.getProperty(PROP_SRC_IDS);
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
                    element = lookupConceptBySourceId(elementCoordinates.sourceId, elementSource, false, conceptIndex);
                if (null == element && importOptions.createHollowAggregateElements) {
                    element = registerNewHollowConceptNode(graphDb, elementCoordinates, conceptIndex);
                    log.trace("    Creating HOLLOW element with source coordinates ({}, {})", elementCoordinates.sourceId,
                            elementSource);
                }
                if (element != null) {
                    aggregate.createRelationshipTo(element, EdgeTypes.HAS_ELEMENT);
                }
            }

            // Set the aggregate's properties
            if (null != aggSrcId) {
                int idIndex = findFirstValueInArrayProperty(aggregate, PROP_SRC_IDS, aggSrcId);
                int sourceIndex = findFirstValueInArrayProperty(aggregate, PROP_SOURCES, aggSource);
                if (!StringUtils.isBlank(aggSrcId)
                        && ((idIndex == -1 && sourceIndex == -1) || (idIndex != sourceIndex))) {
                    addToArrayProperty(aggregate, PROP_SRC_IDS, aggSrcId, true);
                    addToArrayProperty(aggregate, PROP_SOURCES, aggSource, true);
                }
                // if the aggregate has a source ID, add it to the respective
                // map for later access during the relationship insertion phase
                nodesByCoordinates.put(new ConceptCoordinates(aggCoordinates), aggregate);
                conceptIndex.add(aggregate, PROP_SRC_IDS, aggSrcId);
            }
            if (null != aggOrgId)
                aggregate.setProperty(PROP_ORG_ID, aggOrgId);
            if (null != aggOrgSource)
                aggregate.setProperty(PROP_ORG_SRC, aggOrgSource);
            JSONArray copyProperties = JSON.getJSONArray(jsonConcept, ConceptConstants.PROP_COPY_PROPERTIES);
            if (null != copyProperties && copyProperties.length() > 0)
                aggregate.setProperty(ConceptConstants.PROP_COPY_PROPERTIES, JSON.json2JavaArray(copyProperties));

            JSONArray generalLabels = JSON.getJSONArray(jsonConcept, ConceptConstants.PROP_LABELS);
            for (int i = 0; null != generalLabels && i < generalLabels.length(); i++) {
                aggregate.addLabel(Label.label(generalLabels.getString(i)));
            }

            String aggregateId = NodeIDPrefixConstants.AGGREGATE_TERM
                    + SequenceManager.getNextSequenceValue(graphDb, SequenceConstants.SEQ_AGGREGATE_TERM);
            aggregate.setProperty(PROP_ID, aggregateId);
            conceptIndex.add(aggregate, PROP_ID, aggregateId);

            insertionReport.numConcepts++;
        } catch (Exception e) {
            throw new AggregateConceptInsertionException(
                    "Aggregate concept creation failed for aggregate " + jsonConcept, e);
        }
    }

    private void insertConcept(GraphDatabaseService graphDb, String facetId, Index<Node> conceptIndex,
                               JSONObject jsonConcept, CoordinatesMap nodesByCoordinates, InsertionReport insertionReport,
                               ImportOptions importOptions) throws JSONException {
        // Name is mandatory, thus we don't use the
        // null-convenience method here.
        String prefName = JSON.getString(jsonConcept, PROP_PREF_NAME);
        JSONArray synonyms = JSON.getJSONArray(jsonConcept, PROP_SYNONYMS);
        JSONArray generalLabels = JSON.getJSONArray(jsonConcept, ConceptConstants.PROP_LABELS);

        JSONObject coordinatesJson = jsonConcept.getJSONObject(COORDINATES);
        ConceptCoordinates coordinates = new ConceptCoordinates(coordinatesJson);

        if (!jsonConcept.has(COORDINATES) || coordinatesJson.length() == 0)
            throw new IllegalArgumentException(
                    "The concept " + jsonConcept.toString(2) + " does not specify coordinates.");

        // Source ID is mandatory if we have a real concept import and not just a
        // merging operation.
        String srcId = importOptions.merge ? JSON.getString(coordinatesJson, CoordinateConstants.SOURCE_ID)
                : coordinatesJson.getString(CoordinateConstants.SOURCE_ID);
        // The other properties may have values or not, make it
        // null-proof.
        String orgId = JSON.getString(coordinatesJson, CoordinateConstants.ORIGINAL_ID);
        String source = JSON.getString(coordinatesJson, CoordinateConstants.SOURCE);
        String orgSource = JSON.getString(coordinatesJson, CoordinateConstants.ORIGINAL_SOURCE);
        boolean uniqueSourceId = JSON.getBoolean(coordinatesJson, CoordinateConstants.UNIQUE_SOURCE_ID, false);

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
        if (importOptions.merge && jsonConcept.has(PARENT_COORDINATES))
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
                    + coordinatesJson + " and this is not a merging operation.");
        else if (concept == null)
            // we are in merging mode, for nodes we don't know we just do
            // nothing
            return;
        if (concept.hasLabel(ConceptLabel.HOLLOW)) {
            log.trace("Got HOLLOW concept node with coordinates " + coordinatesJson + " and will create full concept.");
            concept.removeLabel(ConceptLabel.HOLLOW);
            concept.addLabel(ConceptLabel.CONCEPT);
            Iterable<Relationship> relationships = concept.getRelationships(EdgeTypes.HAS_ROOT_CONCEPT);
            for (Relationship rel : relationships) {
                Node startNode = rel.getStartNode();
                if (startNode.hasLabel(FacetManager.FacetLabel.FACET))
                    rel.delete();
            }
            String conceptId = NodeIDPrefixConstants.TERM
                    + SequenceManager.getNextSequenceValue(graphDb, SequenceConstants.SEQ_TERM);
            concept.setProperty(PROP_ID, conceptId);
            conceptIndex.putIfAbsent(concept, PROP_ID, conceptId);
        }

        // Merge the new or an already existing concept with what we
        // already
        // have, perhaps the
        // stored information
        // and the new information is complementary to each other
        // (if
        // there
        // is any information already stored, the concept could be
        // fresh
        // and
        // empty).
        // Currently, just do the following: For non-array property
        // values, set those properties which are currently non
        // existent. For array, merge the arrays.
        if (!StringUtils.isBlank(coordinates.originalId) && !concept.hasProperty(PROP_ORG_ID)) {
            concept.setProperty(PROP_ORG_ID, coordinates.originalId);
            concept.setProperty(PROP_ORG_SRC, coordinates.originalSource);
        }

        PropertyUtilities.mergeJSONObjectIntoPropertyContainer(jsonConcept, concept, ConceptConstants.PROP_LABELS,
                PROP_SRC_IDS, PROP_SOURCES, PROP_SYNONYMS, COORDINATES, PARENT_COORDINATES,
                ConceptConstants.RELATIONSHIPS);
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
        int idIndex = findFirstValueInArrayProperty(concept, PROP_SRC_IDS, srcId);
        int sourceIndex = findFirstValueInArrayProperty(concept, PROP_SOURCES, source);
        // (sourceID, source) coordinate has not been found, create it
        if (!StringUtils.isBlank(srcId) && ((idIndex == -1 && sourceIndex == -1) || (idIndex != sourceIndex))) {
            // on first creation, no concept node has a source ID at this point
            if (concept.hasProperty(PROP_SRC_IDS))
                srcIduniqueMarkerChanged = checkUniqueIdMarkerClash(concept, srcId, uniqueSourceId);
            addToArrayProperty(concept, PROP_SRC_IDS, srcId, true);
            addToArrayProperty(concept, PROP_SOURCES, source, true);
            addToArrayProperty(concept, PROP_UNIQUE_SRC_ID, uniqueSourceId, true);
        }
        mergeArrayProperty(concept, ConceptConstants.PROP_SYNONYMS, JSON.json2JavaArray(synonyms, prefName));
        addToArrayProperty(concept, PROP_FACETS, facetId);

        for (int i = 0; null != generalLabels && i < generalLabels.length(); i++) {
            concept.addLabel(Label.label(generalLabels.getString(i)));
        }

        if (srcIduniqueMarkerChanged) {
            log.warn("Merging concept nodes with unique source ID " + srcId
                    + " because on concept with this source ID and source " + source
                    + " the ID was declared non-unique in the past but unique now. Properties from all nodes are merged together and relationships are moved from obsolete nodes to the single remaining node. This is experimental and might lead to errors.");
            List<Node> obsoleteNodes = new ArrayList<>();
            Node mergedNode = NodeUtilities.mergeConceptNodesWithUniqueSourceId(srcId, conceptIndex, obsoleteNodes);
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
     * @param graphDb
     * @param jsonConcepts
     * @param facetId
     * @param nodesByCoordinates
     * @param importOptions
     * @return
     * @throws JSONException
     * @throws AggregateConceptInsertionException
     */
    private InsertionReport insertConcepts(GraphDatabaseService graphDb, JSONArray jsonConcepts, String facetId,
                                           CoordinatesMap nodesByCoordinates, ImportOptions importOptions) throws ConceptInsertionException {
        try {
            long time = System.currentTimeMillis();
            InsertionReport insertionReport = new InsertionReport();
            // Idea: First create all nodes and just store which Node has which
            // parent. Then, after all nodes have been created, do the actual
            // connection.
            Index<Node> conceptIndex = null;
            IndexManager indexManager = graphDb.index();
            conceptIndex = indexManager.forNodes(INDEX_NAME);

            // this MUST be a TreeSort or at least some collection using the
            // Comparable interface because ConceptCoordinates are rather
            // complicated regarding equality
            CoordinatesSet toBeCreated = new CoordinatesSet();
            // First, iterate through all concepts and check if their parents
            // already exist, before any nodes are created (for more efficient
            // relationship creation).

            // When merging, we don't care about parents.
            if (!importOptions.merge) {
                for (int i = 0; i < jsonConcepts.length(); i++) {
                    JSONObject jsonConcept = jsonConcepts.getJSONObject(i);
                    if (jsonConcept.has(PARENT_COORDINATES) && jsonConcept.getJSONArray(PARENT_COORDINATES).length() > 0) {
                        JSONArray parentCoordinatesArray = jsonConcept.getJSONArray(PARENT_COORDINATES);
                        for (int j = 0; j < parentCoordinatesArray.length(); j++) {
                            ConceptCoordinates parentCoordinates = new ConceptCoordinates(
                                    parentCoordinatesArray.getJSONObject(j));
                            Node parentNode = lookupConcept(parentCoordinates, conceptIndex);
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
            for (int i = 0; i < jsonConcepts.length(); i++) {
                JSONObject jsonConcept = jsonConcepts.getJSONObject(i);
                ConceptCoordinates coordinates;
                if (jsonConcept.has(ConceptConstants.COORDINATES)) {
                    coordinates = new ConceptCoordinates(jsonConcept.getJSONObject(ConceptConstants.COORDINATES));
                    insertionReport.addImportedCoordinates(coordinates);
                } else if (!JSON.getBoolean(jsonConcept, ConceptConstants.AGGREGATE)) {
                    throw new IllegalArgumentException("Concept " + jsonConcept + " does not define concept coordinates.");
                } else {
                    continue;
                }
                // many nodes will actually already have been seen as parents
                // above
                if (nodesByCoordinates.containsKey(coordinates) || toBeCreated.contains(coordinates, true))
                    continue;
                Node conceptNode = lookupConcept(coordinates, conceptIndex);
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
                Node conceptNode = registerNewHollowConceptNode(graphDb, coordinates, conceptIndex);
                ++insertionReport.numConcepts;

                nodesByCoordinates.put(coordinates, conceptNode);
            }

            log.info("removing " + importConceptsToRemove.size()
                    + " input concepts that should be omitted because we are merging and don't have them in the database");
            for (int index = importConceptsToRemove.size() - 1; index >= 0; --index)
                jsonConcepts.remove(importConceptsToRemove.get(index));

            log.info("Starting to insert " + jsonConcepts.length() + " concepts.");
            for (int i = 0; i < jsonConcepts.length(); i++) {
                JSONObject jsonConcept = jsonConcepts.getJSONObject(i);
                boolean isAggregate = JSON.getBoolean(jsonConcept, ConceptConstants.AGGREGATE);
                if (isAggregate) {
                    insertAggregateConcept(graphDb, conceptIndex, jsonConcept, nodesByCoordinates, insertionReport,
                            importOptions);
                } else {
                    insertConcept(graphDb, facetId, conceptIndex, jsonConcept, nodesByCoordinates, insertionReport,
                            importOptions);
                }
            }
            log.debug(jsonConcepts.length() + " concepts inserted.");
            time = System.currentTimeMillis() - time;
            log.info(insertionReport.numConcepts
                    + " new concepts - but not yet relationships - have been inserted. This took " + time + " ms ("
                    + (time / 1000) + " s)");
            return insertionReport;
        } catch (JSONException e) {
            throw new ConceptInsertionException(e);
        }
    }

    /**
     * Creates a node with the {@link ConceptLabel#HOLLOW} label, sets the given
     * coordinates and adds the node to the index.
     *
     * @param graphDb
     * @param coordinates
     * @param conceptIndex
     * @return
     */
    private Node registerNewHollowConceptNode(GraphDatabaseService graphDb, ConceptCoordinates coordinates,
                                              Index<Node> conceptIndex, Label... additionalLabels) {
        Node node = graphDb.createNode(ConceptLabel.HOLLOW);
        for (int i = 0; i < additionalLabels.length; i++) {
            Label label = additionalLabels[i];
            node.addLabel(label);
        }
        log.trace("Created new HOLLOW concept node for coordinates {}", coordinates);
        if (!StringUtils.isBlank(coordinates.originalId)) {
            node.setProperty(PROP_ORG_ID, coordinates.originalId);
            node.setProperty(PROP_ORG_SRC, coordinates.originalSource);
        }
        node.setProperty(PROP_SRC_IDS, new String[]{coordinates.sourceId});
        node.setProperty(PROP_SOURCES, new String[]{coordinates.source});
        node.setProperty(PROP_UNIQUE_SRC_ID, new boolean[]{coordinates.uniqueSourceId});

        if (!StringUtils.isBlank(coordinates.sourceId))
            conceptIndex.putIfAbsent(node, PROP_SRC_IDS, coordinates.sourceId);
        if (!StringUtils.isBlank(coordinates.originalId))
            conceptIndex.putIfAbsent(node, PROP_ORG_ID, coordinates.originalId);

        return node;
    }

    public Representation insertConcepts(GraphDatabaseService graphDb, String conceptsWithFacet) throws JSONException, ConceptInsertionException {
        JSONObject input = new JSONObject(conceptsWithFacet);
        JSONObject jsonFacet = JSON.getJSONObject(input, KEY_FACET);
        JSONArray jsonConcepts = input.getJSONArray(KEY_CONCEPTS);
        JSONObject importOptionsJson = JSON.getJSONObject(input, KEY_IMPORT_OPTIONS);
        return insertConcepts(graphDb, jsonFacet != null ? jsonFacet.toString() : null, jsonConcepts.toString(),
                importOptionsJson != null ? importOptionsJson.toString() : null);
    }

    @Name(INSERT_CONCEPTS)
    // TODO
    @Description("TODO")
    @PluginTarget(GraphDatabaseService.class)
    public Representation insertConcepts(@Source GraphDatabaseService graphDb,
                                         @Description("TODO") @Parameter(name = KEY_FACET, optional = true) String facetJson,
                                         @Description("TODO") @Parameter(name = KEY_CONCEPTS, optional = true) String conceptsJson,
                                         @Description("TODO") @Parameter(name = KEY_IMPORT_OPTIONS, optional = true) String importOptionsJsonString)
            throws JSONException, ConceptInsertionException {
        log.info("{} was called", INSERT_CONCEPTS);
        long time = System.currentTimeMillis();

        Gson gson = new Gson();
        log.debug("Parsing input.");
        JSONObject jsonFacet = null;
        JSONArray jsonConcepts = null;
        JSONObject importOptionsJson = null;
        jsonFacet = !StringUtils.isEmpty(facetJson) ? new JSONObject(facetJson) : null;
        if (null != conceptsJson) {
            jsonConcepts = new JSONArray(conceptsJson);
            log.info("Got {} input concepts for import.", jsonConcepts.length());
        } else {
            log.info("Got 0 input concepts for import.");
        }
        ImportOptions importOptions;
        if (null != importOptionsJsonString) {
            importOptionsJson = new JSONObject(importOptionsJsonString);
            importOptions = gson.fromJson(importOptionsJson.toString(), ImportOptions.class);
        } else {
            importOptions = new ImportOptions();
        }

        Map<String, Object> report = new HashMap<>();
        InsertionReport insertionReport = new InsertionReport();
        log.debug("Beginning processing of concept insertion.");
        try (Transaction tx = graphDb.beginTx()) {
            Node facet = null;
            String facetId = null;
            // The facet Id will be added to the facets-property of the concept
            // nodes.
            log.debug("Handling import of facet.");
            if (null != jsonFacet && jsonFacet.has(PROP_ID)) {
                facetId = jsonFacet.getString(PROP_ID);
                log.info("Facet ID {} has been given to add the concepts to.", facetId);
                boolean isNoFacet = JSON.getBoolean(jsonFacet, FacetConstants.NO_FACET);
                if (isNoFacet)
                    facet = FacetManager.getNoFacet(graphDb, facetId);
                else
                    facet = FacetManager.getFacetNode(graphDb, facetId);
                if (null == facet)
                    throw new IllegalArgumentException("The facet with ID \"" + facetId
                            + "\" was not found. You must pass the ID of an existing facet or deliver all information required to create the facet from scratch. Then, the facetId must not be included in the request, it will be created dynamically.");
            } else if (null != jsonFacet && jsonFacet.has(FacetConstants.PROP_NAME)) {
                ResourceIterator<Node> facetIterator = graphDb.findNodes(FacetLabel.FACET);
                while (facetIterator.hasNext()) {
                    facet = facetIterator.next();
                    if (facet.getProperty(FacetConstants.PROP_NAME)
                            .equals(jsonFacet.getString(FacetConstants.PROP_NAME)))
                        break;
                    facet = null;
                }

            }
            if (null != jsonFacet && null == facet) {
                // No existing ID is given, create a new facet.
                facet = FacetManager.createFacet(graphDb, jsonFacet);
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
                insertionReport = insertConcepts(graphDb, jsonConcepts, facetId, nodesByCoordinates, importOptions);
                // If the nodesBySrcId map is empty we either have no concepts or
                // at least no concepts with a source ID. Then,
                // relationship creation is currently not supported.
                if (!nodesByCoordinates.isEmpty() && !importOptions.merge)
                    createRelationships(graphDb, jsonConcepts, facet, nodesByCoordinates, importOptions,
                            insertionReport);
                else
                    log.info("This is a property merging import, no relationships are created.");
                time = System.currentTimeMillis() - time;
                report.put(RET_KEY_NUM_CREATED_CONCEPTS, insertionReport.numConcepts);
                report.put(RET_KEY_NUM_CREATED_RELS, insertionReport.numRelationships);
                report.put(KEY_FACET_ID, facetId);
                report.put(KEY_TIME, time);
                log.debug("Done creating concepts and relationships.");
            } else {
                log.info("No concepts were included in the request.");
            }

            tx.success();
        }
        log.info("Concept insertion complete.");
        log.info(INSERT_CONCEPTS + " is finished processing after " + time + " ms. " + insertionReport.numConcepts
                + " concepts and " + insertionReport.numRelationships + " relationships have been created.");
        return new RecursiveMappingRepresentation(Representation.MAP, report);
    }

    /**
     * RULE: Two concepts are equal, iff they have the same original source ID
     * assigned from the same original source or both have no contradicting original
     * ID and original source but the same source ID and source. Contradicting means
     * two non-null values that are not equal.
     *
     * @param coordinates  The coordinates of the concept to find.
     * @param conceptIndex The index storing concepts by their original and source IDs.
     * @return The node corresponding to the given coordinates or null, if none is found.
     */
    private Node lookupConcept(ConceptCoordinates coordinates, Index<Node> conceptIndex) {
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
        concept = null != orgId ? conceptIndex.get(PROP_ORG_ID, orgId).getSingle() : null;
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
            concept = lookupConceptBySourceId(srcId, source, uniqueSourceId, conceptIndex);
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
     * @param srcId          The source ID of the requested concept node.
     * @param source         The source in which the concept node should be given
     *                       <tt>srcId</tt> as a source ID.
     * @param uniqueSourceId Whether the ID should be unique, independently from the source.
     *                       This holds, for example, for ontology class IRIs.
     * @param conceptIndex   The concept index.
     * @return The requested concept node or <tt>null</tt> if no such node is found.
     */
    private Node lookupConceptBySourceId(String srcId, String source, boolean uniqueSourceId,
                                         Index<Node> conceptIndex) {
        log.trace("Trying to look up existing concept by source ID and source ({}, {})", srcId, source);
        IndexHits<Node> indexHits = conceptIndex.get(PROP_SRC_IDS, srcId);
        if (!indexHits.hasNext())
            log.trace("    Did not find any concept with source ID {}", srcId);

        Node soughtConcept = null;
        boolean uniqueSourceIdNodeFound = false;

        while (indexHits.hasNext()) {
            Node conceptNode = indexHits.next();
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

    @Name(POP_CONCEPTS_FROM_SET)
    // TODO
    @Description("TODO")
    @PluginTarget(GraphDatabaseService.class)
    public Representation popConceptsFromSet(@Source GraphDatabaseService graphDb,
                                             @Description("TODO") @Parameter(name = KEY_LABEL) String labelString,
                                             @Description("TODO") @Parameter(name = KEY_AMOUNT) int amount) {
        Label label = Label.label(labelString);
        List<Node> poppedConcepts = new ArrayList<>(amount);

        try (Transaction tx = graphDb.beginTx()) {
            ResourceIterable<Node> nodesWithLabel = () -> graphDb.findNodes(label);
            {
                Node concept = null;
                int popCount = 0;
                for (Iterator<Node> it = nodesWithLabel.iterator(); it.hasNext() && popCount < amount; popCount++) {
                    concept = it.next();
                    poppedConcepts.add(concept);
                }
            }
            tx.success();
        }
        try (Transaction tx = graphDb.beginTx()) {
            // Remove the retrieved concepts from the set.
            for (Node concept : poppedConcepts) {
                concept.removeLabel(label);
            }
            tx.success();
        }
        Map<String, Object> retMap = new HashMap<>();
        retMap.put(RET_KEY_CONCEPTS, poppedConcepts);
        return new RecursiveMappingRepresentation(Representation.MAP, retMap);
    }

    @Name(PUSH_CONCEPTS_TO_SET)
    @Description("TODO")
    @PluginTarget(GraphDatabaseService.class)
    public long pushConceptsToSet(@Source GraphDatabaseService graphDb,
                                  @Description("TODO") @Parameter(name = KEY_CONCEPT_PUSH_CMD) String conceptPushCommandString,
                                  @Description("The amount of concepts to push into the set. If equal or less than zero or omitted, all concepts will be pushed.") @Parameter(name = KEY_AMOUNT, optional = true) Integer amount) {
        Gson gson = new Gson();
        PushConceptsToSetCommand cmd = gson.fromJson(conceptPushCommandString, PushConceptsToSetCommand.class);
        Label setLabel = Label.label(cmd.setName);
        Set<String> facetsWithSpecifiedGeneralLabel = new HashSet<>();
        ConceptSelectionDefinition eligibleConceptDefinition = cmd.eligibleConceptDefinition;
        ConceptSelectionDefinition excludeDefinition = cmd.excludeConceptDefinition;
        Label facetLabel = null;
        if (null != eligibleConceptDefinition && null != eligibleConceptDefinition.facetLabel)
            facetLabel = Label.label(eligibleConceptDefinition.facetLabel);
        String facetPropertyKey = null != eligibleConceptDefinition ? eligibleConceptDefinition.facetPropertyKey : "*";
        String facetPropertyValue = null != eligibleConceptDefinition ? eligibleConceptDefinition.facetPropertyValue
                : "*";

        // Get the facets for which we want to push concepts into the set.
        try (Transaction tx = graphDb.beginTx()) {
            Node facetGroupsNode = FacetManager.getFacetGroupsNode(graphDb);
            TraversalDescription facetTraversal = PredefinedTraversals.getFacetTraversal(graphDb, facetPropertyKey,
                    facetPropertyValue);
            Traverser traverse = facetTraversal.traverse(facetGroupsNode);
            for (Path path : traverse) {
                Node facetNode = path.endNode();
                // Filter for allowed facet labels.
                if (null != facetLabel && !facetNode.hasLabel(facetLabel))
                    continue;
                facetsWithSpecifiedGeneralLabel.add((String) facetNode.getProperty(FacetConstants.PROP_ID));
            }
            tx.success();
        }

        log.info("Deconceptined " + facetsWithSpecifiedGeneralLabel.size() + " facets with given restrictions.");

        long numberOfConceptsAdded = 0;
        long numberOfConceptsToAdd = null != amount && amount > 0 ? amount : Long.MAX_VALUE;
        try (Transaction tx = graphDb.beginTx()) {
            Label eligibleConceptLabel = ConceptLabel.CONCEPT;
            if (null != eligibleConceptDefinition && !StringUtils.isBlank(eligibleConceptDefinition.conceptLabel))
                eligibleConceptLabel = Label.label(eligibleConceptDefinition.conceptLabel);
            ResourceIterator<Node> conceptIt = graphDb.findNodes(eligibleConceptLabel);
            while (conceptIt.hasNext() && numberOfConceptsAdded < numberOfConceptsToAdd) {

                // Since Neo4j 2.0.0M6 it is not really possible to do this in a
                // batch manner because the concept iterator
                // access must happen inside a transaction.
                // try (Transaction tx = graphDb.beginTx()) {
                for (int i = 0; conceptIt.hasNext() && i < CONCEPT_INSERT_BATCH_SIZE
                        && numberOfConceptsAdded < numberOfConceptsToAdd; i++) {
                    Node concept = conceptIt.next();

                    if (null != excludeDefinition) {
                        boolean exclude = false;
                        String conceptPropertyKey = excludeDefinition.conceptPropertyKey;
                        String conceptPropertyValue = excludeDefinition.conceptPropertyValue;
                        Label conceptLabel = excludeDefinition.conceptLabel == null ? null
                                : Label.label(excludeDefinition.conceptLabel);
                        if (concept.hasProperty(conceptPropertyKey)) {
                            Object property = concept.getProperty(conceptPropertyKey);
                            if (property.getClass().isArray()) {
                                Object[] propertyArray = (Object[]) property;
                                for (Object o : propertyArray) {
                                    if (o.equals(conceptPropertyValue)) {
                                        exclude = true;
                                        break;
                                    }
                                }
                            } else {
                                if (property.equals(conceptPropertyValue))
                                    exclude = true;
                            }
                        }
                        if (concept.hasLabel(conceptLabel))
                            exclude = true;
                        if (exclude)
                            continue;
                    }

                    boolean hasFacetWithCorrectGeneralLabel = false;
                    if (null != eligibleConceptDefinition) {
                        if (concept.hasLabel(ConceptLabel.HOLLOW))
                            continue;
                        if (!concept.hasProperty(PROP_FACETS)) {
                            log.warn("Concept with internal ID " + concept.getId() + " has no facets property.");
                            continue;
                        }
                        String[] facetIds = (String[]) concept.getProperty(PROP_FACETS);
                        for (String facetId : facetIds) {
                            if (facetsWithSpecifiedGeneralLabel.contains(facetId)) {
                                hasFacetWithCorrectGeneralLabel = true;
                                break;
                            }
                        }
                    }
                    if (hasFacetWithCorrectGeneralLabel || null == eligibleConceptDefinition) {
                        concept.addLabel(setLabel);
                        numberOfConceptsAdded++;
                    }
                }
            }
            tx.success();
        }
        log.info("Finished pushing " + numberOfConceptsAdded + " concepts to set \"" + cmd.setName + "\".");
        return numberOfConceptsAdded;
    }

    @Name(UPDATE_CHILDREN_INFORMATION)
    @Description("Updates - or creates - the information which concept has children in which facets."
            + " This information is used in Semedico to either render an 'opening' arrow next to"
            + " a concept to display its children, or no 'drill-down' option depending on whether"
            + " the concept in question has children in the facet it is shown in or not.")
    @PluginTarget(GraphDatabaseService.class)
    public String updateChildrenInformation(@Source GraphDatabaseService graphDb) {
        try (Transaction tx = graphDb.beginTx()) {
            ResourceIterator<Node> conceptIt = graphDb.findNodes(ConceptLabel.CONCEPT);
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
            tx.success();
            return "success";
        }
    }

    @Name("include_concepts")
    @Description("This is only a remedy for a problem we shouldnt have, delete in the future.")
    @PluginTarget(GraphDatabaseService.class)
    public void includeConcepts(@Source GraphDatabaseService graphDb) throws JSONException {
        Label includeLabel = Label.label("INCLUDE");
        try (Transaction tx = graphDb.beginTx()) {
            ResourceIterable<Node> concepts = () -> graphDb.findNodes(includeLabel);
            for (Node concept : concepts)
                concept.removeLabel(includeLabel);
            tx.success();
        }

        Set<String> facetIds = new HashSet<>();
        try (Transaction tx = graphDb.beginTx()) {
            Node facetGroupsNode = FacetManager.getFacetGroupsNode(graphDb);
            TraversalDescription facetTraversal = PredefinedTraversals.getFacetTraversal(graphDb, null, null);
            Traverser traverse = facetTraversal.traverse(facetGroupsNode);
            for (Path path : traverse) {
                Node facetNode = path.endNode();
                facetIds.add((String) facetNode.getProperty(FacetConstants.PROP_ID));
            }
            log.info("Including concepts from facets " + facetIds);
            tx.success();
        }
        try (Transaction tx = graphDb.beginTx()) {
            ResourceIterable<Node> concepts = () -> graphDb.findNodes(ConceptLabel.CONCEPT);
            for (Node concept : concepts) {
                if (!concept.hasProperty(PROP_FACETS)) {
                    log.info("Doesnt have facets: " + PropertyUtilities.getNodePropertiesAsString(concept));
                    continue;
                }
                String[] facets = (String[]) concept.getProperty(PROP_FACETS);
                for (int i = 0; i < facets.length; i++) {
                    String string = facets[i];
                    if (facetIds.contains(string)) {
                        concept.addLabel(includeLabel);
                        break;
                    }
                }
            }
            tx.success();
        }
    }

    @Name("exclude_concepts")
    @Description("This is only a remedy for a problem we shouldnt have, delete in the future.")
    @PluginTarget(GraphDatabaseService.class)
    public void excludeConcepts(@Source GraphDatabaseService graphDb) throws JSONException {
        Label includeLabel = Label.label("INCLUDE");
        Label excludeLabel = Label.label("EXCLUDE");
        Label mappingAggregateLabel = Label.label("MAPPING_AGGREGATE");
        try (Transaction tx = graphDb.beginTx()) {
            ResourceIterable<Node> concepts = () -> graphDb.findNodes(ConceptLabel.CONCEPT);
            for (Node concept : concepts) {
                if (!concept.hasLabel(includeLabel)) {
                    concept.addLabel(excludeLabel);
                    concept.removeLabel(mappingAggregateLabel);
                    concept.removeLabel(ConceptLabel.AGGREGATE);
                    concept.removeLabel(ConceptLabel.CONCEPT);
                } else {
                    concept.addLabel(mappingAggregateLabel);
                }
            }
            tx.success();
        }
        try (Transaction tx = graphDb.beginTx()) {
            ResourceIterable<Node> concepts = () -> graphDb.findNodes(ConceptLabel.AGGREGATE);
            for (Node a : concepts) {
                a.removeLabel(ConceptLabel.AGGREGATE);
                a.removeLabel(mappingAggregateLabel);
            }
            tx.success();
        }

    }

    @Name(INSERT_MAPPINGS)
    @Description("Adds a set of concept mappings to the database. Here, a 'mapping'"
            + " between two concepts means that those concepts are 'similar' to one"
            + " another. The actual similarity - e.g. 'equal' or 'related' - is"
            + " defined by the type of the mapping. Here, all mappings are interpreted"
            + " as being symmetric. That does not mean that two relationships are created"
            + " but that reading commands don't care about the relationship direction.")
    @PluginTarget(GraphDatabaseService.class)
    public int insertMappings(@Source GraphDatabaseService graphDb,
                              @Description("An array of mappings in JSON format. Each mapping is an object with the keys for \"id1\", \"id2\" and \"mappingType\", respectively.") @Parameter(name = KEY_MAPPINGS) String mappingsJson)
            throws JSONException {
        JSONArray mappings = new JSONArray(mappingsJson);
        log.info("Starting to insert " + mappings.length() + " mappings.");
        try (Transaction tx = graphDb.beginTx()) {
            Index<Node> conceptIndex = graphDb.index().forNodes(ConceptConstants.INDEX_NAME);
            Map<String, Node> nodesBySrcId = new HashMap<>(mappings.length());
            InsertionReport insertionReport = new InsertionReport();

            for (int i = 0; i < mappings.length(); i++) {
                JSONObject mapping = mappings.getJSONObject(i);
                String id1 = mapping.getString("id1");
                String id2 = mapping.getString("id2");
                String mappingType = mapping.getString("mappingType");

                log.debug("Inserting mapping " + id1 + " -" + mappingType + "- " + id2);

                if (StringUtils.isBlank(id1))
                    throw new IllegalArgumentException("id1 in mapping \"" + mapping + "\" is missing.");
                if (StringUtils.isBlank(id2))
                    throw new IllegalArgumentException("id2 in mapping \"" + mapping + "\" is missing.");
                if (StringUtils.isBlank(mappingType))
                    throw new IllegalArgumentException("mappingType in mapping \"" + mapping + "\" is missing.");

                Node n1 = nodesBySrcId.get(id1);
                if (null == n1) {
                    IndexHits<Node> indexHits = conceptIndex.get(PROP_SRC_IDS, id1);
                    if (indexHits.size() > 1) {
                        log.error("More than one node for source ID {}", id1);
                        for (Node n : indexHits)
                            log.error(NodeUtilities.getNodePropertiesAsString(n));
                        throw new IllegalStateException("More than one node for source ID " + id1);
                    }
                    n1 = indexHits.getSingle();
                    if (null == n1) {
                        log.warn("There is no concept with source ID \"" + id1 + "\" as required by the mapping \""
                                + mapping + "\" Mapping is skipped.");
                        continue;
                    }
                    nodesBySrcId.put(id1, n1);
                }
                Node n2 = nodesBySrcId.get(id2);
                if (null == n2) {
                    n2 = conceptIndex.get(PROP_SRC_IDS, id2).getSingle();
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
                    for (int j = 0; j < n1Facets.length; j++) {
                        String facet = n1Facets[j];
                        n1FacetSet.add(facet);
                    }
                    for (int j = 0; j < n2Facets.length; j++) {
                        String facet = n2Facets[j];
                        n2FacetSet.add(facet);
                    }
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
            tx.success();
            log.info(insertionReport.numRelationships + " of " + mappings.length()
                    + " new mappings successfully added.");
            return insertionReport.numRelationships;
        }
    }

    @Name(GET_FACET_ROOTS)
    @Description("Returns root concepts for the facets with specified IDs. Can also be restricted to particular roots which is useful for facets that have a lot of roots.")
    @PluginTarget(GraphDatabaseService.class)
    public MappingRepresentation getFacetRoots(@Source GraphDatabaseService graphDb,
                                               @Description("An array of facet IDs in JSON format.") @Parameter(name = KEY_FACET_IDS) String facetIdsJson,
                                               @Description("An array of concept IDs to restrict the retrieval to.") @Parameter(name = KEY_CONCEPT_IDS, optional = true) String conceptIdsJson,
                                               @Description("Restricts the facets to those that have at most the specified number of roots.") @Parameter(name = KEY_MAX_ROOTS, optional = true) long maxRoots)
            throws JSONException {
        Map<String, Object> facetRoots = new HashMap<>();

        JSONArray facetIdsArray = new JSONArray(facetIdsJson);
        Set<String> requestedFacetIds = new HashSet<>();
        for (int i = 0; i < facetIdsArray.length(); i++)
            requestedFacetIds.add(facetIdsArray.getString(i));

        Map<String, Set<String>> requestedConceptIds = null;
        if (!StringUtils.isBlank(conceptIdsJson) && !conceptIdsJson.equals("null")) {
            JSONObject conceptIdsObject = new JSONObject(conceptIdsJson);
            requestedConceptIds = new HashMap<>();
            JSONArray facetIds = conceptIdsObject.names();
            for (int i = 0; null != facetIds && i < facetIds.length(); i++) {
                String facetId = facetIds.getString(i);
                JSONArray requestedRootIdsForFacet = conceptIdsObject.getJSONArray(facetId);
                Set<String> idSet = new HashSet<>();
                for (int j = 0; j < requestedRootIdsForFacet.length(); j++)
                    idSet.add(requestedRootIdsForFacet.getString(j));
                requestedConceptIds.put(facetId, idSet);
            }
        }

        try (Transaction tx = graphDb.beginTx()) {
            log.info("Returning roots for facets " + requestedFacetIds);
            Node facetGroupsNode = FacetManager.getFacetGroupsNode(graphDb);
            TraversalDescription facetTraversal = PredefinedTraversals.getFacetTraversal(graphDb, null, null);
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
                if (requestedFacetIds.contains(facetId)) {
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
            tx.success();
        }

        return new RecursiveMappingRepresentation(Representation.MAP, facetRoots);
    }

    @Name(ADD_CONCEPT_CONCEPT)
    @Description("Allows to add writing variants and acronyms to concepts in the database. For each type of data (variants and acronyms) there is a parameter of its own."
            + " It is allowed to omit a parameter value. The expected format is"
            + " {'tid1': {'docID1': {'variant1': count1, 'variant2': count2, ...}, 'docID2': {...}}, 'tid2':...} for both variants and acronyms.")
    @PluginTarget(GraphDatabaseService.class)
    public void addWritingVariants(@Source GraphDatabaseService graphDb,
                                   @Description("A JSON object mapping concept IDs to an array of writing variants to add to the existing writing variants.") @Parameter(name = KEY_CONCEPT_TERMS, optional = true) String conceptVariants,
                                   @Description("A JSON object mapping concept IDs to an array of acronyms to add to the existing concept acronyms.") @Parameter(name = KEY_CONCEPT_ACRONYMS, optional = true) String conceptAcronyms)
            throws JSONException {
        if (null != conceptVariants)
            addConceptVariant(graphDb, conceptVariants, "writingVariants");
        if (null != conceptAcronyms)
            addConceptVariant(graphDb, conceptAcronyms, "acronyms");
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
     * @param graphDb
     * @param conceptVariants
     * @param type
     */
    private void addConceptVariant(GraphDatabaseService graphDb, String conceptVariants, String type) {
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
                    Node concept = graphDb.findNode(ConceptLabel.CONCEPT, PROP_ID, conceptId);
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
                        Node variantsNode = graphDb.createNode(variantsAggregationLabel);
                        hasVariantsRel = concept.createRelationshipTo(variantsNode, variantRelationshipType);
                    }
                    Node variantsNode = hasVariantsRel.getEndNode();
                    for (String docId : variantCountsInDocs.keySet()) {
                        Map<String, Integer> variantCounts = variantCountsInDocs.get(docId);
                        for (String variant : variantCounts.keySet()) {
                            String normalizedVariant = TermVariantComparator.normalizeVariant(variant);
                            Node variantNode = graphDb.findNode(variantNodeLabel, MorphoConstants.PROP_ID,
                                    normalizedVariant);
                            if (null == variantNode) {
                                variantNode = graphDb.createNode(variantNodeLabel);
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
                tx.success();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static enum EdgeTypes implements RelationshipType {
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

    public static enum ConceptLabel implements Label {
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
        HOLLOW, CONCEPT,
        /**
         * @deprecated It doesn't seem this label is required or used anywhere
         */
        @Deprecated
        AGGREGATE_ELEMENT
    }

    /**
     * Labels for nodes representing lexico-morphological variations of concepts.
     */
    public static enum MorphoLabel implements Label {
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
