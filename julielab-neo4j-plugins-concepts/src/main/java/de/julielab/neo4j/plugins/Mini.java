package de.julielab.neo4j.plugins;

import de.julielab.neo4j.plugins.auxiliaries.semedico.CoordinatesSet;
import de.julielab.neo4j.plugins.auxiliaries.semedico.NodeUtilities;
import de.julielab.neo4j.plugins.datarepresentation.ConceptCoordinates;
import de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.*;

import static de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants.PROP_SRC_IDS;

@javax.ws.rs.Path("/hallo")
public class Mini {


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

    public Mini(@Context DatabaseManagementService dbms) {
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
        if (!aggregate.hasLabel(ConceptManager.ConceptLabel.AGGREGATE))
            throw new IllegalArgumentException(
                    "Node " + NodeUtilities.getNodePropertiesAsString(aggregate) + " is not an aggregate.");
        Iterable<Relationship> elementRels = aggregate.getRelationships(Direction.OUTGOING, ConceptManager.EdgeTypes.HAS_ELEMENT);
        List<String> elementValues = new ArrayList<>();
        for (Relationship elementRel : elementRels) {
            String[] value = NodeUtilities.getNodePropertyAsStringArrayValue(elementRel.getEndNode(), property);
            for (int i = 0; value != null && i < value.length; i++)
                elementValues.add(value[i]);
        }
        return elementValues.isEmpty() ? null : elementValues.toArray(new String[elementValues.size()]);
    }

    public static void createIndexes(Transaction tx) {
        Indexes.createSinglePropertyIndexIfAbsent(tx, ConceptManager.ConceptLabel.CONCEPT, true, ConceptConstants.PROP_ID);
        // The org ID can actually be duplicated. Only the composite (orgId,orgSource) should be unique but this isn't supported
        // by schema indexes it seems
        Indexes.createSinglePropertyIndexIfAbsent(tx, ConceptManager.ConceptLabel.CONCEPT, false, ConceptConstants.PROP_ORG_ID);
        Indexes.createSinglePropertyIndexIfAbsent(tx, NodeConstants.Labels.ROOT, true, NodeConstants.PROP_NAME);
        FullTextIndexUtils.createTextIndex(tx, FULLTEXT_INDEX_CONCEPTS, Map.of("analyzer", "whitespace"), ConceptManager.ConceptLabel.CONCEPT, PROP_SRC_IDS);
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
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("kuschel")
    public String buildAggregatesByMappings(String jsonParameterObject)
            throws IOException {
        return "Haaaaallo Schaaaaaatzi!!!!!!!";
//        ObjectMapper om = new ObjectMapper();
//        Map<String, Object> parameterMap = om.readValue(jsonParameterObject, Map.class);
//        final Set<String> allowedMappingTypes = new HashSet<>((List<String>) parameterMap.get(KEY_ALLOWED_MAPPING_TYPES));
//        Label aggregatedConceptsLabel = Label.label((String) parameterMap.get(KEY_AGGREGATED_LABEL));
//        Label allowedConceptLabel = parameterMap.containsKey(KEY_LABEL) ? Label.label((String) parameterMap.get(KEY_LABEL))
//                : null;
//        log.info("Creating mapping aggregates for concepts with label {} and mapping types {}", allowedConceptLabel,
//                allowedMappingTypes);
//        GraphDatabaseService graphDb = dbms.database(DEFAULT_DATABASE_NAME);
//        try (Transaction tx = graphDb.beginTx()) {
//            ConceptAggregateBuilder.buildAggregatesForMappings(tx, allowedMappingTypes, allowedConceptLabel,
//                    aggregatedConceptsLabel);
//            tx.commit();
//        }
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
         * Such concepts have {@link ConceptManager.EdgeTypes#HAS_ELEMENT} relationships to concepts,
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
