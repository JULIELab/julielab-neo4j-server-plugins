package de.julielab.neo4j.plugins.concepts;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.julielab.neo4j.plugins.FacetManager;
import de.julielab.neo4j.plugins.FacetManager.FacetLabel;
import de.julielab.neo4j.plugins.FullTextIndexUtils;
import de.julielab.neo4j.plugins.Indexes;
import de.julielab.neo4j.plugins.auxiliaries.semedico.CoordinatesMap;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcept;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;
import de.julielab.neo4j.plugins.datarepresentation.ImportFacet;
import de.julielab.neo4j.plugins.datarepresentation.ImportOptions;
import de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.NodeIDPrefixConstants;
import de.julielab.neo4j.plugins.datarepresentation.util.ConceptsJsonSerializer;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.*;

import static de.julielab.neo4j.plugins.concepts.ConceptInsertion.createRelationships;
import static de.julielab.neo4j.plugins.concepts.ConceptManager.CM_REST_ENDPOINT;
import static de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants.PROP_CHILDREN_IN_FACETS;
import static de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants.PROP_SRC_IDS;
import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_ID;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@javax.ws.rs.Path("/" + CM_REST_ENDPOINT)
public class ConceptManager {

    public static final String CM_REST_ENDPOINT = "concept_manager";

    public static final String INSERT_MAPPINGS = "insert_mappings";


    public static final String GET_CHILDREN_OF_CONCEPTS = "get_children_of_concepts";
    public static final String GET_PATHS_FROM_FACET_ROOTS = "get_paths_to_facetroots";
    public static final String INSERT_CONCEPTS = "insert_concepts";
    public static final String GET_FACET_ROOTS = "get_facet_roots";
    public static final String ADD_CONCEPT_TERM = "add_concept_term";
    public static final String KEY_FACET = "facet";
    public static final String KEY_FACET_ID = "facetId";
    public static final String KEY_FACET_IDS = "facetIds";
    public static final String KEY_ID_PROPERTY = "id_property";
    public static final String KEY_RETURN_ID_PROPERTY = "return_id_property";
    public static final String KEY_LABEL = "label";
    public static final String KEY_SORT_RESULT = "sortResult";
    public static final String KEY_CONCEPT_IDS = "conceptIds";
    public static final String KEY_MAX_ROOTS = "maxRoots";
    public static final String KEY_CONCEPT_TERMS = "conceptTerms";
    public static final String KEY_CONCEPT_ACRONYMS = "conceptAcronyms";
    /**
     * The key of the map to send to the {@link #INSERT_CONCEPTS} endpoint.
     */
    public static final String KEY_CONCEPTS = "concepts";
    public static final String KEY_TIME = "time";
    public static final String RET_KEY_CHILDREN = "children";
    public static final String RET_KEY_NUM_AGGREGATES = "numAggregates";
    public static final String RET_KEY_NUM_ELEMENTS = "numElements";
    public static final String RET_KEY_NUM_PROPERTIES = "numProperties";
    public static final String RET_KEY_NUM_CREATED_RELS = "numCreatedRelationships";
    public static final String RET_KEY_NUM_CREATED_CONCEPTS = "numCreatedConcepts";
    public static final String RET_KEY_PATHS = "paths";
    public static final String RET_KEY_RELTYPES = "reltypes";

    public static final String FULLTEXT_INDEX_CONCEPTS = "concepts";

    public static final String UPDATE_CHILD_INFORMATION = "update_children_information";
    public static final String UNKNOWN_CONCEPT_SOURCE = "<unknown>";
    private static final Logger log = LoggerFactory.getLogger(ConceptManager.class);

    private final DatabaseManagementService dbms;

    public ConceptManager(@Context DatabaseManagementService dbms) {
        this.dbms = dbms;
    }



    public static void createIndexes(Transaction tx) {
        Indexes.createSinglePropertyIndexIfAbsent(tx, ConceptLabel.CONCEPT, true, ConceptConstants.PROP_ID);
        // The org ID can actually be duplicated. Only the composite (orgId,orgSource) should be unique but this isn't supported
        // by schema indexes it seems
        Indexes.createSinglePropertyIndexIfAbsent(tx, ConceptLabel.CONCEPT, false, ConceptConstants.PROP_ORG_ID);
        Indexes.createSinglePropertyIndexIfAbsent(tx, NodeConstants.Labels.ROOT, true, NodeConstants.PROP_NAME);
        FullTextIndexUtils.createTextIndex(tx, FULLTEXT_INDEX_CONCEPTS, Map.of("analyzer", "whitespace"), new Label[]{ConceptLabel.CONCEPT, ConceptLabel.HOLLOW}, new String[]{PROP_SRC_IDS});
    }

    public static Response getErrorResponse(Throwable throwable) {
        return Response.serverError().entity(throwable.getMessage() != null ? throwable.getMessage() : throwable).build();
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
    public Object getChildrenOfConcepts(@QueryParam(KEY_CONCEPT_IDS) String conceptIdsCsv, @QueryParam(KEY_LABEL) String labelString) {
        try {
            Label label = labelString != null ? Label.label(labelString) : ConceptLabel.CONCEPT;
            final List<String> conceptIds = Arrays.asList(conceptIdsCsv.split(","));
            GraphDatabaseService graphDb = dbms.database(DEFAULT_DATABASE_NAME);
            try (Transaction tx = graphDb.beginTx()) {
                Map<String, Object> childrenByConceptId = ConceptRetrieval.getChildrenOfConcepts(tx, conceptIds, label);
                return Response.ok(childrenByConceptId);
            }
        } catch (Throwable t) {
            return getErrorResponse(t);
        }
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
    @javax.ws.rs.Path(GET_PATHS_FROM_FACET_ROOTS)
    public Object getPathsFromFacetRoots(@QueryParam(KEY_CONCEPT_IDS) String conceptIdsCsv, @QueryParam(KEY_ID_PROPERTY) String idProperty, @QueryParam(KEY_RETURN_ID_PROPERTY) String returnIdProperty, @QueryParam(KEY_SORT_RESULT) boolean sort, @QueryParam(KEY_FACET_ID) String facetId) {
        try {
            final List<String> conceptIds = Arrays.asList(conceptIdsCsv.split(","));
            GraphDatabaseService graphDb = dbms.database(DEFAULT_DATABASE_NAME);
            try (Transaction tx = graphDb.beginTx()) {

                Map<String, Object> pathsWrappedInMap = ConceptRetrieval.getPathsFromFacetRoots(tx, conceptIds, idProperty, returnIdProperty, sort, facetId);
                return Response.ok(pathsWrappedInMap);
            }
        } catch (Throwable t) {
            return getErrorResponse(t);
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @javax.ws.rs.Path(INSERT_CONCEPTS)
    public Object insertConcepts(String jsonParameterObject) {
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
                    insertionReport = ConceptInsertion.insertConcepts(tx, jsonConcepts, facetId, nodesByCoordinates, importOptions);
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
            return getErrorResponse(throwable);
        }
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
                    if (type.startsWith(ConceptEdgeTypes.IS_BROADER_THAN.toString())) {
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
                            facetsContainingChildren.toArray(new String[0]));
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
     * Parameter:  An array of mappings in JSON format. Each mapping is an object with the keys for "id1", "id2" and "mappingType", respectively.
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
            return ConceptInsertion.insertMappings(tx, mappings);
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
     * @return A JSON object with one key for each facet that returns roots. The values are lists of concept IDs.
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @javax.ws.rs.Path(GET_FACET_ROOTS)
    public Object getFacetRoots(@Context UriInfo uriInfo) {
        try {
                Set<String> requestedFacetIds = new HashSet<>();
                MultivaluedMap<String, String> queryParameters = uriInfo.getQueryParameters();
                Map<String, Set<String>> requestedConceptIds = new HashMap<>();
                int maxRoots = 0;
                for (String param : queryParameters.keySet()) {
                    if (param.equals(KEY_FACET_IDS))
                        requestedFacetIds.addAll(Arrays.asList(queryParameters.getFirst(param).split(",")));
                    else if (param.equals(KEY_MAX_ROOTS))
                        maxRoots = Integer.parseInt(queryParameters.getFirst(param));
                    else {
                        requestedFacetIds.add(param);
                        requestedConceptIds.put(param, Set.of(queryParameters.getFirst(param).split(",")));
                    }
                }
            GraphDatabaseService graphDb = dbms.database(DEFAULT_DATABASE_NAME);
            try (Transaction tx = graphDb.beginTx()) {
                Map<String, List<Node>> facetRoots = FacetRootsRetrieval.getFacetRoots(tx, requestedFacetIds, requestedConceptIds, maxRoots);
                return Response.ok(facetRoots);
            }
        } catch (Throwable t) {
            return getErrorResponse(t);
        }
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
        var parameterMap = om.readValue(jsonParameterObject, Map.class);
        String conceptVariants = (String) parameterMap.get(KEY_CONCEPT_TERMS);
        String conceptAcronyms = (String) parameterMap.get(KEY_CONCEPT_ACRONYMS);
        GraphDatabaseService graphDb = dbms.database(DEFAULT_DATABASE_NAME);
        try (Transaction tx = graphDb.beginTx()) {
            if (null != conceptVariants)
                ConceptTermInsertion.addConceptVariant(tx, conceptVariants, "writingVariants");
            if (null != conceptAcronyms)
                ConceptTermInsertion.addConceptVariant(tx,  conceptAcronyms, "acronyms");
        }
    }



}
