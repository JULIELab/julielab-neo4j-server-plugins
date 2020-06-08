package de.julielab.neo4j.plugins.concepts;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.julielab.neo4j.plugins.Indexes;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;
import de.julielab.neo4j.plugins.datarepresentation.ImportMapping;
import de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.ImportIERelations;
import de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.NodeIDPrefixConstants;
import de.julielab.neo4j.plugins.datarepresentation.util.ConceptsJsonSerializer;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.logging.slf4j.Slf4jLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Path;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.*;
import java.util.*;

import static de.julielab.neo4j.plugins.concepts.ConceptManager.CM_REST_ENDPOINT;
import static de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants.PROP_CHILDREN_IN_FACETS;
import static de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants.PROP_SRC_IDS;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@Path("/" + CM_REST_ENDPOINT)
public class ConceptManager {

    public static final String CM_REST_ENDPOINT = "concept_manager";

    public static final String INSERT_MAPPINGS = "insert_mappings";


    public static final String GET_CHILDREN_OF_CONCEPTS = "get_children_of_concepts";
    public static final String GET_PATHS_FROM_FACET_ROOTS = "get_paths_to_facetroots";
    public static final String INSERT_CONCEPTS = "insert_concepts";
    public static final String GET_FACET_ROOTS = "get_facet_roots";
    public static final String ADD_CONCEPT_TERM = "add_concept_term";
    public static final String INSERT_IE_RELATIONS = "insert_ie_relations";
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

    /**
     * This is a rather arbitrary number. It should just set high enough that all source IDs given to concepts
     * lie below this number. The reason is that we store the source IDs in properties named 'sourceIDsX' where X
     * is the Xth source ID occurring for a particular concept (meaning that multiple source defined the concept
     * as identified via its original source (ID)). We need an index on each sourceIDsX property. This is where this
     * numnber comes into play: We create indexes for all 'sourceIDsX' property with X < MAX_SRC_IDS in {@link #createIndexes(Transaction)}.
     */
    public static final int MAX_SRC_IDS = 10;

    public static final String UPDATE_CHILD_INFORMATION = "update_children_information";
    public static final String UNKNOWN_CONCEPT_SOURCE = "<unknown>";
    private static final Logger log = LoggerFactory.getLogger(ConceptManager.class);


    private final DatabaseManagementService dbms;

    public ConceptManager(@Context DatabaseManagementService dbms) {
        this.dbms = dbms;
    }


    public static void createIndexes(Transaction tx) {
        Indexes.createSinglePropertyIndexIfAbsent(tx, "ConceptId", ConceptLabel.CONCEPT, true, Indexes.PROVIDER_NATIVE_1_0, ConceptConstants.PROP_ID);
        // The org ID can actually be duplicated. Only the composite (orgId,orgSource) should be unique but this isn't supported
        // by schema indexes it seems
        Indexes.createSinglePropertyIndexIfAbsent(tx, "OriginalId", ConceptLabel.CONCEPT, false, Indexes.PROVIDER_NATIVE_1_0, ConceptConstants.PROP_ORG_ID);
        Indexes.createSinglePropertyIndexIfAbsent(tx, "FacetRoots", NodeConstants.Labels.ROOT, true, Indexes.PROVIDER_NATIVE_1_0, NodeConstants.PROP_NAME);
        // We need to create multiple indexes for multiple source IDs that single concepts may have. Also see the comment for the MAX_SRC_IDS constant.
        for (int i = 0; i < MAX_SRC_IDS; ++i)
            Indexes.createSinglePropertyIndexIfAbsent(tx, "ConceptSrcId" + i, ConceptLabel.CONCEPT, false, Indexes.PROVIDER_NATIVE_1_0, PROP_SRC_IDS + i);
    }

    public static Response getErrorResponse(Throwable throwable) {
        StringWriter sw = new StringWriter();
        throwable.printStackTrace(new PrintWriter(sw));
        return Response.serverError().entity(sw.toString()).build();
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
    @Path(GET_CHILDREN_OF_CONCEPTS)
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
    @Path(GET_PATHS_FROM_FACET_ROOTS)
    public Object getPathsFromFacetRoots(@QueryParam(KEY_CONCEPT_IDS) String conceptIdsCsv, @QueryParam(KEY_ID_PROPERTY) String idProperty, @QueryParam(KEY_RETURN_ID_PROPERTY) String returnIdProperty, @QueryParam(KEY_SORT_RESULT) boolean sort, @QueryParam(KEY_FACET_ID) String facetId) {
        try {
            final List<String> conceptIds = Arrays.asList(conceptIdsCsv.split(","));
            GraphDatabaseService graphDb = dbms.database(DEFAULT_DATABASE_NAME);
            try (Transaction tx = graphDb.beginTx()) {
                Map<String, Object> pathsWrappedInMap = ConceptRetrieval.getPathsFromFacetRoots(tx, conceptIds, idProperty, returnIdProperty, sort, facetId);
                return Response.ok(pathsWrappedInMap).build();
            }
        } catch (Throwable t) {
            return getErrorResponse(t);
        }
    }

    /**
     * Calls {@link #insertConcepts(InputStream, Log)} with the SLF4J logger defined in this class.
     *
     * @param is The concepts input.
     * @return The JavaX RS response.
     */
    public Object insertConcepts(InputStream is) {
        return insertConcepts(is, new Slf4jLog(log));
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path(INSERT_CONCEPTS)
    public Object insertConcepts(InputStream is, @Context Log log) {
        try {
            log.info("%s was called", INSERT_CONCEPTS);

            InsertionReport insertionReport;
            log.debug("Beginning processing of concept insertion.");
            GraphDatabaseService graphDb = dbms.database(DEFAULT_DATABASE_NAME);
            Map<String, Object> response = new HashMap<>();
            insertionReport = ConceptInsertion.insertConcepts(log, graphDb, is, response);
            log.info("Concept insertion complete.");
            log.info("%s is finished processing after %s ms. %s concepts and %s relationships have been created.", INSERT_CONCEPTS, response.get(KEY_TIME), insertionReport.numConcepts, insertionReport.numRelationships, response.get(KEY_TIME));
            return Response.ok(response).build();
        } catch (Throwable throwable) {
            log.error("Concept insertion failed", throwable);
            return getErrorResponse(throwable);
        } finally {

        }
    }


    /**
     * Updates - or creates - the information which concept has children in which facets.
     * This information is used in Semedico to either render an 'opening' arrow next to
     * a concept to display its children, or no 'drill-down' option depending on whether
     * the concept in question has children in the facet it is shown in or not.
     */
    @POST
    @Path(UPDATE_CHILD_INFORMATION)
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
     * @param is The mappings in JSON format, wrapped in an InputStream.
     * @return The number of insertes mappings.
     * @throws IOException If the input JSON cannot be read.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path(INSERT_MAPPINGS)
    public int insertMappings(InputStream is) throws IOException {
        // ObjectMapper.readValues(JsonParser, ...) will NOT work for an array, as
        // that assumes a non-wrapped sequence of values. See source of MappingIterator.
        // (comment taken from https://gist.github.com/KlausBrunner/9915362)
        Iterator<ImportMapping> importMappingIterator = new ObjectMapper().readerFor(ImportMapping.class).readValues(is);
        log.info("Starting to insert mappings.");
        GraphDatabaseService graphDb = dbms.database(DEFAULT_DATABASE_NAME);
        try (Transaction tx = graphDb.beginTx()) {
            return ConceptInsertion.insertMappings(tx, importMappingIterator);
        }
    }

    /**
     * Convenience access to {@link #getFacetRoots(UriInfo, Log)}.
     *
     * @param uriInfo
     * @return
     */
    public Object getFacetRoots(@Context UriInfo uriInfo) {
        return getFacetRoots(uriInfo, new Slf4jLog(log));
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
    @Path(GET_FACET_ROOTS)
    public Object getFacetRoots(@Context UriInfo uriInfo, @Context Log log) {
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
    @Path(ADD_CONCEPT_TERM)
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
                ConceptTermInsertion.addConceptVariant(tx, conceptAcronyms, "acronyms");
            tx.commit();
        }
    }

    /**
     * <p>Adds semantic relations between concepts of the database as extracted from literature with information retrieval techniques.</p>
     * <p>Alternatively, adds similar relations between concepts but taken from some database.</p>
     * <p>
     * To insert relations from Java, it is easiest to create objects of {@link ImportIERelations} and convert them
     * to JSON with Jackson or another to-JSON serializer. The expected JSON format is as follows:
     * <pre>
     * {
     *     "id_property": &lt;one of "id", "sourceIds", "originalId"&gt;,
     *     "id_source":   &lt;optional; will be used as a default when not given per concept Id&gt;
     *     "documents": [{
     *              "name": "&lt;docId or DB name&gt;",
     *              "isDb": &lt;true/false&gt;,
     *              "relations": [{
     *                  "&lt;relationshipType1&gt;": [
     *                      {
     *                          "count": &lt;optional, not used when document is a DB; count1&gt;,
     *                          "method": &lt;optional;finding method for DB entries&gt;
     *                          "args": [
     *                              ["&lt;concept id 1&gt;", "&lt;id 1 source if id_property not 'id' and not from 'id_source'&gt;],
     *                              ["&lt;concept id 2&gt;"],
     *                              ["&lt;otherIdProperty&gt;:&lt;concept id 3&gt;", "&lt;id 3 source if otherIdProperty is not 'id'&gt;"]
     *                          ]
     *                      },
     *                      {
     *                          "count": &lt;count2&gt;,
     *                          "args": [
     *                              [...]
     *                          ]
     *                      },
     *                      ...
     *                  ],
     *                  "&lt;relationshipType2&gt;": [
     *                      ...
     *                  ]
     *              }]
     *          },
     *          {
     *              "name": "&lt;docId2&gt;",
     *              "relations:" [{
     *                  "&lt;relationshipType2&gt;": [
     *                      {
     *                          ...
     *                      },
     *                      {
     *                          ...
     *                      },
     *                  ]
     *              }]
     *          ]
     *      }
     *  }
     * </pre>
     * </p>
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path(INSERT_IE_RELATIONS)
    public Response insertIERelations(InputStream is, @Context Log log) {
        GraphDatabaseService graphDb = dbms.database(DEFAULT_DATABASE_NAME);
        try (Transaction tx = graphDb.beginTx()) {
            IERelationInsertion.insertRelations(is, tx, log);
            tx.commit();
            return Response.ok().build();
        } catch (Throwable t) {
            log.error("Error in IE relation insertion.", t);
            return getErrorResponse(t);
        }
    }

    /**
     * Convenience access to {@link #insertIERelations(InputStream)}.
     *
     * @param is
     */
    public void insertIERelations(InputStream is) {
        insertIERelations(is, new Slf4jLog(log));
    }

    public void insertIERelations(ImportIERelations relations) {
        insertIERelations(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(relations).getBytes(UTF_8)));
    }

    /**
     * Convenience access to {@link #insertConcepts(InputStream)} and, as a consequence, {@link #insertConcepts(InputStream, Log)}.
     *
     * @param importConcepts
     */
    public void insertConcepts(ImportConcepts importConcepts) {
        insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(importConcepts).getBytes(UTF_8)));
    }
}
