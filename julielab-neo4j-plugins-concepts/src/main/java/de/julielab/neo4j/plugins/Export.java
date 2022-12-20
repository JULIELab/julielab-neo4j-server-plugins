package de.julielab.neo4j.plugins;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.julielab.neo4j.plugins.auxiliaries.LogUtilities;
import de.julielab.neo4j.plugins.auxiliaries.PropertyUtilities;
import de.julielab.neo4j.plugins.auxiliaries.semedico.NodeUtilities;
import de.julielab.neo4j.plugins.auxiliaries.semedico.PredefinedTraversals;
import de.julielab.neo4j.plugins.concepts.ConceptAggregateManager;
import de.julielab.neo4j.plugins.concepts.ConceptEdgeTypes;
import de.julielab.neo4j.plugins.concepts.ConceptLabel;
import de.julielab.neo4j.plugins.concepts.ConceptManager;
import de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.MorphoConstants;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.logging.Log;
import org.neo4j.server.rest.repr.RecursiveMappingRepresentation;
import org.neo4j.server.rest.repr.Representation;

import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.StreamingOutput;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import static de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants.*;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@javax.ws.rs.Path("/export")
public class Export {

    public static final String HYPERNYMS = "hypernyms";
    public static final String LINGPIPE_DICT = "lingpipe_dictionary";
    public static final String CONCEPT_TO_FACET = "concept_facet_map";
    public static final String CONCEPT_ID_MAPPING = "concept_id_mapping";
    public static final String PARAM_UNIQUE_KEYS = "unique_keys";
    public static final String PARAM_SOURCE_ID_PROPERTY = "source_id_property";
    public static final String PARAM_ADD_SOURCE_PREFIX = "add_source_prefix";
    public static final String PARAM_TARGET_ID_PROPERTY = "target_id_property";
    public static final String PARAM_FACET_NAMES = "facet_names";
    public static final String PARAM_LABELS = "labels";
    public static final String PARAM_LABEL = "label";
    public static final String PARAM_EXCLUSION_LABEL = "exclusion_label";
    @Deprecated
    public static final String PARAM_FURTHER_PROPERTIES = "further_properties";
    public static final int OUTPUTSTREAM_INIT_SIZE = 200000000;
    public static final int HYPERNYMS_CACHE_SIZE = 100000;
    private static final Logger log = Logger.getLogger(Export.class.getName());
    private final DatabaseManagementService dbms;

    public Export(@Context DatabaseManagementService dbms) {
        this.dbms = dbms;
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @javax.ws.rs.Path(CONCEPT_ID_MAPPING)
    public Object exportIdMapping(@QueryParam(PARAM_SOURCE_ID_PROPERTY) String sourceIdProperty, @QueryParam(PARAM_TARGET_ID_PROPERTY) String targetIdProperty, @QueryParam(PARAM_LABELS) String labelStrings, @Context Log log) {
        try {
            final ObjectMapper om = new ObjectMapper();
            log(log, "info", "Exporting ID mapping data.");
            String[] labelsArray = null != labelStrings ? om.readValue(labelStrings, String[].class) : new String[]{ConceptLabel.CONCEPT.name()};
            String sProperty = sourceIdProperty != null ? sourceIdProperty : PROP_SRC_IDS;
            String tProperty = targetIdProperty != null ? targetIdProperty : PROP_ID;
            log(log, "info", "Creating mapping file content with source property %s, target property %s and node labels %s", sourceIdProperty, targetIdProperty, labelStrings);
            return (StreamingOutput) output -> {
                try {
                    createIdMapping(output, sProperty, tProperty, labelsArray);
                } catch (Exception e) {
                    log(log, "error", "Exception occurred during concept ID output streaming.", e);
                    e.printStackTrace();
                }
            };
        } catch (Throwable t) {
            log(log, "error", "Could not export concept ID mappings", t);
            return ConceptManager.getErrorResponse(t);
        }
    }

    public Object exportIdMapping(String sourceIdProperty, String targetIdProperty, String labelStrings) {
        return exportIdMapping(sourceIdProperty, targetIdProperty, labelStrings, LogUtilities.getLogger(Export.class));
    }

    /**
     * Helper method for logging. Checks if the Log instance is null. If so, no logging occurs and no error is thrown.
     *
     * @param log       The logger.
     * @param level     The log level - debug, info, warn or error.
     * @param fmt       The logging format string.
     * @param arguments The format string arguments.
     */
    private void log(@Nullable Log log, String level, String fmt, Object... arguments) {
        if (log != null) {
            switch (level) {
                case "error":
                    log.error(fmt, arguments);
                    break;
                case "warn":
                    log.warn(fmt, arguments);
                    break;
                case "info":
                    log.info(fmt, arguments);
                    break;
                case "debug":
                    log.debug(fmt, arguments);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported log level '" + level + "'.");
            }
        }
    }

    private void createIdMapping(OutputStream os, String sourceIdProperty, String targetIdProperty,
                                 String[] labelsArray) throws Exception {
        GraphDatabaseService graphDb = dbms.database(DEFAULT_DATABASE_NAME);
        try (Transaction tx = graphDb.beginTx()) {
            int numWritten = 0;
            for (String labelString : labelsArray) {
                Label label = Label.label(labelString);
                for (ResourceIterator<Node> terms = tx.findNodes(label); terms.hasNext(); ) {
                    Node n = terms.next();
                    Object sourceIdObject = sourceIdProperty.equals(PROP_SRC_IDS) ? NodeUtilities.getSourceIdArray(n) : PropertyUtilities.getNonNullNodeProperty(n, sourceIdProperty);
                    Object targetIdObject = n.getProperty(targetIdProperty);
                    if (null == sourceIdObject || null == targetIdObject)
                        continue;

                    String[] sourceIds = sourceIdObject.getClass().isArray() ? (String[]) sourceIdObject : new String[]{(String) sourceIdObject};
                    String[] targetIds = targetIdObject.getClass().isArray() ? (String[]) targetIdObject : new String[]{(String) targetIdObject};
                    for (String sourceId : sourceIds) {
                        for (String targetId : targetIds)
                            IOUtils.write(sourceId + "\t" + targetId + "\n", os, "UTF-8");
                        numWritten++;
                    }
                }
            }
            log.info("Num written: " + numWritten);
        }
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @javax.ws.rs.Path(HYPERNYMS)
    public Object exportHypernyms(
            @QueryParam(PARAM_FACET_NAMES) String facetNames,
            @QueryParam(PARAM_LABEL) String conceptLabel, @Context Log log)
            throws Exception {
        ObjectMapper om = new ObjectMapper();
        String[] facetNameArray = null != facetNames ? om.readValue(facetNames, String[].class) : null;
        if (null == facetNameArray)
            log.info("Exporting hypernyms dictionary data for all facets.");
        else
            log.info("Exporting hypernyms dictionary data for the facets with names " + Arrays.toString(facetNameArray) + ".");
        return (StreamingOutput) output -> {
            try {
                writeHypernymList(facetNameArray, conceptLabel, output);
            } catch (Exception e) {
                log.error("Exception occurred during concept ID output streaming.", e);
                e.printStackTrace();
            }
        };
    }

    private void writeHypernymList(String[] facetNames,
                                   String conceptLabelString, OutputStream output) throws IOException {

        Label conceptLabel = null;
        if (!StringUtils.isBlank(conceptLabelString))
            conceptLabel = Label.label(conceptLabelString);

        Map<Node, Set<String>> cache = new HashMap<>(Export.HYPERNYMS_CACHE_SIZE);

        GraphDatabaseService graphDb = dbms.database(DEFAULT_DATABASE_NAME);

        try (Transaction tx = graphDb.beginTx()) {
            // This list will hold the relationship types that are used to
            // connect the terms that belong the facets
            // for which hypernyms should be created. If for all facets
            // hypernyms are to be created this will just
            // include the general IS_BROADER_THAN relationship type that
            // doesn't make a difference between facets.
            List<RelationshipType> relationshipTypeList = new ArrayList<>();
            // Only create the specific facet IDs set when we have not just
            // all facets
            if (facetNames != null && facetNames.length > 1 || !facetNames[0].equals("all")) {
                for (String facetName : facetNames) {
                    ResourceIterable<Node> facets = () -> tx.findNodes(FacetManager.FacetLabel.FACET, FacetConstants.PROP_NAME, facetName);
                    for (Node facet : facets) {
                        String facetId = (String) facet.getProperty(FacetConstants.PROP_ID);
                        RelationshipType reltype = RelationshipType
                                .withName(ConceptEdgeTypes.IS_BROADER_THAN + "_" + facetId);
                        relationshipTypeList.add(reltype);
                    }
                }
            } else {
                relationshipTypeList.add(ConceptEdgeTypes.IS_BROADER_THAN);
            }

            for (String facetName : facetNames) {
                log.info("Now creating hypernyms for facet with name " + facetName);
                ResourceIterable<Node> facets = () -> tx.findNodes(FacetManager.FacetLabel.FACET, FacetConstants.PROP_NAME, facetName);
                Set<Node> visitedNodes = new HashSet<>();
                for (Node facet : facets) {
                    Iterable<Relationship> rels = facet.getRelationships(Direction.OUTGOING,
                            ConceptEdgeTypes.HAS_ROOT_CONCEPT);
                    for (Relationship rel : rels) {
                        Node rootTerm = rel.getEndNode();
                        if (null != conceptLabel && !rootTerm.hasLabel(conceptLabel))
                            continue;
                        writeHypernyms(rootTerm, visitedNodes, cache, output,
                                relationshipTypeList.toArray(new RelationshipType[0]));
                    }
                }
            }
        }
    }

    public Set<String> load(Node n, Map<Node, Set<String>> cache, RelationshipType[] relationshipTypes) {
        Set<String> hypernyms = cache.get(n);
        if (null == hypernyms) {
            hypernyms = new HashSet<>();
            cache.put(n, hypernyms);
        } else {
            return hypernyms;
        }

        Set<Node> visitedNodes = new HashSet<>();
        visitedNodes.add(n);
        for (Relationship rel : n.getRelationships(Direction.INCOMING, relationshipTypes)) {
            Node directHypernym = rel.getStartNode();
            boolean isHollow = false;
            for (Label l : directHypernym.getLabels())
                if (l.equals(ConceptLabel.HOLLOW)) {
                    isHollow = true;
                    break;
                }
            if (isHollow)
                continue;
            if (visitedNodes.contains(directHypernym))
                continue;
            String directHypernymId = ((String) directHypernym.getProperty(ConceptConstants.PROP_ID)).intern();
            hypernyms.add(directHypernymId);
            hypernyms.addAll(load(directHypernym, cache, relationshipTypes));
        }
        visitedNodes.remove(n);
        return hypernyms;
    }

    private void writeHypernyms(Node n, Set<Node> visitedNodes, Map<Node, Set<String>> cache, OutputStream os,
                                RelationshipType[] relationshipTypes) throws IOException {
        if (visitedNodes.contains(n))
            return;
        load(n, cache, relationshipTypes);
        visitedNodes.add(n);
        boolean isHollow = false;
        for (Label l : n.getLabels())
            if (l.equals(ConceptLabel.HOLLOW)) {
                isHollow = true;
                break;
            }
        if (isHollow)
            return;
        Set<String> hypernyms = cache.get(n);
        if (hypernyms.size() > 0)
            IOUtils.write(n.getProperty(ConceptConstants.PROP_ID) + "\t" + StringUtils.join(hypernyms, "|") + "\n", os,
                    "UTF-8");
        for (Relationship rel : n.getRelationships(Direction.OUTGOING, ConceptEdgeTypes.IS_BROADER_THAN)) {
            writeHypernyms(rel.getEndNode(), visitedNodes, cache, os, relationshipTypes);
        }
        if (visitedNodes.size() % 100000 == 0)
            log.info("Finished " + visitedNodes.size() + ".");
    }

    /**
     * <p>Produces a dictionary/mapping from concept node names - preferred name, synonyms and, if added to the database, connected acronym node names - to their concept ID ([at]id[0-9]+).</p>
     * <p>The mapping is a text string that consists of one entry per line, name and conceptId are separated by a tab character. While this format can be used for a number of purposes,
     * it specifically fits the format used by the JCoRe Lingpipe Gazetteer component.</p>
     * @param labelsString One or multiple labels that identify the sets of nodes to process for dictionary creation. Lists of labels must be in JSON format. The labels are processed in the specified order. This is important if <tt>uniqueKeys</tt> is enabled.
     * @param exclusionLabelString One or multiple labels that serve as a node filter. Nodes having one of those labels will be skipped from dictionary creation.
     * @param idProperties The node properties that should be the keys of the dictionary. Separate multiple properties with commas. There are restrictions regarding the types of the properties. They must either all be non-array values or all are arrays of the same length. For multiple properties, a single mapping-target string is created with "||" as a value separator. In case of array values, the string first lists all first elements, then the second elements, then the third elements etc.
     * @param uniqueKeys Determines if keys may occur multiple times or should be unique. In case of uniqueness, the <tt>labelsString</tt> becomes important: the first occurrence of a key will be included in the output, subsequent occurrences will be discarded.
     * @param log
     * @return The dictionary text string GZIP-compressed and Base64-ASCII-encoded.
     * @throws IOException
     */
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @javax.ws.rs.Path(LINGPIPE_DICT)
    public String exportLingpipeDictionary(
            @QueryParam(PARAM_LABELS) String labelsString,
            @QueryParam(PARAM_EXCLUSION_LABEL) String exclusionLabelString,
            @QueryParam(PARAM_SOURCE_ID_PROPERTY) String idProperties,
            @QueryParam(PARAM_ADD_SOURCE_PREFIX) boolean addSourcePrefix,
            @QueryParam(PARAM_UNIQUE_KEYS) boolean uniqueKeys,
            @Context Log log)
            throws IOException {
        final ObjectMapper om = new ObjectMapper();
        Label[] labels;
        // We accept single labels or lists of labels
        if (!labelsString.contains("["))
            labels = StringUtils.isBlank(labelsString) ? new Label[]{ConceptLabel.CONCEPT} : new Label[]{Label.label(labelsString)};
        else
            labels = Arrays.stream(om.readValue(labelsString, String[].class)).map(Label::label).toArray(Label[]::new);
        List<String> propertiesToWrite = new ArrayList<>();
        if (idProperties == null || idProperties.length() == 0) {
            propertiesToWrite.add(PROP_ID);
        } else if (!idProperties.contains("[")){
            Collections.addAll(propertiesToWrite, idProperties.split(","));
        } else
            propertiesToWrite = Arrays.stream(om.readValue(idProperties, String[].class)).collect(Collectors.toList());

        Map<String, String> sourcePropertyNamesByIdPropertyName = Map.of(PROP_ID, "",
                PROP_ORG_ID, PROP_ORG_SRC,
                PROP_SRC_IDS + 0,   PROP_SOURCES + 0,
                PROP_SRC_IDS + 1,   PROP_SOURCES + 1,
                PROP_SRC_IDS + 2,   PROP_SOURCES + 2,
                PROP_SRC_IDS + 3,   PROP_SOURCES + 3,
                PROP_SRC_IDS + 4,   PROP_SOURCES + 4,
                PROP_SRC_IDS + 5,   PROP_SOURCES + 5,
                PROP_SRC_IDS + 6,   PROP_SOURCES + 6,
                PROP_SRC_IDS + 7, PROP_SOURCES + 7);
        log.info("Exporting lingpipe dictionary data for nodes with labels \"" + Arrays.stream(labels).map(Label::name).collect(Collectors.joining(", "))
                + "\", mapping their names to their properties " + propertiesToWrite + ".");
        Label[] exclusionLabels = null;
        if (!StringUtils.isBlank(exclusionLabelString)) {
            try {
                String[] exclusionLabelsJson = om.readValue(exclusionLabelString, String[].class);
                exclusionLabels = new Label[exclusionLabelsJson.length];
                for (int i = 0; i < exclusionLabelsJson.length; i++) {
                    String string = exclusionLabelsJson[i];
                    exclusionLabels[i] = Label.label(string);
                }
            } catch (JsonParseException e) {
                Label exclusionLabel = Label.label(exclusionLabelString);
                exclusionLabels = new Label[]{exclusionLabel};
            }
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream(OUTPUTSTREAM_INIT_SIZE);
        GraphDatabaseService graphDb = dbms.database(DEFAULT_DATABASE_NAME);
        Set<String> writtenKeys = uniqueKeys ? new HashSet<>() : null;
        try (GZIPOutputStream os = new GZIPOutputStream(baos)) {
            for (Label label : labels) {
                try (Transaction tx = graphDb.beginTx()) {
                    ResourceIterator<Node> conceptNodes = tx.findNodes(label);
                    int count = 0;
                    while (conceptNodes.hasNext()) {
                        Node node = conceptNodes.next();
                        count++;
                        boolean termHasExclusionLabel = false;
                        for (int i = 0; null != exclusionLabels && i < exclusionLabels.length; i++) {
                            Label exclusionLabel = exclusionLabels[i];
                            if (node.hasLabel(exclusionLabel)) {
                                termHasExclusionLabel = true;
                                break;
                            }
                        }
                        if (!termHasExclusionLabel && node.hasProperty(PROP_ID) && node.hasProperty(PROP_PREF_NAME)) {

                            int arraySize;
                            String idProperty = propertiesToWrite.get(0);
                            // for array-valued properties we require that all
                            // arrays are of the same length. Thus, to determine the
                            // required number of iterations we just use the first
                            // array since the others should have the same length.
                            String[] ids = NodeUtilities.getNodePropertyAsStringArrayValue(node, idProperty);

                            if (null == ids && node.hasLabel(ConceptLabel.AGGREGATE))
                                // perhaps we have an aggregate term, then we can
                                // try and retrieve the value from its elements
                                ids = ConceptAggregateManager.getPropertyValueOfElements(node, idProperty);
                            if (null == ids) {
                                conceptNodes.close();
                                throw new IllegalArgumentException("A concept occurred that does not have a value for the property \"" + idProperty + "\": " + NodeUtilities.getNodePropertiesAsString(node));
                            }
                            arraySize = ids.length;

                            List<String> categoryStrings = new ArrayList<>();
                            for (int i = 0; i < arraySize; ++i) {
                                StringBuilder sb = new StringBuilder();
                                for (int j = 0; j < propertiesToWrite.size(); ++j) {
                                    String property = propertiesToWrite.get(j);
                                    ids = NodeUtilities.getNodePropertyAsStringArrayValue(node, property);
                                    String[] sources = null;
                                    if (addSourcePrefix) {
                                        final String sourceProperty = sourcePropertyNamesByIdPropertyName.get(property);
                                        if (sourceProperty == null)
                                            throw new IllegalArgumentException("Dictionary creation with source prefix should be performed but the source property is unknown for ID property '" + property + "'.");
                                        // for PROP_ID we assigned an empty string
                                        if (!sourceProperty.isEmpty()) {
                                            sources = NodeUtilities.getNodePropertyAsStringArrayValue(node, sourceProperty);
                                        } else {
                                            sources = new String[ids.length];
                                            for (int k = 0; k < sources.length; k++) {
                                                sources[k] = "id";
                                            }
                                        }

                                    }
                                    if (null == ids && node.hasLabel(ConceptLabel.AGGREGATE)) {
                                        // perhaps we have an aggregate term, then
                                        // we can try and retrieve the value from
                                        // its elements
                                        ids = ConceptAggregateManager.getPropertyValueOfElements(node, idProperty);
                                        if (addSourcePrefix) {
                                            final String sourceProperty = sourcePropertyNamesByIdPropertyName.get(property);
                                            if (sourceProperty == null)
                                                throw new IllegalArgumentException("Dictionary creation with source prefix should be performed but the source property is unknown for ID property '" + property + "'.");
                                            // for PROP_ID we assigned an empty string
                                            if (!sourceProperty.isEmpty()) {
                                                sources = ConceptAggregateManager.getPropertyValueOfElements(node, sourceProperty);
                                            } else {
                                                sources = new String[ids.length];
                                                for (int k = 0; k < sources.length; k++) {
                                                    sources[k] = "id";
                                                }
                                            }
                                        }
                                    }

                                    if (null == ids || ids.length == 0) {
                                        conceptNodes.close();
                                        throw new IllegalArgumentException("The property \"" + property
                                                + "\" does not contain a value for node " + node + " (properties: "
                                                + PropertyUtilities.getNodePropertiesAsString(node) + ")");
                                    }
                                    if (ids.length != arraySize) {
                                        conceptNodes.close();
                                        throw new IllegalArgumentException("The properties \"" + propertiesToWrite
                                                + "\" on term " + PropertyUtilities.getNodePropertiesAsString(node)
                                                + " do not have all the same number of value elements which is required for dictionary creation by this method.");
                                    }
                                    if (addSourcePrefix)
                                        sb.append(sources[i]).append(":").append(ids[i]);
                                        else
                                    sb.append(ids[i]);
                                    if (j < propertiesToWrite.size() - 1)
                                        sb.append("||");
                                }
                                categoryStrings.add(sb.toString());
                            }

                            for (String categoryString : categoryStrings) {
                                String preferredName = (String) node.getProperty(PROP_PREF_NAME);
                                String[] synonyms = new String[0];
                                if (node.hasProperty(PROP_SYNONYMS))
                                    synonyms = (String[]) node.getProperty(PROP_SYNONYMS);
                                // String[] writingVariants = new String[0];
                                // if (term.hasProperty(PROP_WRITING_VARIANTS))
                                // writingVariants = (String[]) term
                                // .getProperty(PROP_WRITING_VARIANTS);

                                writeNormalizedDictionaryEntry(preferredName, categoryString, writtenKeys, os);
                                for (String synonString : synonyms)
                                    writeNormalizedDictionaryEntry(synonString, categoryString, writtenKeys, os);
                                TraversalDescription acronymsTraversal = PredefinedTraversals.getAcronymsTraversal(tx);
                                Traverser traverse = acronymsTraversal.traverse(node);
                                for (Node acronymNode : traverse.nodes()) {
                                    String acronym = (String) acronymNode.getProperty(MorphoConstants.PROP_NAME);
                                    writeNormalizedDictionaryEntry(acronym, categoryString, writtenKeys, os);
                                }
                                // for (String variant : writingVariants)
                                // writeNormalizedDictionaryEntry(variant,
                                // categoryString, os);
                            }
                        }
                        if (count % 100000 == 0)
                            log.info(count + " terms processed.");
                    }
                }
            }
        }
        log.info("Done exporting Lingpipe term dictionary.");
        byte[] bytes = baos.toByteArray();
        return Base64.getEncoder().encodeToString(bytes);
    }

    private void writeNormalizedDictionaryEntry(String name, String termId, Set<String> writtenKeys, OutputStream os) throws IOException {
        String normalizedName = StringUtils.normalizeSpace(name);
        if (normalizedName.length() > 2 && (writtenKeys == null || writtenKeys.add(normalizedName))) {
            IOUtils.write(normalizedName + "\t" + termId + "\n", os, "UTF-8");
        }
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @javax.ws.rs.Path(CONCEPT_TO_FACET)
    public Representation exportTermFacetMapping(
            @QueryParam(PARAM_LABEL) String labelString)
            throws IOException {
        log.info("Exporting lingpipe dictionary data.");
        Label label = !StringUtils.isBlank(labelString) ? Label.label(labelString) : ConceptLabel.CONCEPT;
        ByteArrayOutputStream baos = new ByteArrayOutputStream(OUTPUTSTREAM_INIT_SIZE);
        GraphDatabaseService graphDb = dbms.database(DEFAULT_DATABASE_NAME);
        try (GZIPOutputStream os = new GZIPOutputStream(baos)) {
            try (Transaction tx = graphDb.beginTx()) {
                ResourceIterable<Node> terms = () -> tx.findNodes(label);
                int count = 0;
                for (Node term : terms) {
                    count++;
                    if (term.hasProperty(PROP_ID) && term.hasProperty(PROP_FACETS)) {
                        String termId = (String) term.getProperty(PROP_ID);
                        String[] facetIds = (String[]) term.getProperty(PROP_FACETS);
                        IOUtils.write(termId + "\t" + StringUtils.join(facetIds, "|") + "\n", os, "UTF-8");
                    }
                    if (count % 100000 == 0)
                        log.info(count + " terms processed.");
                }
                log.info("Done exporting mapping from term ID to corresponding facet IDs.");
            }
        }
        return RecursiveMappingRepresentation.getObjectRepresentation(baos.toByteArray());
    }
}
