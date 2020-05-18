package de.julielab.neo4j.plugins;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.julielab.neo4j.plugins.auxiliaries.JulieNeo4jUtilities;
import de.julielab.neo4j.plugins.auxiliaries.PropertyUtilities;
import de.julielab.neo4j.plugins.auxiliaries.semedico.NodeUtilities;
import de.julielab.neo4j.plugins.auxiliaries.semedico.PredefinedTraversals;
import de.julielab.neo4j.plugins.concepts.ConceptAggregateManager;
import de.julielab.neo4j.plugins.concepts.ConceptEdgeTypes;
import de.julielab.neo4j.plugins.concepts.ConceptLabel;
import de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.MorphoConstants;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.server.rest.repr.RecursiveMappingRepresentation;
import org.neo4j.server.rest.repr.Representation;

import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import static de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants.*;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@javax.ws.rs.Path("/export")
public class Export {

    public static final String HYPERNYMS = "hypernyms";
    public static final String LINGPIPE_DICT = "lingpipe_dictionary";
    public static final String CONCEPT_TO_FACET = "concept_facet_map";
    public static final String CONCEPT_ID_MAPPING = "concept_id_mapping";
    public static final String PARAM_ID_PROPERTY = "id_property";
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
    @Produces(MediaType.APPLICATION_JSON)
    @javax.ws.rs.Path("/" + CONCEPT_ID_MAPPING)
    public Representation exportIdMapping(@QueryParam(PARAM_ID_PROPERTY) String idProperty, @QueryParam(PARAM_LABELS) String labelStrings) throws Exception {
        final ObjectMapper om = new ObjectMapper();
        log.info("Exporting ID mapping data.");
        String[] labelsArray = null != labelStrings ? om.readValue(labelStrings, String[].class) : null;
        log.info("Creating mapping file content for property \"" + idProperty + "\" and facets " + Arrays.toString(labelsArray));
        ByteArrayOutputStream gzipBytes = createIdMapping(idProperty, labelsArray);
        byte[] bytes = gzipBytes.toByteArray();
        log.info("Sending all " + bytes.length + " bytes of GZIPed ID mapping file data.");
        log.info("Done exporting ID mapping data.");
        return RecursiveMappingRepresentation.getObjectRepresentation(bytes);
    }

    private ByteArrayOutputStream createIdMapping(String idProperty,
                                                  String[] labelsArray) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(OUTPUTSTREAM_INIT_SIZE);
        GraphDatabaseService graphDb = dbms.database(DEFAULT_DATABASE_NAME);
        try (GZIPOutputStream os = new GZIPOutputStream(baos)) {
            try (Transaction tx = graphDb.beginTx()) {
                int numWritten = 0;
                for (String labelString : labelsArray) {
                    Label label = Label.label(labelString);
                    for (ResourceIterator<Node> terms = tx.findNodes(label); terms.hasNext(); ) {
                        Node term = terms.next();
                        String termId = (String) term.getProperty(ConceptConstants.PROP_ID);
                        Object idObject = idProperty.equals(PROP_SRC_IDS) ? NodeUtilities.getSourceIds(term) : PropertyUtilities.getNonNullNodeProperty(term, idProperty);
                        if (null == idObject)
                            continue;

                        if (idObject.getClass().isArray()) {
                            Object[] idArray = JulieNeo4jUtilities.convertArray(idObject);
                            for (Object id : idArray) {
                                IOUtils.write(id + "\t" + termId + "\n", os, "UTF-8");
                                numWritten++;
                            }
                        } else {
                            IOUtils.write(idObject + "\t" + termId + "\n", os, "UTF-8");
                            numWritten++;
                        }
                        // }
                    }
                }
                log.info("Num written: " + numWritten);
            }
        }
        return baos;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @javax.ws.rs.Path("/"+HYPERNYMS)
    public Representation exportHypernyms(
            @QueryParam(PARAM_LABELS) String facetLabelStrings,
            @QueryParam(PARAM_LABEL) String conceptLabel)
            throws Exception {
        ObjectMapper om = new ObjectMapper();
        String[] labelsArray = null != facetLabelStrings ? om.readValue(facetLabelStrings, String[].class) : null;
        if (null == labelsArray)
            log.info("Exporting hypernyms dictionary data for all facets.");
        else
            log.info("Exporting hypernyms dictionary data for the facets with labels " + Arrays.toString(labelsArray) + ".");
        ByteArrayOutputStream hypernymsGzipBytes = writeHypernymList(labelsArray, conceptLabel
        );
        byte[] bytes = hypernymsGzipBytes.toByteArray();
        log.info("Sending all " + bytes.length + " bytes of GZIPed hypernym file data.");
        log.info("Done exporting hypernym data.");
        return RecursiveMappingRepresentation.getObjectRepresentation(bytes);
    }

    private ByteArrayOutputStream writeHypernymList(String[] labelsArray,
                                                    String termLabelString) throws IOException {

        String[] labels = labelsArray;
        if (null == labels) {
            labels = new String[]{FacetManager.FacetLabel.FACET.name()};
        }
        Label termLabel = null;
        if (!StringUtils.isBlank(termLabelString))
            termLabel = Label.label(termLabelString);

        Map<Node, Set<String>> cache = new HashMap<>(Export.HYPERNYMS_CACHE_SIZE);

        ByteArrayOutputStream baos = new ByteArrayOutputStream(OUTPUTSTREAM_INIT_SIZE);
        GraphDatabaseService graphDb = dbms.database(DEFAULT_DATABASE_NAME);
        try (GZIPOutputStream os = new GZIPOutputStream(baos)) {

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
                if (labels.length > 1 || !labels[0].equals(FacetManager.FacetLabel.FACET.name())) {
                    for (String labelString : labels) {
                        Label label = Label.label(labelString);
                        ResourceIterable<Node> facets = () -> tx.findNodes(label);
                        for (Node facet : facets) {
                            if (!facet.hasLabel(FacetManager.FacetLabel.FACET))
                                throw new IllegalArgumentException("Label node " + facet + " with the label " + label
                                        + " is no facet since it does not have the " + FacetManager.FacetLabel.FACET
                                        + " label.");
                            String facetId = (String) facet.getProperty(FacetConstants.PROP_ID);
                            RelationshipType reltype = RelationshipType
                                    .withName(ConceptEdgeTypes.IS_BROADER_THAN + "_" + facetId);
                            relationshipTypeList.add(reltype);
                        }
                    }
                } else {
                    relationshipTypeList.add(ConceptEdgeTypes.IS_BROADER_THAN);
                }

                for (String labelString : labels) {
                    Label label = Label.label(labelString);
                    log.info("Now creating hypernyms for facets with label " + label);
                    ResourceIterable<Node> facets = () -> tx.findNodes(label);
                    Set<Node> visitedNodes = new HashSet<>();
                    for (Node facet : facets) {
                        Iterable<Relationship> rels = facet.getRelationships(Direction.OUTGOING,
                                ConceptEdgeTypes.HAS_ROOT_CONCEPT);
                        for (Relationship rel : rels) {
                            Node rootTerm = rel.getEndNode();
                            if (null != termLabel && !rootTerm.hasLabel(termLabel))
                                continue;
                            writeHypernyms(rootTerm, visitedNodes, cache, os,
                                    relationshipTypeList.toArray(new RelationshipType[0]));
                        }
                    }
                }
            }
        }
        return baos;
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

    private void writeHypernyms(Node n, Set<Node> visitedNodes, Map<Node, Set<String>> cache, GZIPOutputStream os,
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

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @javax.ws.rs.Path("/"+LINGPIPE_DICT)
    public String exportLingpipeDictionary(
            @QueryParam(PARAM_LABEL) String labelString,
            @QueryParam(PARAM_EXCLUSION_LABEL) String exclusionLabelString,
            @QueryParam(PARAM_ID_PROPERTY) String nodeCategories)
            throws IOException {
        Label label = StringUtils.isBlank(labelString) ? ConceptLabel.CONCEPT : Label.label(labelString);
        List<String> propertiesToWrite = new ArrayList<>();
        if (nodeCategories == null || nodeCategories.length() == 0) {
            propertiesToWrite.add(PROP_ID);
        } else {
            Collections.addAll(propertiesToWrite, nodeCategories.split(","));
        }
        Label[] exclusionLabels = null;
        if (!StringUtils.isBlank(exclusionLabelString)) {
            final ObjectMapper om = new ObjectMapper();
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
        log.info("Exporting lingpipe dictionary data for nodes with label \"" + label.name()
                + "\", mapping their names to their properties " + propertiesToWrite + ".");
        ByteArrayOutputStream baos = new ByteArrayOutputStream(OUTPUTSTREAM_INIT_SIZE);
        GraphDatabaseService graphDb = dbms.database(DEFAULT_DATABASE_NAME);
        try (GZIPOutputStream os = new GZIPOutputStream(baos)) {
            try (Transaction tx = graphDb.beginTx()) {
                ResourceIterator<Node> terms = tx.findNodes(label);
                int count = 0;
                while (terms.hasNext()) {
                    Node term = terms.next();
                    count++;
                    boolean termHasExclusionLabel = false;
                    for (int i = 0; null != exclusionLabels && i < exclusionLabels.length; i++) {
                        Label exclusionLabel = exclusionLabels[i];
                        if (term.hasLabel(exclusionLabel)) {
                            termHasExclusionLabel = true;
                            break;
                        }
                    }
                    if (!termHasExclusionLabel && term.hasProperty(PROP_ID) && term.hasProperty(PROP_PREF_NAME)) {

                        int arraySize;
                        String idProperty = propertiesToWrite.get(0);
                        // for array-valued properties we require that all
                        // arrays are of the same length. Thus, to determine the
                        // required number of iterations we just use the first
                        // array since the others should have the same length.
                        String[] value = NodeUtilities.getNodePropertyAsStringArrayValue(term, idProperty);

                        if (null == value && term.hasLabel(ConceptLabel.AGGREGATE))
                            // perhaps we have an aggregate term, then we can
                            // try and retrieve the value from its elements
                            value = ConceptAggregateManager.getPropertyValueOfElements(term, idProperty);
                        if (null == value) {
                            terms.close();
                            throw new IllegalArgumentException("A concept occurred that does not have a value for the property \"" + idProperty + "\": " + NodeUtilities.getNodePropertiesAsString(term));
                        }
                        arraySize = value.length;

                        List<String> categoryStrings = new ArrayList<>();
                        for (int i = 0; i < arraySize; ++i) {
                            StringBuilder sb = new StringBuilder();
                            for (int j = 0; j < propertiesToWrite.size(); ++j) {
                                String property = propertiesToWrite.get(j);
                                value = NodeUtilities.getNodePropertyAsStringArrayValue(term, property);
                                if (null == value && term.hasLabel(ConceptLabel.AGGREGATE))
                                    // perhaps we have an aggregate term, then
                                    // we can try and retrieve the value from
                                    // its elements
                                    value = ConceptAggregateManager.getPropertyValueOfElements(term, idProperty);
                                if (null == value || value.length == 0) {
                                    terms.close();
                                    throw new IllegalArgumentException("The property \"" + property
                                            + "\" does not contain a value for node " + term + " (properties: "
                                            + PropertyUtilities.getNodePropertiesAsString(term) + ")");
                                }
                                if (value.length != arraySize) {
                                    terms.close();
                                    throw new IllegalArgumentException("The properties \"" + propertiesToWrite
                                            + "\" on term " + PropertyUtilities.getNodePropertiesAsString(term)
                                            + " do not have all the same number of value elements which is required for dictionary creation by this method.");
                                }
                                sb.append(value[i]);
                                if (j < propertiesToWrite.size() - 1)
                                    sb.append("||");
                            }
                            categoryStrings.add(sb.toString());
                        }

                        for (String categoryString : categoryStrings) {
                            String preferredName = (String) term.getProperty(PROP_PREF_NAME);
                            String[] synonyms = new String[0];
                            if (term.hasProperty(PROP_SYNONYMS))
                                synonyms = (String[]) term.getProperty(PROP_SYNONYMS);
                            // String[] writingVariants = new String[0];
                            // if (term.hasProperty(PROP_WRITING_VARIANTS))
                            // writingVariants = (String[]) term
                            // .getProperty(PROP_WRITING_VARIANTS);

                            writeNormalizedDictionaryEntry(preferredName, categoryString, os);
                            for (String synonString : synonyms)
                                writeNormalizedDictionaryEntry(synonString, categoryString, os);
                            TraversalDescription acronymsTraversal = PredefinedTraversals.getAcronymsTraversal(tx);
                            Traverser traverse = acronymsTraversal.traverse(term);
                            for (Node acronymNode : traverse.nodes()) {
                                String acronym = (String) acronymNode.getProperty(MorphoConstants.PROP_NAME);
                                writeNormalizedDictionaryEntry(acronym, categoryString, os);
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
        log.info("Done exporting Lingpipe term dictionary.");
        byte[] bytes = baos.toByteArray();
        return Base64.getEncoder().encodeToString(bytes);
    }

    private void writeNormalizedDictionaryEntry(String name, String termId, OutputStream os) throws IOException {
        String normalizedName = StringUtils.normalizeSpace(name);
        if (normalizedName.length() > 2)
            IOUtils.write(normalizedName + "\t" + termId + "\n", os, "UTF-8");
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @javax.ws.rs.Path("/" + CONCEPT_TO_FACET)
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
