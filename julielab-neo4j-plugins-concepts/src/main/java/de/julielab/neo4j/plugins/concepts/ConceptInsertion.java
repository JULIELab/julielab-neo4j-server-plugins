package de.julielab.neo4j.plugins.concepts;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.Sets;
import de.julielab.neo4j.plugins.FacetManager;
import de.julielab.neo4j.plugins.FullTextIndexUtils;
import de.julielab.neo4j.plugins.auxiliaries.semedico.CoordinatesMap;
import de.julielab.neo4j.plugins.auxiliaries.semedico.CoordinatesSet;
import de.julielab.neo4j.plugins.auxiliaries.semedico.NodeUtilities;
import de.julielab.neo4j.plugins.auxiliaries.semedico.SequenceManager;
import de.julielab.neo4j.plugins.constants.semedico.SequenceConstants;
import de.julielab.neo4j.plugins.datarepresentation.*;
import de.julielab.neo4j.plugins.datarepresentation.constants.ConceptRelationConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.NodeIDPrefixConstants;
import de.julielab.neo4j.plugins.datarepresentation.util.ConceptsJsonSerializer;
import de.julielab.neo4j.plugins.util.AggregateConceptInsertionException;
import de.julielab.neo4j.plugins.util.ConceptInsertionException;
import org.apache.commons.lang.StringUtils;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.logging.slf4j.Slf4jLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.StreamSupport;

import static de.julielab.neo4j.plugins.auxiliaries.PropertyUtilities.*;
import static de.julielab.neo4j.plugins.concepts.ConceptEdgeTypes.HAS_ROOT_CONCEPT;
import static de.julielab.neo4j.plugins.concepts.ConceptLabel.AGGREGATE;
import static de.julielab.neo4j.plugins.concepts.ConceptLabel.*;
import static de.julielab.neo4j.plugins.concepts.ConceptLookup.lookupConcept;
import static de.julielab.neo4j.plugins.concepts.ConceptManager.*;
import static de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;

public class ConceptInsertion {
    private final static Logger log = LoggerFactory.getLogger(ConceptInsertion.class);

    static void createRelationships(Log log, Transaction tx, List<ImportConcept> jsonConcepts, String facetId,
                                    CoordinatesMap nodesByCoordinates, ImportOptions importOptions, InsertionReport insertionReport) {
        log.debug("Creating relationship between inserted concepts.");
        Node facet = FacetManager.getFacetNode(tx, facetId);
        RelationshipType relBroaderThanInFacet = null;
        if (null != facet)
            relBroaderThanInFacet = RelationshipType.withName(ConceptEdgeTypes.IS_BROADER_THAN.toString() + "_" + facetId);
        AddToNonFacetGroupCommand noFacetCmd = importOptions.noFacetCmd;
        Node noFacet = null;
        for (ImportConcept jsonConcept : jsonConcepts) {
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
                            createRelationshipIfNotExists(facet, concept, HAS_ROOT_CONCEPT, insertionReport);
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

                        if (insertionReport.importedCoordinates.contains(parentCoordinates)
                                || insertionReport.existingConcepts.contains(parent)) {
                            createRelationshipIfNotExists(parent, concept, ConceptEdgeTypes.IS_BROADER_THAN, insertionReport);
                            // Since a concept may appear in multiple facets, we
                            // connect concepts with a general taxonomic
                            // relation as well as a special relation only
                            // relevant to the
                            // particular structure‚ of the current facet.
                            createRelationshipIfNotExists(parent, concept, relBroaderThanInFacet, insertionReport);
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
                                                + HOLLOW + "\" and be connected to the facet root.");
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
                                parent.addLabel(CONCEPT);
                                // nodesByCoordinates.put(parentCoordinates,
                                // hollowParent);
                                // insertionReport.numConcepts++;
                                createRelationshipIfNotExists(parent, concept, ConceptEdgeTypes.IS_BROADER_THAN,
                                        insertionReport);
                                createRelationshipIfNotExists(parent, concept, relBroaderThanInFacet, insertionReport);
                                createRelationshipIfNotExists(facet, parent, HAS_ROOT_CONCEPT,
                                        insertionReport);
                            } else {
                                assert facet != null;
                                log.warn(
                                        "Creating hollow parents is switched off. Hence the concept will be added as root concept for its facet (\""
                                                + facet.getProperty(FacetConstants.PROP_NAME) + "\").");
                                // Connect the concept as a root, it's the best we
                                // can
                                // do.
                                createRelationshipIfNotExists(facet, concept, HAS_ROOT_CONCEPT,
                                        insertionReport);
                            }
                        }
                        if (parent.hasLabel(AGGREGATE) && !parent.hasLabel(CONCEPT))
                            throw new IllegalArgumentException("Concept with source ID " + srcId
                                    + " specifies source ID " + parentSrcId
                                    + " as parent. This node is an aggregate but not a CONCEPT itself and thus is not included in the hierarchy and cannot be the conceptual parent of other concepts. To achieve this, import the aggregate with the property "
                                    + AGGREGATE_INCLUDE_IN_HIERARCHY
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
                            assert facet != null;
                            noFacet = FacetManager.getNoFacet(tx, (String) facet.getProperty(PROP_ID));
                        }

                        createRelationshipIfNotExists(noFacet, concept, HAS_ROOT_CONCEPT, insertionReport);
                    } else if (null != facet) {
                        // This concept does not have a concept parent. It is a facet
                        // root,
                        // thus connect it to the facet node.
                        createRelationshipIfNotExists(facet, concept, HAS_ROOT_CONCEPT, insertionReport);
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
                    for (ImportConceptRelationship jsonRelationship : jsonConcept.relationships) {
                        String rsTypeStr = jsonRelationship.type;
                        final ConceptCoordinates targetCoordinates = jsonRelationship.targetCoordinates;
                        Node target = lookupConcept(tx, targetCoordinates);
                        if (null == target) {
                            log.debug("Creating hollow relationship target with orig Id/orig source " + targetCoordinates);
                            target = registerNewHollowConceptNode(tx, targetCoordinates);
                        }
                        ConceptEdgeTypes type = ConceptEdgeTypes.valueOf(rsTypeStr);
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
                        insertionReport.numRelationships++;
                    }
                }
            }
        }
        log.debug("Finished 100% of concepts for relationship creation.");
    }

    /**
     * Creates a node with the {@link ConceptLabel#HOLLOW} label, sets the given
     * coordinates and adds the node to the index.
     *
     * @param tx          The current transaction.
     * @param coordinates The concept coordinates to register the hollow node for.
     * @return The newly created hollow node.
     */
    static Node registerNewHollowConceptNode(Transaction tx, ConceptCoordinates coordinates,
                                             Label... additionalLabels) {
        Node node = tx.createNode(HOLLOW);
        node.addLabel(CONCEPT);
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

    static void insertConcept(Transaction tx, String facetId,
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
        if (concept.hasLabel(HOLLOW)) {
            log.trace("Got HOLLOW concept node with coordinates " + coordinates + " and will create full concept.");
            concept.removeLabel(HOLLOW);
            concept.addLabel(CONCEPT);
            Iterable<Relationship> relationships = concept.getRelationships(HAS_ROOT_CONCEPT);
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
        setNonNullNodeProperty(concept, PROP_PREF_NAME, jsonConcept.prefName);
        mergeArrayProperty(concept, PROP_DESCRIPTIONS, () -> jsonConcept.descriptions.toArray(new String[0]));
        mergeArrayProperty(concept, PROP_WRITING_VARIANTS, () -> jsonConcept.writingVariants.toArray(new String[0]));
        mergeArrayProperty(concept, PROP_COPY_PROPERTIES, () -> jsonConcept.copyProperties.toArray(new String[0]));
        mergeArrayProperty(concept, PROP_SYNONYMS, synonyms.stream().filter(s -> !s.equals(prefName)).toArray());
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

        NodeUtilities.mergeSourceId(tx, concept, srcId, source, uniqueSourceId);

        for (int i = 0; null != generalLabels && i < generalLabels.size(); i++) {
            concept.addLabel(Label.label(generalLabels.get(i)));
        }

        if (StringUtils.isBlank(prefName) && !insertionReport.existingConcepts.contains(concept))
            throw new IllegalArgumentException("Concept has no property \"" + PROP_PREF_NAME + "\": " + jsonConcept);
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
     * @param source          The node to create a new relationship from (note that the relationship direction has yet to be considered).
     * @param target          The node to create the new relationship to (note that the relationship direction has yet to be considered).
     * @param type            The relationship type of new new relationship.
     * @param insertionReport The insertion report keeping track of the number of inserted elements.
     * @param direction       The relationship direction.
     * @param properties      A sequence of property key and property values. These properties
     *                        will be used to determine whether a relationship - with those
     *                        properties - already exists.
     * @return The newly created relationship. Null if the relationship did already exist.
     */
    private static Relationship createRelationShipIfNotExists(Node source, Node target, RelationshipType type,
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
                    relationShipExists = mergeProperties(relationship, properties);
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
     * Creates a relationship of type <tt>type</tt> from <tt>source</tt> to
     * <tt>target</tt>, if this relationship does not already exist.
     *
     * @param source          The node to create a new relationship from (note that the relationship direction has yet to be considered).
     * @param target          The node to create the new relationship to (note that the relationship direction has yet to be considered).
     * @param type            The relationship type of new new relationship.
     * @param insertionReport The insertion report keeping track of the number of inserted elements.
     */
    private static void createRelationshipIfNotExists(Node source, Node target, RelationshipType type,
                                                      InsertionReport insertionReport) {
        createRelationShipIfNotExists(source, target, type, insertionReport, Direction.OUTGOING);
    }

    public static void insertConcepts(GraphDatabaseService graphDb, ImportConcepts importConcepts, Map<String, Object> response) throws ConceptInsertionException {
        insertConcepts(new Slf4jLog(log), graphDb, new ByteArrayInputStream(ConceptsJsonSerializer.toJson(importConcepts).getBytes(UTF_8)), response);
    }

    public static InsertionReport insertConcepts(Log log, GraphDatabaseService graphDb, InputStream importConceptsStream, Map<String, Object> response) throws ConceptInsertionException {
        long time = System.currentTimeMillis();
        ObjectMapper mapper = new ObjectMapper().registerModule(new Jdk8Module());
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        JsonParser parser;
        InsertionReport insertionReport = new InsertionReport();
        long numConcepts = -1;
        log.debug("Parsing JSON stream. This is right before the synchronization block of the concept insertion process.");
        synchronized (ConceptManager.class) {
            try {
                parser = new JsonFactory(mapper).createParser(importConceptsStream);

                ImportFacet importFacet = null;
                ImportOptions importOptions = new ImportOptions();
                Iterator<ImportConcept> importConcepts = null;
                String lastName = null;
                // We need to stop as soon as we we found the concepts which must be the last property of ImportConcepts
                // object. Then, we use the iterator to continue.
                while (parser.nextToken() != null && importConcepts == null) {
                    JsonToken currentToken = parser.currentToken();
                    if (currentToken == JsonToken.FIELD_NAME) {
                        lastName = parser.getCurrentName();
                    } else if (currentToken == JsonToken.START_OBJECT) {
                        if (lastName != null && lastName.equals(ImportConcepts.NAME_FACET))
                            importFacet = parser.readValueAs(ImportFacet.class);
                        else if (lastName != null && lastName.equals(ImportConcepts.NAME_IMPORT_OPTIONS))
                            importOptions = parser.readValueAs(ImportOptions.class);
                        else if (lastName != null && lastName.equals(ImportConcepts.NAME_NUM_CONCEPTS))
                            numConcepts = parser.readValueAs(Long.class);
                    } else if (lastName != null && lastName.equals(ImportConcepts.NAME_CONCEPTS) && currentToken == JsonToken.START_ARRAY) {
                        importConcepts = parser.readValuesAs(ImportConcept.class);
                    }
                }


                log.info("Got %s concepts to import.", numConcepts);
                String facetId = null;
                try (Transaction tx = graphDb.beginTx()) {
                    Node facet = null;
                    // The facet Id will be added to the facets-property of the concept
                    // nodes.
                    log.debug("Handling import of facet.");
                    if (null != importFacet && importFacet.getId() != null) {
                        facetId = importFacet.getId();
                        log.info("Facet ID {} has been given to add the concepts to.", facetId);
                        boolean isNoFacet = importFacet.isNoFacet();
                        if (isNoFacet)
                            facet = FacetManager.getNoFacet(tx, facetId);
                        else
                            facet = FacetManager.getFacetNode(tx, facetId);
                        if (null == facet)
                            throw new IllegalArgumentException("The facet with ID \"" + facetId
                                    + "\" was not found. You must pass the ID of an existing facet or deliver all information required to create the facet from scratch. Then, the facetId must not be included in the request, it will be created dynamically.");
                    } else if (null != importFacet && importFacet.getName() != null) {
                        ResourceIterator<Node> facetIterator = tx.findNodes(FacetManager.FacetLabel.FACET);
                        while (facetIterator.hasNext()) {
                            facet = facetIterator.next();
                            if (facet.getProperty(FacetConstants.PROP_NAME)
                                    .equals(importFacet.getName()))
                                break;
                            facet = null;
                        }

                    }
                    if (null != importFacet && null == facet) {
                        // No existing ID is given, create a new facet.
                        facet = FacetManager.createFacet(tx, importFacet);
                    }
                    if (null != facet) {
                        facetId = (String) facet.getProperty(PROP_ID);
                        log.debug("Facet {} was successfully created or determined by ID.", facetId);
                    } else {
                        log.debug(
                                "No facet was specified for this import. This is currently equivalent to specifying the merge import option, i.e. concept properties will be merged but no new nodes or relationships will be created.");
                        importOptions.merge = true;
                    }
                    tx.commit();
                }

                if (null != importConcepts) {
                    int batchsize = 10000;
                    log.debug("Beginning to create concept nodes and relationships.");
                    List<ImportConcept> buffer = new ArrayList<>(batchsize);
                    long imported = 0;
                    while (importConcepts.hasNext()) {
                        while (importConcepts.hasNext() && buffer.size() < batchsize)
                            buffer.add(importConcepts.next());
                        log.debug("Importing a batch of %s concepts", batchsize);
                        try (Transaction tx = graphDb.beginTx()) {
                            CoordinatesMap nodesByCoordinates = new CoordinatesMap();
                            InsertionReport bufferInsertionReport = ConceptInsertion.insertConcepts(tx, buffer, facetId, nodesByCoordinates, importOptions, log);
                            // If the nodesBySrcId map is empty we either have no concepts or
                            // at least no concepts with a source ID. Then,
                            // relationship creation is currently not supported.
                            if (!nodesByCoordinates.isEmpty() && !importOptions.merge) {
                                log.debug("Beginning to create relationships between the imported concepts.");
                                createRelationships(log, tx, buffer, facetId, nodesByCoordinates, importOptions,
                                        bufferInsertionReport);
                            } else
                                log.debug("This is a property merging import, no relationships are created.");
                            insertionReport.merge(bufferInsertionReport);
                            buffer.clear();
                            imported += bufferInsertionReport.numConcepts;
                            tx.commit();
                        }
                        log.debug("Imported %s concepts", imported);
                    }
                    response.put(RET_KEY_NUM_CREATED_CONCEPTS, insertionReport.numConcepts);
                    response.put(RET_KEY_NUM_CREATED_RELS, insertionReport.numRelationships);
                    log.info("Done creating %s concepts and %s relationships.", insertionReport.numConcepts, insertionReport.numRelationships);
                } else {
                    log.info("No concepts were included in the request.");
                }

                time = System.currentTimeMillis() - time;
                response.put(KEY_TIME, time);
                response.put(KEY_FACET_ID, facetId);
                return insertionReport;
            } catch (IOException e) {
                throw new ConceptInsertionException(e);
            }
        }
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
     * @param tx                 The current transaction.
     * @param concepts           The concepts to be imported.
     * @param facetId            The ID of the facet the imported concepts belong to.
     * @param nodesByCoordinates The insertion process specific in-memory map keeping track of inserted nodes.
     * @param importOptions      The concept import options.
     * @return The report of the insertions, counting created nodes, relationships and the passed time.
     * @throws AggregateConceptInsertionException If the insertion of an aggregate concept failed.
     * @throws ConceptInsertionException          If concept insertion failed.
     */
    private static InsertionReport insertConcepts(Transaction tx, List<ImportConcept> concepts, String facetId,
                                                  CoordinatesMap nodesByCoordinates, ImportOptions importOptions, Log log) throws ConceptInsertionException {
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
            for (ImportConcept jsonConcept : concepts) {
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
        for (int i = 0; i < concepts.size(); i++) {
            ImportConcept jsonConcept = concepts.get(i);
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
            concepts.remove(importConceptsToRemove.get(index).intValue());

        log.info("Starting to insert " + concepts.size() + " concepts.");
        for (ImportConcept jsonConcept : concepts) {
            boolean isAggregate = jsonConcept.aggregate;
            if (isAggregate) {
                ConceptAggregateManager.insertAggregateConcept(tx, jsonConcept, nodesByCoordinates, insertionReport,
                        importOptions, log);
            } else {
                insertConcept(tx, facetId, jsonConcept, nodesByCoordinates, insertionReport,
                        importOptions);
            }
        }
        log.debug(concepts.size() + " concepts inserted.");
        time = System.currentTimeMillis() - time;
        log.info(insertionReport.numConcepts
                + " new concepts - but not yet relationships - have been inserted. This took " + time + " ms ("
                + (time / 1000) + " s)");
        return insertionReport;
    }

    private static boolean checkUniqueIdMarkerClash(Node conceptNode, String srcId, boolean uniqueSourceId) {
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
        return !uniqueOnConcept && uniqueOnConcept != uniqueSourceId;
    }

    public static int insertMappings(Transaction tx, Iterator<ImportMapping> mappings) {
        Map<String, Node> nodesBySrcId = new HashMap<>();
        InsertionReport insertionReport = new InsertionReport();

        int count = 0;
        for (ImportMapping mapping : (Iterable<ImportMapping>) () -> mappings) {
            ++count;
            String id1 = mapping.id1;
            String id2 = mapping.id2;
            String mappingType = mapping.mappingType;

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
                Set<String> n2FacetSet = new HashSet<>();
                Set<String> n1FacetSet = new HashSet<>(Arrays.asList(n1Facets));
                Collections.addAll(n2FacetSet, n2Facets);
                if (!Sets.intersection(n1FacetSet, n2FacetSet).isEmpty()) {
                    // Of course an ontology might contain two equivalent
                    // classes; possible they are even asserted to be equal.
                    // But this is nothing LOOM would detect.
                    log.debug("Omitting LOOM mapping between " + id1 + " and " + id2
                            + " because both concepts appear in the same terminology. We assume that the conceptinology does not have two equal concepts and that LOOM is wrong here.");
                    continue;
                }
            }
            insertionReport.addExistingConcept(n1);
            insertionReport.addExistingConcept(n2);
            createRelationShipIfNotExists(n1, n2, ConceptEdgeTypes.IS_MAPPED_TO, insertionReport, Direction.BOTH,
                    ConceptRelationConstants.PROP_MAPPING_TYPE, new String[]{mappingType});
        }
        tx.commit();
        log.info("{} of {} new mappings successfully added.", insertionReport.numRelationships, count);
        return insertionReport.numRelationships;
    }
}
