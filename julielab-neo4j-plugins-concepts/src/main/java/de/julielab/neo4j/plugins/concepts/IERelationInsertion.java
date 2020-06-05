package de.julielab.neo4j.plugins.concepts;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import de.julielab.neo4j.plugins.datarepresentation.*;
import de.julielab.neo4j.plugins.datarepresentation.constants.ImportIERelations;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;
import java.util.stream.StreamSupport;

import static de.julielab.neo4j.plugins.concepts.ConceptLabel.CONCEPT;
import static de.julielab.neo4j.plugins.constants.semedico.SemanticRelationConstants.*;
import static de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants.PROP_ORG_ID;
import static de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants.PROP_SRC_IDS;
import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_ID;

public class IERelationInsertion {
    public static void insertRelations(InputStream ieRelationsStream, Transaction tx, Log log) {
        ObjectMapper mapper = new ObjectMapper().registerModule(new Jdk8Module());
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        JsonParser parser;
        try {
            parser = new JsonFactory(mapper).createParser(ieRelationsStream);
            Iterator<ImportIERelationDocument> documents = null;
            String idProperty = null;
            String idSource = null;
            String lastName = null;
            // We need to stop as soon as we we found the concepts which must be the last property of ImportConcepts
            // object. Then, we use the iterator to continue.
            while (parser.nextToken() != null && documents == null) {
                JsonToken currentToken = parser.currentToken();
                if (currentToken == JsonToken.FIELD_NAME) {
                    lastName = parser.getCurrentName();
                } else if (currentToken == JsonToken.VALUE_STRING) {
                    if (lastName != null && lastName.equals(ImportIERelations.NAME_ID_PROPERTY))
                        idProperty = parser.readValueAs(String.class);
                    else if (lastName != null && lastName.equals(ImportIERelations.NAME_ID_SOURCE))
                        idSource = parser.readValueAs(String.class);
                } else if (lastName != null && lastName.equals(ImportIERelations.NAME_DOCUMENTS) && currentToken == JsonToken.START_ARRAY) {
                    documents = parser.readValuesAs(ImportIERelationDocument.class);
                }
            }
            if (documents == null)
                throw new IllegalArgumentException("No documents were given.");
            insertRelations(tx, idProperty, idSource, documents, log);
        } catch (JsonParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * <p>Inserts all the relations in <tt>documents</tt>.</p>
     *
     * @param tx
     * @param idProperty
     * @param idSource
     * @param documents
     * @param log
     */
    private static void insertRelations(Transaction tx, String idProperty, String idSource, Iterator<ImportIERelationDocument> documents, Log log) {
        while (documents.hasNext()) {
            ImportIERelationDocument document = documents.next();
            for (ImportIETypedRelations typedRelations : document.getRelations()) {
                for (String relationType : typedRelations.keySet()) {
                    ImportIERelation relation = typedRelations.get(relationType);
                    insertRelation(tx, idProperty, idSource, document.getDocId(), relationType, relation, log);
                }
            }
        }
    }

    /**
     * <p>Creates the relationships of type <tt>relationType</tt> between all arguments of <tt>relation</tt>.</p>
     *
     * @param tx
     * @param idProperty
     * @param idSource
     * @param docId
     * @param relationType
     * @param relation
     * @param log
     */
    private static void insertRelation(Transaction tx, String idProperty, String idSource, String docId, String relationType, ImportIERelation relation, Log log) {
        for (int i = 0; i < relation.getArgs().size(); i++) {
            for (int j = i + 1; j < relation.getArgs().size(); j++) {
                ImportIERelationArgument argument1 = relation.getArgs().get(i);
                ImportIERelationArgument argument2 = relation.getArgs().get(j);
                Node arg1 = findConceptNode(tx, idProperty, idSource, argument1, log);
                Node arg2 = findConceptNode(tx, idProperty, idSource, argument2, log);
                if (arg1 != null && arg2 != null)
                    storeRelationTypeCount(tx, arg1, arg2, docId, relationType, relation.getCount());
            }
        }
    }

    private static void storeRelationTypeCount(Transaction tx, Node arg1, Node arg2, String docId, String relationType, int count) {
        RelationshipType relType = RelationshipType.withName(relationType);
        Lock arg1Lock = tx.acquireWriteLock(arg1);
        Lock arg2Lock = tx.acquireWriteLock(arg2);
        Optional<Relationship> relOpt = StreamSupport.stream(arg1.getRelationships(Direction.BOTH, relType).spliterator(), false).filter(r -> r.getOtherNode(arg1).equals(arg2)).findAny();
        Relationship rel = relOpt.isPresent() ? relOpt.get() : arg1.createRelationshipTo(arg2, relType);
        Lock relLock = tx.acquireWriteLock(rel);
        arg1Lock.release();
        arg2Lock.release();
        String[] docIds = rel.hasProperty(PROP_DOC_IDS) ? (String[]) rel.getProperty(PROP_DOC_IDS) : new String[0];
        int index = Arrays.binarySearch(docIds, docId);
        int oldCount = 0;
        if (index >= 0) {
            int[] counts = (int[]) rel.getProperty(PROP_COUNTS);
            oldCount = counts[index];
            counts[index] = count;
        } else {
            int insertionPoint = -1 * (index + 1);
            // insert docId
            String[] newDocIds = new String[docIds.length + 1];
            newDocIds[insertionPoint] = docId;
            System.arraycopy(docIds, 0, newDocIds, 0, insertionPoint);
            System.arraycopy(docIds, insertionPoint, newDocIds, insertionPoint + 1, docIds.length - insertionPoint);

            // insert count
            int[] counts = rel.hasProperty(PROP_COUNTS) ? (int[]) rel.getProperty(PROP_COUNTS) : new int[0];
            int[] newCounts = new int[counts.length + 1];
            newCounts[insertionPoint] = count;
            System.arraycopy(docIds, 0, newDocIds, 0, insertionPoint);
            System.arraycopy(docIds, insertionPoint, newDocIds, insertionPoint + 1, docIds.length - insertionPoint);
        }
        // Update total relation count
        int totalCount = relOpt.isPresent() ? (int) rel.getProperty(PROP_TOTAL_COUNT) : 0;
        totalCount = totalCount - oldCount + count;
        rel.setProperty(PROP_TOTAL_COUNT, totalCount);
        relLock.release();
    }

    private static Node findConceptNode(Transaction tx, String defaultIdProperty, String defaultIdSource, ImportIERelationArgument argument, Log log) {
        Node concept = null;
        String idProperty = argument.hasIdProperty() ? argument.getIdProperty() : defaultIdProperty;
        // Check if the potentially specified idProperty of the argument is valid. Otherwise, fall back to the default ID property.
        boolean isId = PROP_ID.equals(idProperty);
        boolean isOrgId = PROP_ORG_ID.equals(idProperty);
        boolean isSrcId = PROP_SRC_IDS.equals(idProperty);
        if (!isId && !isOrgId && !isSrcId) {
            if (defaultIdProperty == null)
                throw new IllegalArgumentException("The argument " + argument + " does not specify an idProperty and there is no default property set.");
            idProperty = defaultIdProperty;
            isId = PROP_ID.equals(idProperty);
            isOrgId = PROP_ORG_ID.equals(idProperty);
        }
        if (isId)
            concept = tx.findNode(CONCEPT, PROP_ID, argument.getId());
        String source = argument.hasSource() ? argument.getSource() : defaultIdSource;
        if (concept == null)
            concept = ConceptLookup.lookupConcept(tx, new ConceptCoordinates(argument.getId(), source, isOrgId ? CoordinateType.OSRC : CoordinateType.SRC));
        if (concept == null)
            log.debug("Could not find a concept with ID '%s' for idProperty '%s' and source '%s'.", argument.getId(), idProperty, source);
        return concept;
    }
}
