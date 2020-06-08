package de.julielab.neo4j.plugins.concepts;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import de.julielab.neo4j.plugins.datarepresentation.*;
import de.julielab.neo4j.plugins.datarepresentation.constants.ImportIERelations;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
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
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * <p>Inserts all the relations in <tt>documents</tt>.</p>
     *
     * @param tx The current transaction.
     * @param idProperty The concept ID property.
     * @param idSource The optional concept source.
     * @param documents The documents or database entries containing relations.
     * @param log A logger.
     */
    private static void insertRelations(Transaction tx, String idProperty, String idSource, Iterator<ImportIERelationDocument> documents, Log log) {
        Set<IERelationKey> seenDocLevelKeys = new HashSet<>();
        while (documents.hasNext()) {
            ImportIERelationDocument document = documents.next();
            seenDocLevelKeys.clear();
            for (ImportIETypedRelations typedRelations : document.getRelations()) {
                for (String relationType : typedRelations.keySet()) {
                    ImportIERelation relation = typedRelations.get(relationType);
                    insertIERelation(tx, idProperty, idSource, document.getName(), relationType, relation, seenDocLevelKeys, document.isDb(), log);
                }
            }
        }
    }

    /**
     * <p>Creates the relationships of type <tt>relationType</tt> between all arguments of <tt>relation</tt>.</p>
     *
     * @param tx The current transaction.
     * @param idProperty The ID node property.
     * @param idSource The optional concept ID source.
     * @param documentName The document ID or database name to insert relations for.
     * @param relationType The name of the relation type to create.
     * @param relation The relation to insert.
     * @param seenDocLevelKeys Information about relations already seen during the current relation insertion process.
     * @param dbEntry Whether or not this is a database entry.
     * @param log A Logger.
     */
    private static void insertIERelation(Transaction tx, String idProperty, String idSource, String documentName, String relationType, ImportIERelation relation, Set<IERelationKey> seenDocLevelKeys, boolean dbEntry, Log log) {
        for (int i = 0; i < relation.getArgs().size(); i++) {
            for (int j = i + 1; j < relation.getArgs().size(); j++) {
                ImportIERelationArgument argument1 = relation.getArgs().get(i);
                ImportIERelationArgument argument2 = relation.getArgs().get(j);
                Node arg1 = findConceptNode(tx, idProperty, idSource, argument1, log);
                Node arg2 = findConceptNode(tx, idProperty, idSource, argument2, log);
                IERelationKey relationKey = new IERelationKey(relationType, new IEArgumentKey(argument1), new IEArgumentKey(argument2));
                if (arg1 != null && arg2 != null)
                    if (!dbEntry)
                        storeRelationTypeCount(tx, arg1, arg2, documentName, relationType, relation.getCount(), seenDocLevelKeys.contains(relationKey));
                    else
                        storeDBRelation(tx, arg1, arg2, documentName, relationType, relation.getMethod());
                seenDocLevelKeys.add(relationKey);
            }
        }
    }

    private static void storeDBRelation(Transaction tx, Node arg1, Node arg2, String databaseName, String relationType, String method) {
        Pair<Relationship, Boolean> relExistedPair = getRelationship(tx, arg1, arg2, relationType);
        Relationship rel = relExistedPair.getLeft();
        if (rel.hasProperty(PROP_DB_NAMES)) {
            String[] dbNames = (String[]) rel.getProperty(PROP_DB_NAMES);
            int index = Arrays.binarySearch(dbNames, databaseName);
            if (index < 0) {
                int insertionPoint = -1 * (index + 1);
                String[] newDbNames = new String[dbNames.length + 1];
                newDbNames[insertionPoint] = databaseName;
                System.arraycopy(dbNames, 0, newDbNames, 0, insertionPoint);
                System.arraycopy(dbNames, insertionPoint, newDbNames, insertionPoint + 1, dbNames.length - insertionPoint);
                rel.setProperty(PROP_DB_NAMES, newDbNames);

                String[] methods = (String[]) rel.getProperty(PROP_METHODS);
                String[] newMethods = new String[methods.length + 1];
                newMethods[insertionPoint] = method != null && !method.isBlank() ? method : "<unknown>";
                System.arraycopy(methods, 0, newMethods, 0, insertionPoint);
                System.arraycopy(methods, insertionPoint, newMethods, insertionPoint + 1, methods.length - insertionPoint);
                rel.setProperty(PROP_METHODS, newMethods);
            }
        } else {
            rel.setProperty(PROP_DB_NAMES, new String[]{databaseName});
            rel.setProperty(PROP_METHODS, new String[]{method});
        }
    }

    private static Pair<Relationship, Boolean> getRelationship(Transaction tx, Node arg1, Node arg2, String relationType) {
        RelationshipType relType = RelationshipType.withName(relationType);
        Lock arg1Lock = tx.acquireWriteLock(arg1);
        Lock arg2Lock = tx.acquireWriteLock(arg2);
        Optional<Relationship> relOpt = StreamSupport.stream(arg1.getRelationships(Direction.BOTH, relType).spliterator(), false).filter(r -> r.getOtherNode(arg1).equals(arg2)).findAny();
        Relationship rel = relOpt.orElseGet(() -> arg1.createRelationshipTo(arg2, relType));
        arg1Lock.release();
        arg2Lock.release();
        return new ImmutablePair<>(rel, relOpt.isPresent());
    }

    private static void storeRelationTypeCount(Transaction tx, Node arg1, Node arg2, String docId, String relationType, int count, boolean relationAlreadySeen) {
        Pair<Relationship, Boolean> relExistedPair = getRelationship(tx, arg1, arg2, relationType);
        Relationship rel = relExistedPair.getLeft();
        Lock relLock = tx.acquireWriteLock(rel);
        String[] docIds = rel.hasProperty(PROP_DOC_IDS) ? (String[]) rel.getProperty(PROP_DOC_IDS) : new String[0];
        int index = Arrays.binarySearch(docIds, docId);
        int oldCount = 0;
        if (index >= 0) {
            int[] counts = (int[]) rel.getProperty(PROP_COUNTS);
            oldCount = counts[index];
            // We count up if this relation (i.e. same type, same arguments) has already been seen during this import
            // for the current document. Otherwise this is an old number and we overwrite the old value.
            counts[index] = relationAlreadySeen ? oldCount + count : count;
            rel.setProperty(PROP_COUNTS, counts);
        } else {
            int insertionPoint = -1 * (index + 1);
            // insert docId
            String[] newDocIds = new String[docIds.length + 1];
            newDocIds[insertionPoint] = docId;
            System.arraycopy(docIds, 0, newDocIds, 0, insertionPoint);
            System.arraycopy(docIds, insertionPoint, newDocIds, insertionPoint + 1, docIds.length - insertionPoint);
            rel.setProperty(PROP_DOC_IDS, newDocIds);

            // insert count
            int[] counts = rel.hasProperty(PROP_COUNTS) ? (int[]) rel.getProperty(PROP_COUNTS) : new int[0];
            int[] newCounts = new int[counts.length + 1];
            newCounts[insertionPoint] = count;
            System.arraycopy(counts, 0, newCounts, 0, insertionPoint);
            System.arraycopy(counts, insertionPoint, newCounts, insertionPoint + 1, docIds.length - insertionPoint);
            rel.setProperty(PROP_COUNTS, newCounts);
        }
        // Update total relation count
        int totalCount = relExistedPair.getRight() ? (int) rel.getProperty(PROP_TOTAL_COUNT) : 0;
        // Again: When we already had this relation then we count forward and do not overwrite the old value
        totalCount = relationAlreadySeen ? totalCount + count : totalCount - oldCount + count;
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
        if (concept == null && !isId)
            concept = ConceptLookup.lookupConcept(tx, new ConceptCoordinates(argument.getId(), source, isOrgId ? CoordinateType.OSRC : CoordinateType.SRC));
        if (concept == null)
            log.debug("Could not find a concept with ID '%s' for idProperty '%s' and source '%s'.", argument.getId(), idProperty, source);
        return concept;
    }

    private static class IERelationKey {
        private final Set<IEArgumentKey> argKeys;
        private final String relType;

        public IERelationKey(String relationType, IEArgumentKey argkey1, IEArgumentKey argkey2) {
            this.relType = relationType;
            this.argKeys = Set.of(argkey1, argkey2);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IERelationKey that = (IERelationKey) o;
            return argKeys.equals(that.argKeys) &&
                    relType.equals(that.relType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(argKeys, relType);
        }
    }

    private static class IEArgumentKey implements Comparable<IEArgumentKey> {
        private final String id;
        private final String idProperty;
        private final String source;

        public IEArgumentKey(ImportIERelationArgument arg) {
            this.id = arg.getId();
            this.idProperty = arg.getIdProperty();
            this.source = arg.getSource();
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IEArgumentKey that = (IEArgumentKey) o;
            return id.equals(that.id) &&
                    Objects.equals(idProperty, that.idProperty) &&
                    Objects.equals(source, that.source);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, idProperty, source);
        }

        @Override
        public int compareTo(IEArgumentKey o) {
            if (!o.getClass().equals(IEArgumentKey.class))
                throw new IllegalArgumentException("Wrong class to compare with " + IEArgumentKey.class + ": " + o.getClass());
            int cid = o.id.compareTo(id);
            if (cid != 0)
                return cid;
            int cprop = o.idProperty == null || idProperty == null ? getNullComparison(o.idProperty, idProperty) : o.idProperty.compareTo(idProperty);
            if (cprop != 0)
                return cprop;
            return o.source == null || source == null ? getNullComparison(o.source, source) : o.source.compareTo(source);
        }

        public int getNullComparison(Object o1, Object o2) {
            if (o1 == null && o2 != null)
                return -1;
            if (o1 != null && o2 == null)
                return 1;
            return 0;
        }
    }
}
