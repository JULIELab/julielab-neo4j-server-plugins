package de.julielab.neo4j.plugins;

import de.julielab.neo4j.plugins.auxiliaries.semedico.SequenceManager;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.PUT;
import javax.ws.rs.QueryParam;
import java.util.Arrays;
import java.util.Set;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class Indexes {
    public static final String CREATE_INDEXES = "create_indexes";
    public static final String DB_NAME = "db_name";
    private final static Logger log = LoggerFactory.getLogger(Indexes.class);
    private DatabaseManagementService dbms;

    public Indexes(DatabaseManagementService dbms) {
        this.dbms = dbms;
    }

    /**
     * Checks whether an automatic index for the <tt>label</tt> exists on the
     * <tt>property</tt> and creates it, if not.
     *
     * @param tx         The current transaction.
     * @param label      The node label to create the index for.
     * @param properties The node property to create the index for.
     * @param unique     Whether or not to impose a unique constraint for this label/property combination.
     */
    public static void createSinglePropertyIndexIfAbsent(Transaction tx, Label label, boolean unique, String... properties) {
        Schema schema = tx.schema();
        Set<String> propertySet = Set.of(properties);
        int foundProperties = 0;
        for (IndexDefinition id : schema.getIndexes(label)) {
            for (String propertyKey : id.getPropertyKeys()) {
                if (propertySet.contains(propertyKey))
                    ++foundProperties;
            }
        }
        boolean indexExists = foundProperties == properties.length;
        if (!indexExists) {
            log.info("Creating index for label {} on properties {} (unique: {}).", label, Arrays.toString(properties), unique);
            if (!unique) {
                IndexCreator indexCreator = schema.indexFor(label);
                for (String property : properties) {
                    indexCreator = indexCreator.on(property);
                }
                indexCreator.create();
            } else {
                if (properties.length > 1)
                    throw new IllegalArgumentException("Passed multiple properties for a unique constraint. Unique constraints are only supported on single properties in the Neo4j community edition.");
                schema.constraintFor(label).assertPropertyIsUnique(properties[0]).create();
            }
        }
    }

    /**
     * Creates uniqueness constraints (and thus, indexes), on the following label / property combinations:
     * <ul>
     *
     * <li>CONCEPT /  ConceptConstants.PROP_ID</li>
     * <li>CONCEPT / ConceptConstants.PROP_ORG_ID</li>
     * <li>FACET / FacetConstants.PROP_ID</li>
     * <li>NO_FACET /  FacetConstants.PROP_ID</li>
     * <li>ROOT / NodeConstants.PROP_NAME</li>
     *  This should be done after the main initial import because node insertion with uniqueness switched on costs significant insertion performance.
     * </ul>
     *
     * @param databaseName The name of the database to create the indexes in.
     */
    @PUT
    @javax.ws.rs.Path("/{" + CREATE_INDEXES + "}")
    public void createIndexes(@QueryParam(DB_NAME) String databaseName) {
        final String effectiveDbName = databaseName == null ? DEFAULT_DATABASE_NAME : databaseName;
        try (Transaction tx = dbms.database(effectiveDbName).beginTx()) {
            ConceptManager.createIndexes(tx);
            SequenceManager.createIndexes(tx);
            FacetManager.createIndexes(tx);
            tx.commit();
        }
    }

}
