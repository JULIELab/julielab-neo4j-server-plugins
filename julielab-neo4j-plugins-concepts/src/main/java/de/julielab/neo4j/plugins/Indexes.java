package de.julielab.neo4j.plugins;

import de.julielab.neo4j.plugins.auxiliaries.semedico.SequenceManager;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.PUT;
import javax.ws.rs.QueryParam;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class Indexes {
    private final static Logger log = LoggerFactory.getLogger(Indexes.class);
    public static final String CREATE_INDEXES = "create_indexes";
    public static final String DB_NAME = "db_name";
    private DatabaseManagementService dbms;

    public Indexes(DatabaseManagementService dbms) {
        this.dbms = dbms;
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
     * @param databaseName The name of the database to create the indexes in.
     */
    @PUT
    @javax.ws.rs.Path("/{" + CREATE_INDEXES + "}")
    public void createIndexes(@QueryParam(DB_NAME)String databaseName) {
        final String effectiveDbName = databaseName == null ? DEFAULT_DATABASE_NAME : databaseName;
        try(Transaction tx = dbms.database(effectiveDbName).beginTx()) {
            ConceptManager.createIndexes(tx);
            SequenceManager.createIndexes(tx);
            FacetManager.createIndexes(tx);
            tx.commit();
        }
    }

    /**
     * Checks whether an automatic index for the <tt>label</tt> exists on the
     * <tt>property</tt> and creates it, if not.
     *
     * @param tx The current transaction.
     * @param label The node label to create the index for.
     * @param property The node property to create the index for.
     * @param unique Whether or not to impose a unique constraint for this label/property combination.
     */
    public static void createSinglePropertyIndexIfAbsent(Transaction tx, Label label, String property, boolean unique) {
        Schema schema = tx.schema();
        boolean indexExists = false;
        for (IndexDefinition id : schema.getIndexes(label)) {
            for (String propertyKey : id.getPropertyKeys()) {
                if (propertyKey.equals(property))
                    indexExists = true;
            }
        }
        if (!indexExists) {
            log.info("Creating index for label " + label + " on property " + property + " (unique: " + unique + ").");
            schema.constraintFor(label).assertPropertyIsUnique(property).create();
        }
    }

}
