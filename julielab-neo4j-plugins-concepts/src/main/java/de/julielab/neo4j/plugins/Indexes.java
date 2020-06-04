package de.julielab.neo4j.plugins;

import de.julielab.neo4j.plugins.auxiliaries.semedico.SequenceManager;
import de.julielab.neo4j.plugins.concepts.ConceptManager;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import java.util.List;
import java.util.Map;

import static de.julielab.neo4j.plugins.Indexes.INDEXES_REST_ENDPOINT;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@Path("/" + INDEXES_REST_ENDPOINT)
public class Indexes {
    public static final String INDEXES_REST_ENDPOINT = "indexes";
    public static final String CREATE_INDEXES = "create_indexes";
    public static final String DB_NAME = "db_name";

    public static final String PROVIDER_NATIVE_1_0 = "native-btree-1.0";
    public static final String PROVIDER_LUCENE_NATIVE_1_0 = "lucene+native-3.0";

    private final static Logger log = LoggerFactory.getLogger(Indexes.class);
    private final DatabaseManagementService dbms;

    public Indexes(@Context DatabaseManagementService dbms) {
        this.dbms = dbms;
    }

    /**
     * Checks whether an automatic index for the <tt>label</tt> exists on the
     * <tt>property</tt> and creates it, if not.
     *
     * @param tx       The current transaction.
     * @param label    The node label to create the index for.
     * @param property The node property to create the index for.
     * @param unique   Whether or not to impose a unique constraint for this label/property combination.
     */
    public static void createSinglePropertyIndexIfAbsent(Transaction tx, String indexName, Label label, boolean unique, String indexProvider, String property) {
        String indexCreationProcedure = unique ? "createUniquePropertyConstraint" : "createIndex";
        if (indexProvider == null)
            tx.execute("CALL db." + indexCreationProcedure + "($indexName, $label, $property)", Map.of("indexName", indexName, "label", List.of(label.name()), "property", List.of(property)));
        else
            tx.execute("CALL db." + indexCreationProcedure + "($indexName, $label, $property, $indexProvider)", Map.of("indexName", indexName, "label", List.of(label.name()), "property", List.of(property), "indexProvider", indexProvider));
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
    @javax.ws.rs.Path(CREATE_INDEXES)
    public void createIndexes(@QueryParam(DB_NAME) String databaseName) {
        final String effectiveDbName = databaseName == null ? DEFAULT_DATABASE_NAME : databaseName;
        GraphDatabaseService graphDb = dbms.database(effectiveDbName);
        createIndexes(graphDb);
    }

    public void createIndexes(GraphDatabaseService graphDb) {
        try (Transaction tx = graphDb.beginTx()) {
            ConceptManager.createIndexes(tx);
            SequenceManager.createIndexes(tx);
            FacetManager.createIndexes(tx);
            tx.commit();
        }
    }

}
