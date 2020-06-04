package de.julielab.neo4j.plugins;

import de.julielab.neo4j.plugins.concepts.ConceptInsertion;
import de.julielab.neo4j.plugins.concepts.ConceptManager;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;
import de.julielab.neo4j.plugins.datarepresentation.util.ConceptsJsonSerializer;
import de.julielab.neo4j.plugins.test.TestUtilities;
import org.junit.*;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;

import static de.julielab.neo4j.plugins.concepts.ConceptLabel.CONCEPT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@Ignore
public class ImportTest {

    private static GraphDatabaseService graphDb;
    private static DatabaseManagementService graphDBMS;

    @BeforeClass
    public static void initialize() {
        graphDBMS = TestUtilities.getGraphDBMS();
        graphDb = graphDBMS.database(DEFAULT_DATABASE_NAME);
    }

    @AfterClass
    public static void shutdown() {
        graphDBMS.shutdown();
    }

    @Before
    public void cleanForTest() {
        TestUtilities.deleteEverythingInDB(graphDb);
        new Indexes(graphDBMS).createIndexes((String) null);
    }

    @Test
    public void testLargeImport() {
        Thread t = new Thread() {
            @Override
            public void run() {
                while (true) {
                    System.out.println("Concept lookup time: " + ConceptInsertion.lookupTime);
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        t.start();

        ImportConcepts importConcepts = ConceptManagerTest.getTestConcepts(100000);
        ConceptManager cm = new ConceptManager(graphDBMS);
        cm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(importConcepts).getBytes(UTF_8)));
    }

    @Test
    public void testIndex() {
        try (Transaction tx = graphDb.beginTx()) {
            tx.execute("CALL db.createIndex($indexName, $label, $property, $indexProvider)", Map.of("indexName", "TestIndex", "label", List.of(CONCEPT.name()), "property", List.of("testprop"), "indexProvider", Indexes.PROVIDER_LUCENE_NATIVE_1_0));
            tx.commit();
        }
        try (Transaction tx = graphDb.beginTx()) {
            Node n = tx.createNode(CONCEPT);
            n.setProperty("testprop", new String[]{"val1", "val2"});
            tx.commit();
        }
        try (Transaction tx = graphDb.beginTx()) {
            Node node = tx.findNode(CONCEPT, "testprop", new String[]{"val1", "val2"});
            System.out.println(node);
        }
    }

    @Test
    public void testIndex2() {
        String indexName = "TestIndex";
        String property = "sourceIds";
        try (Transaction tx = graphDb.beginTx()) {
            tx.execute("CALL db.index.fulltext.createNodeIndex($indexName, $labels, $properties, $indexSettings)", Map.of("indexName", indexName, "labels", List.of(CONCEPT.name()), "properties", List.of(property), "indexSettings", Map.of("analyzer", "whitespace" )));
            tx.commit();
        }
        try (Transaction tx = graphDb.beginTx()) {
            for (int i = 0; i < 50000; i++) {
                Node n = tx.createNode(CONCEPT);
                n.setProperty(property, "CONCEPT" + i + " CONCEPT" + (i + 1));
            }
            for (int i = 0; i < 100; i++) {
//                try (Transaction tx = graphDb.beginTx()) {
                long time = System.currentTimeMillis();
                Node node = null;
                String searchvalue = "CONCEPT" + i;
                try (ResourceIterator<Node> it = tx.execute("CALL db.index.fulltext.queryNodes($indexName, $query)", Map.of("indexName", indexName, "query", property + ":\"" + searchvalue + "\"")).columnAs("node")) {
                    if (it.hasNext())
                        node = it.next();
                }
                time = System.currentTimeMillis() - time;
                System.out.println("Found " + node + " in " + time + "ms");
            }
        }
    }
}
