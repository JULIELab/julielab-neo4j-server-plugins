package de.julielab.neo4j.plugins;

import com.google.common.collect.Lists;
import de.julielab.neo4j.plugins.auxiliaries.NodeUtilities;
import de.julielab.neo4j.plugins.datarepresentation.ConceptCoordinates;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcept;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;
import de.julielab.neo4j.plugins.datarepresentation.util.ConceptsJsonSerializer;
import de.julielab.neo4j.plugins.test.TestUtilities;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Method;
import java.util.zip.GZIPInputStream;

import static de.julielab.neo4j.plugins.ConceptManager.ConceptLabel.CONCEPT;
import static org.junit.Assert.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class ExportTest {

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
    }

    @Test
    public void createIdMappingOneFacetSourceIds() throws Exception {
        ImportConcepts importConcepts = ConceptManagerTest.getTestTerms(10);
        importConcepts.getFacet().setName("facet1");
        for (ImportConcept term : importConcepts.getConcepts()) {
            term.generalLabels = Lists.newArrayList("TESTLABEL");
            // clear the original ID and source of the test terms for this test
            term.coordinates.originalId = null;
            term.coordinates.originalSource = null;
        }
        importConcepts.getConcepts().get(0).coordinates.originalId = "orgId1";
        importConcepts.getConcepts().get(0).coordinates.originalSource = "src1";
        importConcepts.getConcepts().get(1).coordinates.originalId = "orgId2";
        importConcepts.getConcepts().get(1).coordinates.originalSource = "src2";
        importConcepts.getConcepts().get(2).coordinates.originalId = "orgId3";
        importConcepts.getConcepts().get(2).coordinates.originalSource = "src3";
        // We create a second term for orgId2 - this will result in a single new term but with multiple source IDs.
        ImportConcept term = new ImportConcept("Added Last PrefName", new ConceptCoordinates("addedLastSourceId", "TEST_SOURCE", "orgId2", "src2"));
        importConcepts.getConcepts().add(term);
        ConceptManager tm = new ConceptManager(graphDBMS);
        tm.insertConcepts(ConceptsJsonSerializer.toJson(importConcepts));
        // Get some more terms; those will be in another label and should be ignored here.
        importConcepts = ConceptManagerTest.getTestTerms(15);
        importConcepts.getFacet().setName("facet2");
        for (ImportConcept t : importConcepts.getConcepts()) {
            t.coordinates.originalId = null;
            t.coordinates.originalSource = null;
        }

        tm.insertConcepts(ConceptsJsonSerializer.toJson(importConcepts));

        // Assure we have two facets.
        try (Transaction tx = graphDb.beginTx()) {
            ResourceIterator<Node> allNodesWithLabel = tx.findNodes(FacetManager.FacetLabel.FACET);
            int facetCount = 0;
            while (allNodesWithLabel.hasNext()) {
                Node facet = allNodesWithLabel.next();
                System.out.println("Got facet node: " + NodeUtilities.getNodePropertiesAsString(facet));
                facetCount++;
            }
            assertEquals(2, facetCount);
        }

        Method method = Export.class.getDeclaredMethod("createIdMapping", String.class,
                String[].class);
        method.setAccessible(true);
        Export export = new Export(graphDBMS);
        String[] labelsArray = new String[]{"TESTLABEL"};
        ByteArrayOutputStream baos;
        byte[] byteArray;
        ByteArrayInputStream bais;
        GZIPInputStream gzis;
        String fileContent;

        // create mapping file contents for sourceIDs
        baos = (ByteArrayOutputStream) method.invoke(export,  "sourceIds", labelsArray);
        byteArray = baos.toByteArray();
        bais = new ByteArrayInputStream(byteArray);
        gzis = new GZIPInputStream(bais);
        fileContent = IOUtils.toString(gzis, "UTF-8");

        // We have inserted 15 terms
        long numTerms;
        try (Transaction tx = graphDb.beginTx()) {
            numTerms = tx.findNodes(CONCEPT).stream().count();
        }
        assertEquals(15, numTerms);
        int countMatches = StringUtils.countMatches(fileContent, "\n");
        // All terms have a source ID, in the test case one term has two source IDs, thus we should have 11 mapping
        // lines.
        assertEquals(11, countMatches);

        // NEXT TEST
        // create mapping file contents for original Ids
        baos = (ByteArrayOutputStream) method.invoke(export,  "originalId", labelsArray);
        byteArray = baos.toByteArray();
        bais = new ByteArrayInputStream(byteArray);
        gzis = new GZIPInputStream(bais);
        fileContent = IOUtils.toString(gzis, "UTF-8");

        // We have inserted 15 terms
        try (Transaction tx = graphDb.beginTx()) {
            numTerms = tx.findNodes(CONCEPT).stream().count();
        }
        assertEquals(15, numTerms);
        countMatches = StringUtils.countMatches(fileContent, "\n");
        // only three terms have an original ID. No term can have more than one original ID.
        assertEquals(3, countMatches);
    }

    @Test
    public void createIdMappingOneFacetOriginalId() throws Exception {
        ImportConcepts importConcepts = ConceptManagerTest.getTestTerms(10);
        for (ImportConcept term : importConcepts.getConcepts()) {
            term.generalLabels = Lists.newArrayList("TESTLABEL");
            term.coordinates.originalId = null;
            term.coordinates.originalSource = null;
        }
        importConcepts.getConcepts().get(0).coordinates.originalId = "orgId1";
        importConcepts.getConcepts().get(0).coordinates.originalSource = "src1";
        importConcepts.getConcepts().get(1).coordinates.originalId = "orgId2";
        importConcepts.getConcepts().get(1).coordinates.originalSource = "src2";
        importConcepts.getConcepts().get(2).coordinates.originalId = "orgId3";
        importConcepts.getConcepts().get(2).coordinates.originalSource = "src3";
        // We create a second term for orgId2 - this will result in a single new term but with multiple source IDs.
        ImportConcept term = new ImportConcept("Added Last PrefName", new ConceptCoordinates("addedLastSourceId", "TEST_SOURCE", "orgId2", "src2"));
        term.coordinates.originalId = "orgId2";
        term.coordinates.originalSource = "src2";
        importConcepts.getConcepts().add(term);
        ConceptManager tm = new ConceptManager(graphDBMS);
        tm.insertConcepts(ConceptsJsonSerializer.toJson(importConcepts));

        Method method = Export.class.getDeclaredMethod("createIdMapping", String.class,
                String[].class);
        method.setAccessible(true);
        Export export = new Export(graphDBMS);
        String[] labelsArray = new String[]{"TESTLABEL"};
        ByteArrayOutputStream baos;
        byte[] byteArray;
        ByteArrayInputStream bais;
        GZIPInputStream gzis;
        String fileContent;

        // create mapping file contents for original Ids
        baos = (ByteArrayOutputStream) method.invoke(export,  "originalId", labelsArray);
        byteArray = baos.toByteArray();
        bais = new ByteArrayInputStream(byteArray);
        gzis = new GZIPInputStream(bais);
        fileContent = IOUtils.toString(gzis, "UTF-8");

        // We have inserted 10 terms
        long numTerms;
        try (Transaction tx = graphDb.beginTx()) {
            numTerms = tx.findNodes(CONCEPT).stream().count();
        }
        assertEquals(10, numTerms);
        int countMatches = StringUtils.countMatches(fileContent, "\n");
        // only three terms have an original ID. No term can have more than one original ID.
        assertEquals(3, countMatches);
    }

}
