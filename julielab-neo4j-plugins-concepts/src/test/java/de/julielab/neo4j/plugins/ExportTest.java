package de.julielab.neo4j.plugins;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Method;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.shell.util.json.JSONArray;

import com.google.common.collect.Lists;

import de.julielab.neo4j.plugins.auxiliaries.NodeUtilities;
import de.julielab.neo4j.plugins.datarepresentation.ConceptCoordinates;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcept;
import de.julielab.neo4j.plugins.datarepresentation.ImportConceptAndFacet;
import de.julielab.neo4j.plugins.datarepresentation.JsonSerializer;
import de.julielab.neo4j.plugins.test.TestUtilities;

public class ExportTest {
	
	private static GraphDatabaseService graphDb;

	@BeforeClass
	public static void initialize() {
		graphDb = TestUtilities.getGraphDB();
	}

	@Before
	public void cleanForTest() {
		TestUtilities.deleteEverythingInDB(graphDb);
	}

	@AfterClass
	public static void shutdown() {
		graphDb.shutdown();
	}

	@Test
	public void createIdMappingOneFacetSourceIds() throws Exception {
		ImportConceptAndFacet testTerms = ConceptManagerTest.getTestTerms(10);
		testTerms.facet.name = "facet1";
		for (ImportConcept term : testTerms.terms) { 
			term.generalLabels = Lists.newArrayList("TESTLABEL");
			// clear the original ID and source of the test terms for this test
			term.coordinates.originalId = null;
			term.coordinates.originalSource = null;
		}
		testTerms.terms.get(0).coordinates.originalId = "orgId1";
		testTerms.terms.get(0).coordinates.originalSource = "src1";
		testTerms.terms.get(1).coordinates.originalId = "orgId2";
		testTerms.terms.get(1).coordinates.originalSource = "src2";
		testTerms.terms.get(2).coordinates.originalId = "orgId3";
		testTerms.terms.get(2).coordinates.originalSource = "src3";
		// We create a second term for orgId2 - this will result in a single new term but with multiple source IDs.
		ImportConcept term = new ImportConcept("Added Last PrefName", new ConceptCoordinates("addedLastSourceId", "TEST_SOURCE", "orgId2", "src2"));
		testTerms.terms.add(term);
		ConceptManager tm = new ConceptManager();
		tm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms));
		// Get some more terms; those will be in another label and should be ignored here.
		testTerms = ConceptManagerTest.getTestTerms(15);
		testTerms.facet.name = "facet2";
		for (ImportConcept t : testTerms.terms) {
			t.coordinates.originalId = null;
			t.coordinates.originalSource = null;
		}

		tm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms));

		// Assure we have two facets.
		try (Transaction tx = graphDb.beginTx();
				ResourceIterator<Node> allNodesWithLabel = graphDb.findNodes(FacetManager.FacetLabel.FACET)){
			int facetCount = 0;
			while (allNodesWithLabel.hasNext()) {
				Node facet = allNodesWithLabel.next();
				System.out.println("Got facet node: "  + NodeUtilities.getNodePropertiesAsString(facet));
				facetCount++;
			}
			assertEquals(2, facetCount);
		}
		
		Method method = Export.class.getDeclaredMethod("createIdMapping", GraphDatabaseService.class, String.class,
				JSONArray.class);
		method.setAccessible(true);
		Export export = new Export();
		JSONArray labelsArray = new JSONArray(Lists.newArrayList("TESTLABEL"));
		
		// create mapping file contents for sourceIDs
		try (ByteArrayOutputStream baos = (ByteArrayOutputStream) method.invoke(export, graphDb, "sourceIds", labelsArray)) {
			byte[] byteArray = baos.toByteArray();
			ByteArrayInputStream bais = new ByteArrayInputStream(byteArray);
			GZIPInputStream gzis = new GZIPInputStream(bais);
			String fileContent = IOUtils.toString(gzis, "UTF-8");
	
			// We have inserted 15 terms
			long numTerms = tm.getNumTerms(graphDb);
			assertEquals(15, numTerms);
			int countMatches = StringUtils.countMatches(fileContent, "\n");
			// All terms have a source ID, in the test case one term has two source IDs, thus we should have 11 mapping
			// lines.
			assertEquals(11, countMatches);
		}

		// NEXT TEST
		// create mapping file contents for original Ids
		try (ByteArrayOutputStream baos = (ByteArrayOutputStream) method.invoke(export, graphDb, "originalId", labelsArray)) {
			byte[] byteArray = baos.toByteArray();
			ByteArrayInputStream bais = new ByteArrayInputStream(byteArray);
			GZIPInputStream gzis = new GZIPInputStream(bais);
			String fileContent = IOUtils.toString(gzis, "UTF-8");
	
			// We have inserted 15 terms
			long numTerms = tm.getNumTerms(graphDb);
			assertEquals(15, numTerms);
			int countMatches = StringUtils.countMatches(fileContent, "\n");
			// only three terms have an original ID. No term can have more than one original ID.
			assertEquals(3, countMatches);
		}
	}

	@Test
	public void createIdMappingOneFacetOriginalId() throws Exception {
		ImportConceptAndFacet testTerms = ConceptManagerTest.getTestTerms(10);
		for (ImportConcept term : testTerms.terms) {
			term.generalLabels = Lists.newArrayList("TESTLABEL");
			term.coordinates.originalId = null;
			term.coordinates.originalSource = null;
		}
		testTerms.terms.get(0).coordinates.originalId = "orgId1";
		testTerms.terms.get(0).coordinates.originalSource = "src1";
		testTerms.terms.get(1).coordinates.originalId = "orgId2";
		testTerms.terms.get(1).coordinates.originalSource = "src2";
		testTerms.terms.get(2).coordinates.originalId = "orgId3";
		testTerms.terms.get(2).coordinates.originalSource = "src3";
		// We create a second term for orgId2 - this will result in a single new term but with multiple source IDs.
		ImportConcept term = new ImportConcept("Added Last PrefName", new ConceptCoordinates("addedLastSourceId", "TEST_SOURCE", "orgId2", "src2"));
		term.coordinates.originalId = "orgId2";
		term.coordinates.originalSource = "src2";
		testTerms.terms.add(term);
		ConceptManager tm = new ConceptManager();
		tm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms));

		Method method = Export.class.getDeclaredMethod("createIdMapping", GraphDatabaseService.class, String.class,
				JSONArray.class);
		method.setAccessible(true);
		Export export = new Export();
		JSONArray labelsArray = new JSONArray(Lists.newArrayList("TESTLABEL"));

		// create mapping file contents for original Ids
		try (ByteArrayOutputStream baos = (ByteArrayOutputStream) method.invoke(export, graphDb, "originalId", labelsArray)) {
			byte[] byteArray = baos.toByteArray();
			ByteArrayInputStream bais = new ByteArrayInputStream(byteArray);
			GZIPInputStream gzis = new GZIPInputStream(bais);
			String fileContent = IOUtils.toString(gzis, "UTF-8");
	
			// We have inserted 10 terms
			long numTerms = tm.getNumTerms(graphDb);
			assertEquals(10, numTerms);
			int countMatches = StringUtils.countMatches(fileContent, "\n");
			// only three terms have an original ID. No term can have more than one original ID.
			assertEquals(3, countMatches);
		}
	}

}
