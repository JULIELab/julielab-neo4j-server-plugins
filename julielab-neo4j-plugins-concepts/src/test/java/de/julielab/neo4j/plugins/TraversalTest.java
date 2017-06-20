package de.julielab.neo4j.plugins;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;

import de.julielab.neo4j.plugins.TermManager.MorphoLabel;
import de.julielab.neo4j.plugins.TermManager.TermLabel;
import de.julielab.neo4j.plugins.auxiliaries.PropertyUtilities;
import de.julielab.neo4j.plugins.auxiliaries.semedico.PredefinedTraversals;
import de.julielab.neo4j.plugins.constants.semedico.MorphoConstants;
import de.julielab.neo4j.plugins.constants.semedico.NodeIDPrefixConstants;
import de.julielab.neo4j.plugins.constants.semedico.TermConstants;
import de.julielab.neo4j.plugins.datarepresentation.ImportTermAndFacet;
import de.julielab.neo4j.plugins.datarepresentation.JsonSerializer;
import de.julielab.neo4j.plugins.test.TestUtilities;

public class TraversalTest {
	
	private static GraphDatabaseService graphDb;

	@BeforeClass
	public static void initialize() {
		graphDb = TestUtilities.getGraphDB();
	}

	@Before
	public void cleanForTest() throws IOException {
		TestUtilities.deleteEverythingInDB(graphDb);
	}

	@AfterClass
	public static void shutdown() {
		graphDb.shutdown();
	}
	
	// This test has to be adapted to including document IDs
	@Ignore
	@Test
	public void testGetAcronymsTraversal() throws Exception {
		ImportTermAndFacet testTerms = TermManagerTest.getTestTerms(2);
		TermManager tm = new TermManager();
		tm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms));
		
		Map<String, Integer> acronymCounts = new HashMap<>();
		// acro1 is a shared acronym
		acronymCounts.put("acro1", 2);
		acronymCounts.put("acro2", 42);
		Map<String, Map<String, Integer>> acronyms = new HashMap<>();
		acronyms.put(NodeIDPrefixConstants.TERM+0, acronymCounts);
		
		acronymCounts = new HashMap<>();
		acronymCounts.put("acro1", 4);
		acronymCounts.put("acro3", 7);
		
		tm.addWritingVariants(graphDb, null, JsonSerializer.toJson(acronyms));
		
		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterator<Node> acronymsNodes = graphDb.findNodes(TermManager.MorphoLabel.ACRONYMS);
			assertTrue(acronymsNodes.hasNext());
			ResourceIterator<Node> acronymNodes = graphDb.findNodes(TermManager.MorphoLabel.ACRONYM);
			assertTrue(acronymNodes.hasNext());
			
			Node term0 = graphDb.findNode(TermLabel.TERM, TermConstants.PROP_ID, NodeIDPrefixConstants.TERM + 0);
			TraversalDescription acronymsTraversal = PredefinedTraversals.getAcronymsTraversal(graphDb);
			Traverser traverse = acronymsTraversal.traverse(term0);
			Set<String> expectedAcronyms = new HashSet<>(Arrays.asList("acro1", "acro2"));
			for (Node n : traverse.nodes()) {
				String acronym = (String) n.getProperty(MorphoConstants.PROP_NAME);
				assertTrue(expectedAcronyms.remove(acronym));
			}
			assertTrue(expectedAcronyms.isEmpty());
		}
	}
	
	// This test has to be adapted to including document IDs
		@Ignore
	@Test
	public void testGetWritingVariantsTraversal() throws Exception {
		// this is the same traversal as above, just for writing variants instead of acronyms
		ImportTermAndFacet testTerms = TermManagerTest.getTestTerms(2);
		TermManager tm = new TermManager();
		tm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms));
		
		Map<String, Integer> acronymCounts = new HashMap<>();
		// acro1 is a shared acronym
		acronymCounts.put("variant1", 2);
		acronymCounts.put("variant2", 42);
		Map<String, Map<String, Integer>> acronyms = new HashMap<>();
		acronyms.put(NodeIDPrefixConstants.TERM+0, acronymCounts);
		
		acronymCounts = new HashMap<>();
		acronymCounts.put("variant1", 4);
		acronymCounts.put("variant2", 7);
		
		tm.addWritingVariants(graphDb, JsonSerializer.toJson(acronyms), null);
		
		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterator<Node> variantsNodes = graphDb.findNodes(TermManager.MorphoLabel.WRITING_VARIANTS);
			assertTrue(variantsNodes.hasNext());
			ResourceIterator<Node> variantNodes = graphDb.findNodes(TermManager.MorphoLabel.WRITING_VARIANT);
			assertTrue(variantNodes.hasNext());
			
			Node term0 = graphDb.findNode(TermLabel.TERM, TermConstants.PROP_ID, NodeIDPrefixConstants.TERM + 0);
			TraversalDescription variantTraversal = PredefinedTraversals.getWritingVariantsTraversal(graphDb);
			Traverser traverse = variantTraversal.traverse(term0);
			Set<String> expectedVariants = new HashSet<>(Arrays.asList("variant1", "variant2"));
			for (Node n : traverse.nodes()) {
				String variants = (String) n.getProperty(MorphoConstants.PROP_NAME);
				assertTrue(expectedVariants.remove(variants));
			}
			assertTrue(expectedVariants.isEmpty());
		}
	}
}
