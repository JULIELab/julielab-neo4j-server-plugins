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
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;

import de.julielab.neo4j.plugins.ConceptManager.ConceptLabel;
import de.julielab.neo4j.plugins.auxiliaries.semedico.PredefinedTraversals;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;
import de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.MorphoConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.NodeIDPrefixConstants;
import de.julielab.neo4j.plugins.datarepresentation.util.ConceptsJsonSerializer;
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

	@Test
	public void testGetAcronymsTraversal() throws Exception {
		ImportConcepts testTerms = ConceptManagerTest.getTestTerms(2);
		ConceptManager tm = new ConceptManager();
		tm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms));

		Map<String, Integer> acronymCounts = new HashMap<>();
		// acro1 is a shared acronym
		acronymCounts.put("acro1", 2);
		acronymCounts.put("acro2", 42);
		Map<String, Map<String, Integer>> docs = new HashMap<>();
		docs.put("doc1", acronymCounts);
		Map<String, Map<String, Map<String, Integer>>> acronyms = new HashMap<>();
		acronyms.put(NodeIDPrefixConstants.TERM + 0, docs);

		acronymCounts = new HashMap<>();
		acronymCounts.put("acro1", 4);
		acronymCounts.put("acro3", 7);

		tm.addWritingVariants(graphDb, null, ConceptsJsonSerializer.toJson(acronyms));

		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterator<Node> acronymsNodes = graphDb.findNodes(ConceptManager.MorphoLabel.ACRONYMS);
			assertTrue(acronymsNodes.hasNext());
			ResourceIterator<Node> acronymNodes = graphDb.findNodes(ConceptManager.MorphoLabel.ACRONYM);
			assertTrue(acronymNodes.hasNext());

			Node term0 = graphDb.findNode(ConceptLabel.CONCEPT, ConceptConstants.PROP_ID, NodeIDPrefixConstants.TERM + 0);
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

	@Test
	public void testGetWritingVariantsTraversal() throws Exception {
		// this is the same traversal as above, just for writing variants
		// instead of acronyms
		ImportConcepts testTerms = ConceptManagerTest.getTestTerms(2);
		ConceptManager tm = new ConceptManager();
		tm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms));

		Map<String, Integer> variantCounts = new HashMap<>();
		// acro1 is a shared acronym
		variantCounts.put("variant1", 2);
		variantCounts.put("variant2", 42);
		Map<String, Map<String, Integer>> docs = new HashMap<>();
		docs.put("doc1", variantCounts);
		Map<String, Map<String, Map<String, Integer>>> variants = new HashMap<>();
		variants.put(NodeIDPrefixConstants.TERM + 0, docs);

		variantCounts = new HashMap<>();
		variantCounts.put("variant1", 4);
		variantCounts.put("variant2", 7);

		tm.addWritingVariants(graphDb, ConceptsJsonSerializer.toJson(variants), null);

		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterator<Node> variantsNodes = graphDb.findNodes(ConceptManager.MorphoLabel.WRITING_VARIANTS);
			assertTrue(variantsNodes.hasNext());
			ResourceIterator<Node> variantNodes = graphDb.findNodes(ConceptManager.MorphoLabel.WRITING_VARIANT);
			assertTrue(variantNodes.hasNext());

			Node term0 = graphDb.findNode(ConceptLabel.CONCEPT, ConceptConstants.PROP_ID, NodeIDPrefixConstants.TERM + 0);
			TraversalDescription variantTraversal = PredefinedTraversals.getWritingVariantsTraversal(graphDb);
			Traverser traverse = variantTraversal.traverse(term0);
			Set<String> expectedVariants = new HashSet<>(Arrays.asList("variant1", "variant2"));
			for (Node n : traverse.nodes()) {
				String variant = (String) n.getProperty(MorphoConstants.PROP_NAME);
				assertTrue(expectedVariants.remove(variant));
			}
			assertTrue(expectedVariants.isEmpty());
		}
	}
}
