package de.julielab.neo4j.plugins;

import de.julielab.neo4j.plugins.auxiliaries.semedico.PredefinedTraversals;
import de.julielab.neo4j.plugins.concepts.ConceptLabel;
import de.julielab.neo4j.plugins.concepts.ConceptManager;
import de.julielab.neo4j.plugins.concepts.MorphoLabel;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;
import de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.MorphoConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.NodeIDPrefixConstants;
import de.julielab.neo4j.plugins.datarepresentation.util.ConceptsJsonSerializer;
import de.julielab.neo4j.plugins.test.TestUtilities;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;

import java.io.ByteArrayInputStream;
import java.util.*;

import static de.julielab.neo4j.plugins.concepts.ConceptManager.KEY_CONCEPT_ACRONYMS;
import static de.julielab.neo4j.plugins.concepts.ConceptManager.KEY_CONCEPT_TERMS;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class TraversalTest {

	private static GraphDatabaseService graphDb;
	private static DatabaseManagementService graphDBMS;

	@BeforeClass
	public static void initialize() {
		graphDBMS = TestUtilities.getGraphDBMS();
		graphDb = graphDBMS.database(DEFAULT_DATABASE_NAME);
	}

	@Before
	public void cleanForTest() {
		TestUtilities.deleteEverythingInDB(graphDb);
		new Indexes(graphDBMS).createIndexes(DEFAULT_DATABASE_NAME);
	}

	@AfterClass
	public static void shutdown() {
		graphDBMS.shutdown();
	}

	@Test
	public void testGetAcronymsTraversal() throws Exception {
		ImportConcepts importConcepts = ConceptManagerTest.getTestConcepts(2);
		ConceptManager tm = new ConceptManager(graphDBMS);
		tm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(importConcepts).getBytes(UTF_8)));

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

		tm.addWritingVariants(ConceptsJsonSerializer.toJson(Map.of(KEY_CONCEPT_ACRONYMS, ConceptsJsonSerializer.toJson( acronyms))));

		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterator<Node> acronymsNodes = tx.findNodes(MorphoLabel.ACRONYMS);
			assertTrue(acronymsNodes.hasNext());
			ResourceIterator<Node> acronymNodes = tx.findNodes(MorphoLabel.ACRONYM);
			assertTrue(acronymNodes.hasNext());

			Node term0 = tx.findNode(ConceptLabel.CONCEPT, ConceptConstants.PROP_ID, NodeIDPrefixConstants.TERM + 0);
			TraversalDescription acronymsTraversal = PredefinedTraversals.getAcronymsTraversal(tx);
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
		ImportConcepts testTerms = ConceptManagerTest.getTestConcepts(2);
		ConceptManager tm = new ConceptManager(graphDBMS);
		tm.insertConcepts(new ByteArrayInputStream(ConceptsJsonSerializer.toJson(testTerms).getBytes(UTF_8)));

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

		tm.addWritingVariants(ConceptsJsonSerializer.toJson(Map.of(KEY_CONCEPT_TERMS, ConceptsJsonSerializer.toJson(variants))));

		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterator<Node> variantsNodes = tx.findNodes(MorphoLabel.WRITING_VARIANTS);
			assertTrue(variantsNodes.hasNext());
			ResourceIterator<Node> variantNodes = tx.findNodes(MorphoLabel.WRITING_VARIANT);
			assertTrue(variantNodes.hasNext());

			Node term0 = tx.findNode(ConceptLabel.CONCEPT, ConceptConstants.PROP_ID, NodeIDPrefixConstants.TERM + 0);
			TraversalDescription variantTraversal = PredefinedTraversals.getWritingVariantsTraversal(tx);
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
