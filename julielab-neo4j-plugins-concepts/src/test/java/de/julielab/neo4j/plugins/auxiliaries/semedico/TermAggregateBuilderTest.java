package de.julielab.neo4j.plugins.auxiliaries.semedico;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import de.julielab.neo4j.plugins.FacetManagerTest;
import de.julielab.neo4j.plugins.TermManager;
import de.julielab.neo4j.plugins.TermManager.TermLabel;
import de.julielab.neo4j.plugins.auxiliaries.PropertyUtilities;
import de.julielab.neo4j.plugins.auxiliaries.RecursiveMappingRepresentation;
import de.julielab.neo4j.plugins.auxiliaries.semedico.TermAggregateBuilder;
import de.julielab.neo4j.plugins.auxiliaries.semedico.TermAggregateBuilder.CopyAggregatePropertiesStatistics;
import de.julielab.neo4j.plugins.constants.semedico.AggregateConstants;
import de.julielab.neo4j.plugins.constants.semedico.NodeIDPrefixConstants;
import de.julielab.neo4j.plugins.constants.semedico.TermConstants;
import de.julielab.neo4j.plugins.datarepresentation.ImportFacet;
import de.julielab.neo4j.plugins.datarepresentation.ImportMapping;
import de.julielab.neo4j.plugins.datarepresentation.ImportTerm;
import de.julielab.neo4j.plugins.datarepresentation.JsonSerializer;
import de.julielab.neo4j.plugins.test.TestUtilities;

public class TermAggregateBuilderTest {
	private static GraphDatabaseService graphDb;

	@BeforeClass
	public static void initialize() {
		graphDb = TestUtilities.getGraphDB();
	}

	@Before
	public void cleanForTest() throws IOException {
		TestUtilities.deleteEverythingInDB(graphDb);
	}

	@Test
	public void testCopyAggregateProperties() {
		try (Transaction tx = graphDb.beginTx()) {
			// Create the aggregate node and its element nodes
			Node aggregate = graphDb.createNode();
			Node element1 = graphDb.createNode();
			Node element2 = graphDb.createNode();
			Node element3 = graphDb.createNode();
			Node element4 = graphDb.createNode();

			// Set some properties to the element nodes that should then be
			// copied to the aggregate.
			element1.setProperty("name", "apfelsine");
			element1.setProperty("geschmack", new String[] { "suess", "saftig" });
			element1.setProperty("synonyms", new String[] { "oraNgE" });
			element2.setProperty("name", "apfelsine");
			element2.setProperty("geschmack", new String[] { "fruchtig", "spritzig" });
			element2.setProperty("synonyms", new String[] { "orange" });
			element3.setProperty("name", "orange");
			element4.setProperty("name", "orangendings");
			element4.setProperty("synonyms", new String[] { "apfelsine" });

			// Connect the element nodes to the aggregate.
			aggregate.createRelationshipTo(element1, TermManager.EdgeTypes.HAS_ELEMENT);
			aggregate.createRelationshipTo(element2, TermManager.EdgeTypes.HAS_ELEMENT);
			aggregate.createRelationshipTo(element3, TermManager.EdgeTypes.HAS_ELEMENT);
			aggregate.createRelationshipTo(element4, TermManager.EdgeTypes.HAS_ELEMENT);

			// Copy the element properties to the aggregate.
			CopyAggregatePropertiesStatistics copyStats = new CopyAggregatePropertiesStatistics();
			TermAggregateBuilder.copyAggregateProperties(aggregate, new String[] { "name", "geschmack", "synonyms" },
					copyStats);

			// Check whether everything is as expected.
			// There should be as name the majority name "apfelsine".
			String name = (String) PropertyUtilities.getNonNullNodeProperty(aggregate, "name");
			assertEquals("apfelsine", name);
			// Since there were two other names - "orange" and "organgendings" -
			// we expect those names to be stored in a
			// special property of the aggregate.
			String[] minorityNames = (String[]) PropertyUtilities.getNonNullNodeProperty(aggregate,
					"name" + AggregateConstants.SUFFIX_DIVERGENT_ELEMENT_ROPERTY);
			assertNotNull("The minority property \"" + "name" + AggregateConstants.SUFFIX_DIVERGENT_ELEMENT_ROPERTY
					+ "\" is null", minorityNames);
			List<String> minorityNamesList = Arrays.asList(minorityNames);
			assertTrue(minorityNamesList.contains("orange"));
			assertTrue(minorityNamesList.contains("orangendings"));

			String[] geschmaecker = (String[]) PropertyUtilities.getNonNullNodeProperty(aggregate, "geschmack");
			List<String> geschmaeckerList = Arrays.asList(geschmaecker);
			assertTrue(geschmaeckerList.contains("fruchtig"));
			assertTrue(geschmaeckerList.contains("saftig"));
			assertTrue(geschmaeckerList.contains("spritzig"));
			assertTrue(geschmaeckerList.contains("suess"));
			assertEquals("Alle geschmaecker: " + StringUtils.join(geschmaeckerList, ", "), 4, geschmaeckerList.size());

			String[] synonyms = (String[]) PropertyUtilities.getNonNullNodeProperty(aggregate, "synonyms");
			assertNotNull("Synonyms sind null", synonyms);
			assertEquals("Synonyms are: " + StringUtils.join(synonyms, ", "), 2, synonyms.length);
			assertEquals("apfelsine", synonyms[0]);
			// note: we make actually no effort to decide for a specific
			// writing variant (here: orange vs. oraNgE) so this test might fail
			// if the original array order changes for any reason
			// this test sometimes fails because on different machines, the relationships may be ordered differently and another aggregate element comes first
//			assertEquals("orange", synonyms[1]);

			tx.success();
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testBuildAggregatesForMappingsSimpleCase() throws Exception {
		// In this test we will define two facets with a path of three terms,
		// respectively, where two term of those
		// parts are mapped to each other. Thus, we will expect that after
		// aggregation, the two paths will be connected
		// through
		// one aggregation node.
		ImportTerm t11 = new ImportTerm("t11", "t11");
		ImportTerm t12 = new ImportTerm("t12", "t12");
		t12.parentSrcIds = Lists.newArrayList("t11");
		ImportTerm t13 = new ImportTerm("t13", "t13");
		t13.parentSrcIds = Lists.newArrayList("t12");
		ArrayList<ImportTerm> terms1 = Lists.newArrayList(t11, t12, t13);
		ImportFacet importFacet1 = FacetManagerTest.getImportFacet();

		ImportTerm t21 = new ImportTerm("t21", "t21");
		ImportTerm t22 = new ImportTerm("t22", "t22");
		t22.parentSrcIds = Lists.newArrayList("t21");
		ImportTerm t23 = new ImportTerm("t23", "t23");
		t23.parentSrcIds = Lists.newArrayList("t22");
		ArrayList<ImportTerm> terms2 = Lists.newArrayList(t21, t22, t23);
		ImportFacet importFacet2 = FacetManagerTest.getImportFacet();

		List<ImportMapping> mapping = Lists.newArrayList(new ImportMapping("t12", "t21", "EQUAL"));

		TermManager tm = new TermManager();
		tm.insertFacetTerms(graphDb, JsonSerializer.toJson(importFacet1), JsonSerializer.toJson(terms1), null);
		tm.insertFacetTerms(graphDb, JsonSerializer.toJson(importFacet2), JsonSerializer.toJson(terms2), null);
		tm.insertMappings(graphDb, JsonSerializer.toJson(mapping));
		Label aggregatedTermsLabel = DynamicLabel.label("EQUAL_AGG");
		TermAggregateBuilder.buildAggregatesForMappings(graphDb, Sets.newHashSet("EQUAL"), null, aggregatedTermsLabel);

		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterable<Node> mappingAggregates = GlobalGraphOperations.at(graphDb)
					.getAllNodesWithLabel(aggregatedTermsLabel);
			int count = 0;
			for (Node aggregate : mappingAggregates) {
				if (!aggregate.hasLabel(TermLabel.AGGREGATE))
					continue;
				count++;

				// Check that all element terms are there
				Set<String> elementIds = new HashSet<>();
				Iterable<Relationship> elementRels = aggregate.getRelationships(TermManager.EdgeTypes.HAS_ELEMENT);
				for (Relationship rel : elementRels) {
					Node element = rel.getOtherNode(aggregate);
					String[] srcIds = (String[]) element.getProperty(TermConstants.PROP_SRC_IDS);
					elementIds.add(srcIds[0]);
				}
				assertTrue(elementIds.contains(t12.sourceId));
				assertTrue(elementIds.contains(t21.sourceId));
			}
			assertEquals(1, count);

			tx.success();
		}

		// Test that we can get the "children" of the aggregate, i.e. its
		// elements via the respective FacetManager
		// method
		try (Transaction tx = graphDb.beginTx()) {
			RecursiveMappingRepresentation responseMap = (RecursiveMappingRepresentation) tm.getChildrenOfTerms(graphDb,
					"[\"" + NodeIDPrefixConstants.AGGREGATE_TERM + 0 + "\"]", aggregatedTermsLabel.name());
			Map<String, Object> map = (Map<String, Object>) responseMap.getUnderlyingMap()
					.get(NodeIDPrefixConstants.AGGREGATE_TERM + 0);
			Map<String, List<String>> reltypes = (Map<String, List<String>>) map.get(TermManager.RET_KEY_RELTYPES);
			List<String> list1 = reltypes.get(NodeIDPrefixConstants.TERM + 1);
			assertEquals("HAS_ELEMENT", list1.get(0));
			List<String> list2 = reltypes.get(NodeIDPrefixConstants.TERM + 3);
			assertEquals("HAS_ELEMENT", list2.get(0));
			Set<Node> children = (Set<Node>) map.get(TermManager.RET_KEY_CHILDREN);
			Set<String> childrenIds = new HashSet<>();
			for (Node term : children)
				childrenIds.add(((String[]) term.getProperty(TermConstants.PROP_SRC_IDS))[0]);
			assertTrue(childrenIds.contains(t12.sourceId));
			assertTrue(childrenIds.contains(t21.sourceId));
		}

		// Now test whether the removal of aggregates is working as well
		try (Transaction tx = graphDb.beginTx()) {
			TermAggregateBuilder.deleteAggregates(graphDb, aggregatedTermsLabel);

			ResourceIterable<Node> mappingAggregates = GlobalGraphOperations.at(graphDb)
					.getAllNodesWithLabel(aggregatedTermsLabel);
			int count = 0;
			for (@SuppressWarnings("unused")
			Node aggregate : mappingAggregates) {
				count++;
			}
			assertEquals(0, count);

			tx.success();
		}

	}

	@Test
	public void testBuildAggregatesForMappingsTransitiveCase() throws Exception {
		// In this test we will define a single path of three terms where the
		// first two terms are mapped by one mapping
		// type and the last two (thus with the same term "in the middle") by
		// another type (we use two types because if
		// it would be the same, the three terms would form a "mapping-clique"
		// most probably in reality).
		// The algorithm should recognize the transitive mapping by first
		// creating a "small" aggregate only spanning two
		// terms and later including the third term.
		ImportTerm t1 = new ImportTerm("t1", "t1");
		ImportTerm t2 = new ImportTerm("t2", "t2");
		t2.parentSrcIds = Lists.newArrayList("t1");
		ImportTerm t3 = new ImportTerm("t3", "t3");
		t3.parentSrcIds = Lists.newArrayList("t2");
		ArrayList<ImportTerm> terms1 = Lists.newArrayList(t1, t2, t3);
		ImportFacet importFacet1 = FacetManagerTest.getImportFacet();

		List<ImportMapping> mapping = Lists.newArrayList(new ImportMapping("t1", "t2", "EQUAL"),
				new ImportMapping("t2", "t3", "OTHER_EQUAL"));

		TermManager tm = new TermManager();
		tm.insertFacetTerms(graphDb, JsonSerializer.toJson(importFacet1), JsonSerializer.toJson(terms1), null);
		tm.insertMappings(graphDb, JsonSerializer.toJson(mapping));
		Label aggLabel = DynamicLabel.label("EQUAL_AGG");
		TermAggregateBuilder.buildAggregatesForMappings(graphDb, Sets.newHashSet("EQUAL", "OTHER_EQUAL"), null,
				aggLabel);

		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterable<Node> mappingAggregates = GlobalGraphOperations.at(graphDb).getAllNodesWithLabel(aggLabel);
			int count = 0;
			for (Node aggregate : mappingAggregates) {
				count++;

				// Check that all element terms are there
				Set<String> elementIds = new HashSet<>();
				Iterable<Relationship> elementRels = aggregate.getRelationships(TermManager.EdgeTypes.HAS_ELEMENT);
				for (Relationship rel : elementRels) {
					Node element = rel.getOtherNode(aggregate);
					String[] srcIds = (String[]) element.getProperty(TermConstants.PROP_SRC_IDS);
					elementIds.add(srcIds[0]);
				}
				assertTrue(aggregate.hasLabel(aggLabel));

				assertTrue(elementIds.contains(t1.sourceId));
				assertTrue(elementIds.contains(t2.sourceId));
				assertTrue(elementIds.contains(t3.sourceId));
			}
			assertEquals(1, count);
		}
	}

	@Test
	public void testBuildAggregatesForMappingsTransitiveCaseTwoSets() throws Exception {
		// Here, we build three distinct sets of nodes:
		// The first three nodes are mapped to each other transitively.
		// Terms 4 and 5 are mapped to each other.
		// Term 6 is not mapped at all and thus forms "its own aggregate".
		ImportTerm t1 = new ImportTerm("t1", "t1");
		ImportTerm t2 = new ImportTerm("t2", "t2");
		t2.parentSrcIds = Lists.newArrayList("t1");
		ImportTerm t3 = new ImportTerm("t3", "t3");
		t3.parentSrcIds = Lists.newArrayList("t2");
		ImportTerm t4 = new ImportTerm("t4", "t4");
		t4.parentSrcIds = Lists.newArrayList("t3");
		ImportTerm t5 = new ImportTerm("t5", "t5");
		t5.parentSrcIds = Lists.newArrayList("t4");
		ImportTerm t6 = new ImportTerm("t6", "t6");
		t6.parentSrcIds = Lists.newArrayList("t5");
		ArrayList<ImportTerm> terms1 = Lists.newArrayList(t1, t2, t3, t4, t5, t6);
		ImportFacet importFacet1 = FacetManagerTest.getImportFacet();

		// Define the mappings. Term 6 is not mapped.
		List<ImportMapping> mapping = Lists.newArrayList(new ImportMapping("t1", "t2", "EQUAL"),
				new ImportMapping("t2", "t3", "OTHER_EQUAL"), new ImportMapping("t4", "t5", "EQUAL"));

		TermManager tm = new TermManager();
		tm.insertFacetTerms(graphDb, JsonSerializer.toJson(importFacet1), JsonSerializer.toJson(terms1), null);
		tm.insertMappings(graphDb, JsonSerializer.toJson(mapping));
		// The label by which we will identify all nodes representing an
		// aggregated unit, i.e. an actual aggregate node
		// or a term without any mappings that is its own aggregate.
		Label aggLabel = DynamicLabel.label("EQUAL_AGG");
		TermAggregateBuilder.buildAggregatesForMappings(graphDb, Sets.newHashSet("EQUAL", "OTHER_EQUAL"), null,
				aggLabel);

		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterable<Node> mappingAggregates = GlobalGraphOperations.at(graphDb).getAllNodesWithLabel(aggLabel);
			// Count of the aggregation terms, i.e. the representation terms
			// that have formerly existing terms as their
			// elements.
			int aggCount = 0;
			for (Node aggregate : mappingAggregates) {
				if (!aggregate.hasLabel(TermLabel.AGGREGATE))
					continue;
				aggCount++;

				// Check that all element terms are there
				Set<String> elementIds = new HashSet<>();
				Iterable<Relationship> elementRels = aggregate.getRelationships(TermManager.EdgeTypes.HAS_ELEMENT);
				for (Relationship rel : elementRels) {
					Node element = rel.getOtherNode(aggregate);
					String[] srcIds = (String[]) element.getProperty(TermConstants.PROP_SRC_IDS);
					elementIds.add(srcIds[0]);
				}
				if (elementIds.size() == 3) {
					assertTrue(elementIds.contains(t1.sourceId));
					assertTrue(elementIds.contains(t2.sourceId));
					assertTrue(elementIds.contains(t3.sourceId));
				}
				if (elementIds.size() == 2) {
					assertTrue(elementIds.contains(t4.sourceId));
					assertTrue(elementIds.contains(t5.sourceId));
				}
			}
			assertEquals(2, aggCount);

			ResourceIterable<Node> aggregatedTerms = GlobalGraphOperations.at(graphDb).getAllNodesWithLabel(aggLabel);
			// Count of all terms that represent the result of the aggegation,
			// i.e. aggregate terms as well as original
			// terms that are no element of an aggregate term and as such "are
			// their own aggregate".
			int aggregatedTermsCount = 0;
			for (Node aggregatedTerm : aggregatedTerms) {
				aggregatedTermsCount++;

				// Check that all element terms are there
				Iterable<Relationship> elementRels = aggregatedTerm.getRelationships(TermManager.EdgeTypes.HAS_ELEMENT);
				Iterator<Relationship> elementIt = elementRels.iterator();
				if (!elementIt.hasNext()) {
					String[] srcIds = (String[]) aggregatedTerm.getProperty(TermConstants.PROP_SRC_IDS);
					assertEquals(t6.sourceId, srcIds[0]);
				}
			}
			assertEquals(3, aggregatedTermsCount);
		}
	}

	@AfterClass
	public static void shutdown() {
		graphDb.shutdown();
	}
}
