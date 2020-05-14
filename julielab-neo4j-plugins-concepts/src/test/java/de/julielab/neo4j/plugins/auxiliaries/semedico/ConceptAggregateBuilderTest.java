package de.julielab.neo4j.plugins.auxiliaries.semedico;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import de.julielab.neo4j.plugins.ConceptManager;
import de.julielab.neo4j.plugins.ConceptManager.ConceptLabel;
import de.julielab.neo4j.plugins.FacetManagerTest;
import de.julielab.neo4j.plugins.auxiliaries.PropertyUtilities;
import de.julielab.neo4j.plugins.auxiliaries.semedico.ConceptAggregateBuilder.CopyAggregatePropertiesStatistics;
import de.julielab.neo4j.plugins.datarepresentation.*;
import de.julielab.neo4j.plugins.datarepresentation.constants.AggregateConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.NodeIDPrefixConstants;
import de.julielab.neo4j.plugins.datarepresentation.util.ConceptsJsonSerializer;
import de.julielab.neo4j.plugins.test.TestUtilities;
import org.apache.commons.lang.StringUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.server.rest.repr.RecursiveMappingRepresentation;

import java.io.IOException;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.junit.Assert.*;

public class ConceptAggregateBuilderTest {
	private static GraphDatabaseService graphDb;
	/**
	 * Takes a source ID <code>srcId</code> and creates a coordinate for
	 * <code>(srcId, TEST_SOURCE)</code> with original ID coordinates.
	 */
	private static Function<String, ConceptCoordinates> coords;
	/**
	 * Takes a preferred name and a source ID. Creates an ImportConcept with the
	 * respective name and source ID using {@link #coords}.
	 */
	private static BiFunction<String, String, ImportConcept> cs;

	@BeforeClass
	public static void initialize() {
		graphDb = TestUtilities.getGraphDB();
		coords = srcId -> new ConceptCoordinates(srcId, "TEST_DATA", CoordinateType.SRC);
		cs = (name, srcId) -> new ImportConcept(name, coords.apply(srcId));
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
			aggregate.createRelationshipTo(element1, ConceptManager.EdgeTypes.HAS_ELEMENT);
			aggregate.createRelationshipTo(element2, ConceptManager.EdgeTypes.HAS_ELEMENT);
			aggregate.createRelationshipTo(element3, ConceptManager.EdgeTypes.HAS_ELEMENT);
			aggregate.createRelationshipTo(element4, ConceptManager.EdgeTypes.HAS_ELEMENT);

			// Copy the element properties to the aggregate.
			CopyAggregatePropertiesStatistics copyStats = new CopyAggregatePropertiesStatistics();
			ConceptAggregateBuilder.copyAggregateProperties(aggregate, new String[] { "name", "geschmack", "synonyms" },
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
			// this test sometimes fails because on different machines, the
			// relationships may be ordered differently and another aggregate
			// element comes first
			// assertEquals("orange", synonyms[1]);

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
		ImportConcept t11 = cs.apply("t11", "t11");
		ImportConcept t12 = cs.apply("t12", "t12");
		t12.parentCoordinates = Arrays.asList(coords.apply("t11"));
		ImportConcept t13 = cs.apply("t13", "t13");
		t13.parentCoordinates = Arrays.asList(coords.apply("t12"));
		ArrayList<ImportConcept> terms1 = Lists.newArrayList(t11, t12, t13);
		ImportFacet importFacet1 = FacetManagerTest.getImportFacet();

		ImportConcept t21 = cs.apply("t21", "t21");
		ImportConcept t22 = cs.apply("t22", "t22");
		t22.parentCoordinates = Arrays.asList(coords.apply("t21"));
		ImportConcept t23 = cs.apply("t23", "t3");
		t23.parentCoordinates = Arrays.asList(coords.apply("t22"));
		ArrayList<ImportConcept> terms2 = Lists.newArrayList(t21, t22, t23);
		ImportFacet importFacet2 = FacetManagerTest.getImportFacet();

		List<ImportMapping> mapping = Lists.newArrayList(new ImportMapping("t12", "t21", "EQUAL"));

		ConceptManager tm = new ConceptManager();
		tm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(importFacet1), ConceptsJsonSerializer.toJson(terms1), null);
		tm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(importFacet2), ConceptsJsonSerializer.toJson(terms2), null);
		tm.insertMappings(graphDb, ConceptsJsonSerializer.toJson(mapping));
		Label aggregatedTermsLabel = Label.label("EQUAL_AGG");
		ConceptAggregateBuilder.buildAggregatesForMappings(graphDb, Sets.newHashSet("EQUAL"), null, aggregatedTermsLabel);

		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterable<Node> mappingAggregates = () -> graphDb.findNodes(aggregatedTermsLabel);
			int count = 0;
			for (Node aggregate : mappingAggregates) {
				if (!aggregate.hasLabel(ConceptLabel.AGGREGATE))
					continue;
				count++;

				// Check that all element terms are there
				Set<String> elementIds = new HashSet<>();
				Iterable<Relationship> elementRels = aggregate.getRelationships(ConceptManager.EdgeTypes.HAS_ELEMENT);
				for (Relationship rel : elementRels) {
					Node element = rel.getOtherNode(aggregate);
					String[] srcIds = (String[]) element.getProperty(ConceptConstants.PROP_SRC_IDS);
					elementIds.add(srcIds[0]);
				}
				assertTrue(elementIds.contains(t12.coordinates.sourceId));
				assertTrue(elementIds.contains(t21.coordinates.sourceId));
			}
			assertEquals(1, count);

			tx.success();
		}

		// Test that we can get the "children" of the aggregate, i.e. its
		// elements via the respective FacetManager
		// method
		try (Transaction tx = graphDb.beginTx()) {
			RecursiveMappingRepresentation responseMap = (RecursiveMappingRepresentation) tm.getChildrenOfConcepts(graphDb,
					"[\"" + NodeIDPrefixConstants.AGGREGATE_TERM + 0 + "\"]", aggregatedTermsLabel.name());
			Map<String, Object> map = (Map<String, Object>) responseMap.getUnderlyingMap()
					.get(NodeIDPrefixConstants.AGGREGATE_TERM + 0);
			Map<String, List<String>> reltypes = (Map<String, List<String>>) map.get(ConceptManager.RET_KEY_RELTYPES);
			List<String> list1 = reltypes.get(NodeIDPrefixConstants.TERM + 1);
			assertEquals("HAS_ELEMENT", list1.get(0));
			List<String> list2 = reltypes.get(NodeIDPrefixConstants.TERM + 3);
			assertEquals("HAS_ELEMENT", list2.get(0));
			Set<Node> children = (Set<Node>) map.get(ConceptManager.RET_KEY_CHILDREN);
			Set<String> childrenIds = new HashSet<>();
			for (Node term : children)
				childrenIds.add(((String[]) term.getProperty(ConceptConstants.PROP_SRC_IDS))[0]);
			assertTrue(childrenIds.contains(t12.coordinates.sourceId));
			assertTrue(childrenIds.contains(t21.coordinates.sourceId));
		}

		// Now test whether the removal of aggregates is working as well
		try (Transaction tx = graphDb.beginTx()) {
			ConceptAggregateBuilder.deleteAggregates(graphDb, aggregatedTermsLabel);

			ResourceIterable<Node> mappingAggregates = () -> graphDb.findNodes(aggregatedTermsLabel);
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
		ImportConcept t1 = cs.apply("t1", "t1");
		ImportConcept t2 = cs.apply("t2", "t2");
		t2.parentCoordinates = Arrays.asList(coords.apply("t1"));
		ImportConcept t3 = cs.apply("t3", "t3");
		t3.parentCoordinates = Arrays.asList(coords.apply("t2"));
		ArrayList<ImportConcept> terms1 = Lists.newArrayList(t1, t2, t3);
		ImportFacet importFacet1 = FacetManagerTest.getImportFacet();

		List<ImportMapping> mapping = Lists.newArrayList(new ImportMapping("t1", "t2", "EQUAL"),
				new ImportMapping("t2", "t3", "OTHER_EQUAL"));

		ConceptManager tm = new ConceptManager();
		tm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(importFacet1), ConceptsJsonSerializer.toJson(terms1), null);
		tm.insertMappings(graphDb, ConceptsJsonSerializer.toJson(mapping));
		Label aggLabel = Label.label("EQUAL_AGG");
		ConceptAggregateBuilder.buildAggregatesForMappings(graphDb, Sets.newHashSet("EQUAL", "OTHER_EQUAL"), null,
				aggLabel);

		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterable<Node> mappingAggregates = () -> graphDb.findNodes(aggLabel);
			int count = 0;
			for (Node aggregate : mappingAggregates) {
				count++;

				// Check that all element terms are there
				Set<String> elementIds = new HashSet<>();
				Iterable<Relationship> elementRels = aggregate.getRelationships(ConceptManager.EdgeTypes.HAS_ELEMENT);
				for (Relationship rel : elementRels) {
					Node element = rel.getOtherNode(aggregate);
					String[] srcIds = (String[]) element.getProperty(ConceptConstants.PROP_SRC_IDS);
					elementIds.add(srcIds[0]);
				}
				assertTrue(aggregate.hasLabel(aggLabel));

				assertTrue(elementIds.contains(t1.coordinates.sourceId));
				assertTrue(elementIds.contains(t2.coordinates.sourceId));
				assertTrue(elementIds.contains(t3.coordinates.sourceId));
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
		ImportConcept t1 = cs.apply("t1", "t1");
		ImportConcept t2 = cs.apply("t2", "t2");
		t2.parentCoordinates = Arrays.asList(coords.apply("t1"));
		ImportConcept t3 = cs.apply("t3", "t3");
		t3.parentCoordinates = Arrays.asList(coords.apply("t2"));
		ImportConcept t4 = cs.apply("t4", "t4");
		t4.parentCoordinates = Arrays.asList(coords.apply("t3"));
		ImportConcept t5 = cs.apply("t5", "t5");
		t5.parentCoordinates = Arrays.asList(coords.apply("t4"));
		ImportConcept t6 = cs.apply("t6", "t6");
		t6.parentCoordinates = Arrays.asList(coords.apply("t5"));
		ArrayList<ImportConcept> terms1 = Lists.newArrayList(t1, t2, t3, t4, t5, t6);
		ImportFacet importFacet1 = FacetManagerTest.getImportFacet();

		// Define the mappings. Term 6 is not mapped.
		List<ImportMapping> mapping = Lists.newArrayList(new ImportMapping("t1", "t2", "EQUAL"),
				new ImportMapping("t2", "t3", "OTHER_EQUAL"), new ImportMapping("t4", "t5", "EQUAL"));

		ConceptManager tm = new ConceptManager();
		tm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(importFacet1), ConceptsJsonSerializer.toJson(terms1), null);
		tm.insertMappings(graphDb, ConceptsJsonSerializer.toJson(mapping));
		// The label by which we will identify all nodes representing an
		// aggregated unit, i.e. an actual aggregate node
		// or a term without any mappings that is its own aggregate.
		Label aggLabel = Label.label("EQUAL_AGG");
		ConceptAggregateBuilder.buildAggregatesForMappings(graphDb, Sets.newHashSet("EQUAL", "OTHER_EQUAL"), null,
				aggLabel);

		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterable<Node> mappingAggregates = () -> graphDb.findNodes(aggLabel);
			// Count of the aggregation terms, i.e. the representation terms
			// that have formerly existing terms as their
			// elements.
			int aggCount = 0;
			for (Node aggregate : mappingAggregates) {
				if (!aggregate.hasLabel(ConceptLabel.AGGREGATE))
					continue;
				aggCount++;

				// Check that all element terms are there
				Set<String> elementIds = new HashSet<>();
				Iterable<Relationship> elementRels = aggregate.getRelationships(ConceptManager.EdgeTypes.HAS_ELEMENT);
				for (Relationship rel : elementRels) {
					Node element = rel.getOtherNode(aggregate);
					String[] srcIds = (String[]) element.getProperty(ConceptConstants.PROP_SRC_IDS);
					elementIds.add(srcIds[0]);
				}
				if (elementIds.size() == 3) {
					assertTrue(elementIds.contains(t1.coordinates.sourceId));
					assertTrue(elementIds.contains(t2.coordinates.sourceId));
					assertTrue(elementIds.contains(t3.coordinates.sourceId));
				}
				if (elementIds.size() == 2) {
					assertTrue(elementIds.contains(t4.coordinates.sourceId));
					assertTrue(elementIds.contains(t5.coordinates.sourceId));
				}
			}
			assertEquals(2, aggCount);

			ResourceIterable<Node> aggregatedTerms = () -> graphDb.findNodes(aggLabel);
			// Count of all terms that represent the result of the aggegation,
			// i.e. aggregate terms as well as original
			// terms that are no element of an aggregate term and as such "are
			// their own aggregate".
			int aggregatedTermsCount = 0;
			for (Node aggregatedTerm : aggregatedTerms) {
				aggregatedTermsCount++;

				// Check that all element terms are there
				Iterable<Relationship> elementRels = aggregatedTerm
						.getRelationships(ConceptManager.EdgeTypes.HAS_ELEMENT);
				Iterator<Relationship> elementIt = elementRels.iterator();
				if (!elementIt.hasNext()) {
					String[] srcIds = (String[]) aggregatedTerm.getProperty(ConceptConstants.PROP_SRC_IDS);
					assertEquals(t6.coordinates.sourceId, srcIds[0]);
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
