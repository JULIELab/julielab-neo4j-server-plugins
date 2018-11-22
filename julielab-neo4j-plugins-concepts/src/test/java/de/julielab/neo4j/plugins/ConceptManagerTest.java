package de.julielab.neo4j.plugins;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import de.julielab.neo4j.plugins.ConceptManager.ConceptLabel;
import de.julielab.neo4j.plugins.ConceptManager.EdgeTypes;
import de.julielab.neo4j.plugins.ConceptManager.MorphoLabel;
import de.julielab.neo4j.plugins.FacetManager.FacetLabel;
import de.julielab.neo4j.plugins.auxiliaries.NodeUtilities;
import de.julielab.neo4j.plugins.auxiliaries.PropertyUtilities;
import de.julielab.neo4j.plugins.auxiliaries.semedico.ConceptAggregateBuilder;
import de.julielab.neo4j.plugins.auxiliaries.semedico.TermNameAndSynonymComparator;
import de.julielab.neo4j.plugins.datarepresentation.*;
import de.julielab.neo4j.plugins.datarepresentation.constants.*;
import de.julielab.neo4j.plugins.datarepresentation.util.ConceptsJsonSerializer;
import de.julielab.neo4j.plugins.test.TestUtilities;
import de.julielab.neo4j.plugins.util.AggregateConceptInsertionException;
import de.julielab.neo4j.plugins.util.ConceptInsertionException;
import org.apache.commons.lang.StringUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.server.rest.repr.RecursiveMappingRepresentation;
import org.neo4j.shell.util.json.JSONException;
import org.neo4j.shell.util.json.JSONObject;

import java.util.*;
import java.util.stream.Stream;

import static de.julielab.neo4j.plugins.datarepresentation.CoordinateType.SRC;
import static de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants.*;
import static de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants.NAME_NO_FACET_GROUPS;
import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_ID;
import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_NAME;
import static org.junit.Assert.*;

public class ConceptManagerTest {

	private static GraphDatabaseService graphDb;

	@BeforeClass
	public static void initialize() {
		graphDb = TestUtilities.getGraphDB();
	}

	@Before
	public void cleanForTest()  {
		TestUtilities.deleteEverythingInDB(graphDb);
	}

	@AfterClass
	public static void shutdown() {
		graphDb.shutdown();
	}

	@Test
	public void testOriginalIdMerging() throws Exception {
		// This test checks whether multiple, different sources and terms with
		// the same origin (same original ID and
		// original source) are stored correctly.
		ConceptManager tm = new ConceptManager();

		ImportConcepts testTerms;
		testTerms = getTestTerms(1);
		testTerms.getConceptsAsList().get(0).coordinates.originalId = "orgId";
		testTerms.getConceptsAsList().get(0).coordinates.originalSource = "src1";
		testTerms.getConceptsAsList().get(0).coordinates.source = "src1";
		tm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms));

		testTerms = getTestTerms(1);
		testTerms.getConceptsAsList().get(0).coordinates.originalId = "orgId";
		testTerms.getConceptsAsList().get(0).coordinates.originalSource = "src1";
		testTerms.getConceptsAsList().get(0).coordinates.source = "src2";
		tm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms));

		testTerms = getTestTerms(1);
		testTerms.getConceptsAsList().get(0).coordinates.sourceId = "CONCEPT42";
		testTerms.getConceptsAsList().get(0).coordinates.originalId = "orgId";
		testTerms.getConceptsAsList().get(0).coordinates.originalSource = "src1";
		testTerms.getConceptsAsList().get(0).coordinates.source = null;
		tm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms));

		try (Transaction tx = graphDb.beginTx()) {
			Node term = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, ConceptManager.ConceptLabel.CONCEPT,
					PROP_ID, NodeIDPrefixConstants.TERM + 0);
			assertNotNull(term);
			assertEquals("orgId", term.getProperty(PROP_ORG_ID));
			String[] sourceIds = (String[]) term.getProperty(PROP_SRC_IDS);
			String[] sources = (String[]) term.getProperty(ConceptConstants.PROP_SOURCES);
			assertArrayEquals(new String[] { "src1", "src2", "<unknown>" }, sources);
			assertArrayEquals(new String[] { "CONCEPT0", "CONCEPT0", "CONCEPT42" }, sourceIds);
            tx.success();
		}

	}

	@Test
	public void testMergeOnOriginalIdWithoutSourceId() throws Exception {
		// This test assures that pure term merging works by addressing terms
		// via their original ID without
		// specification of their source ID.
		ConceptManager tm = new ConceptManager();

		ImportConcepts testTerms;
		testTerms = getTestTerms(1);
		testTerms.getConceptsAsList().get(0).coordinates.originalId = "orgId";
		testTerms.getConceptsAsList().get(0).coordinates.originalSource = "src1";
		testTerms.getConceptsAsList().get(0).coordinates.source = "someSource";
		testTerms.getConceptsAsList().get(0).descriptions = null;
		tm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms));

		try (Transaction tx = graphDb.beginTx()) {
			Node term = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, ConceptManager.ConceptLabel.CONCEPT,
					PROP_ID, NodeIDPrefixConstants.TERM + 0);
			assertNotNull(term);
			assertFalse(term.hasProperty(PROP_DESCRIPTIONS));
            tx.success();
		}

		// Now add a description, only by knowing the term's original ID.
		testTerms = getTestTerms(1);
		testTerms.getConceptsAsList().get(0).coordinates.originalId = "orgId";
		testTerms.getConceptsAsList().get(0).coordinates.originalSource = "src1";
		testTerms.getConceptsAsList().get(0).coordinates.sourceId = null;
		testTerms.getConceptsAsList().get(0).descriptions = Lists.newArrayList("desc");
		ImportOptions importOptions = new ImportOptions();
		importOptions.merge = true;
		testTerms.setImportOptions(importOptions);
		tm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms));

		try (Transaction tx = graphDb.beginTx()) {
			Node term = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, ConceptManager.ConceptLabel.CONCEPT,
					PROP_ID, NodeIDPrefixConstants.TERM + 0);
			assertNotNull(term);
			assertTrue(term.hasProperty(PROP_DESCRIPTIONS));
			assertArrayEquals(new String[] { "desc" }, (String[]) term.getProperty(PROP_DESCRIPTIONS));
			assertArrayEquals(new String[] { "someSource" }, (String[]) term.getProperty(PROP_SOURCES));
            tx.success();
		}

	}

	@Test
	public void testSameOrgIdDifferentSource() throws Exception {
		// This test assures that two terms with the same original ID are still
		// different if the original sources
		// differ.
		ConceptManager tm = new ConceptManager();

		ImportConcepts testTerms;
		testTerms = getTestTerms(1);
		testTerms.getConceptsAsList().get(0).coordinates.originalId = "orgId";
		testTerms.getConceptsAsList().get(0).coordinates.originalSource = "src1";
		// we also have to set the source ID and source of at least one term to
		// a different value or we will get an exception telling us that our
		// data is inconsistent (because source ID and source would match but
		// the original ID and original source wouldn't)
		testTerms.getConceptsAsList().get(0).coordinates.sourceId = "anothersourceid";
		testTerms.getConceptsAsList().get(0).coordinates.source = "anothersource";
		tm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms));

		testTerms = getTestTerms(1);
		testTerms.getConceptsAsList().get(0).coordinates.originalId = "orgId";
		testTerms.getConceptsAsList().get(0).coordinates.originalSource = "src2";
		tm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms));

		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterable<Node> terms = () -> graphDb.findNodes(ConceptLabel.CONCEPT);
			int counter = 0;
			Set<String> idSet = Sets.newHashSet("src1", "src2");
			for (Node term : terms) {
				assertEquals("orgId", term.getProperty(PROP_ORG_ID));
				assertTrue(idSet.remove(term.getProperty(PROP_ORG_SRC)));
				counter++;
			}
			// There should be two terms even though they have the same original
			// id
			assertEquals(2, counter);
            tx.success();
		}

	}

	/**
	 * Tests the import of a (small) set of terms together with their complete facet
	 * definition. The facet should be created and the terms be created and
	 * connected to it.
	 * 
	 *
	 */
	@Test
	public void testImportConceptsWithFacetDefinition() throws JSONException, SecurityException,
			IllegalArgumentException, ConceptInsertionException {
		testTermImportWithOrWithoutFacetDefinition(true);
	}

	@Test
	public void testImportConceptsWithoutFacetDefinition() throws Exception {
		// Here, create the facet "manually" and then do the term import. The
		// import method "knows" which facet Id to use (there will only be this
		// one facet, so its fid0...)
		ImportFacet facetMap = FacetManagerTest.getImportFacet();

		String jsonFacet = ConceptsJsonSerializer.toJson(facetMap);
		FacetManager.createFacet(graphDb, new JSONObject(jsonFacet));

		// Do the term import and tests.
		testTermImportWithOrWithoutFacetDefinition(false);
	}

	private void testTermImportWithOrWithoutFacetDefinition(boolean withFacetDefinition) throws JSONException, ConceptInsertionException {
		// ----------- THE FACET --------------
		ImportFacet facetMap;
		if (withFacetDefinition) {
			facetMap = FacetManagerTest.getImportFacet();
		} else {
			// The facet has already been created and only its ID has been
			// given.
			facetMap = new ImportFacet("fid0");
		}

		// ----------- THE CONCEPTS ---------------
		// Structure:
		// CONCEPT1
		// ===== CONCEPT2
		// ---------- CONCEPT4
		// ----- CONCEPT3
		// Note the ===== relationship: We will insert term2 twice but we expect
		// that only ONE relationship is created in the end, i.e. the duplicate
		// is recognized.
		List<ImportConcept> termList = new ArrayList<>();
		ConceptCoordinates coord1 = new ConceptCoordinates("CONCEPT1", "TEST_SOURCE", SRC);
		ConceptCoordinates coord2 = new ConceptCoordinates("CONCEPT2", "TEST_SOURCE", "orgId2", "orgSrc1");
		ConceptCoordinates coord3 = new ConceptCoordinates("CONCEPT3", "TEST_SOURCE", SRC);
		ConceptCoordinates coord4 = new ConceptCoordinates("CONCEPT4", "TEST_SOURCE", SRC);
		termList.add(new ImportConcept("prefname1", "desc of term1", coord1));
		termList.add(new ImportConcept("prefname2", coord2, coord1));
		// duplicate of term 2 to test relationship de-duplication.
		termList.add(new ImportConcept("prefname2", coord2, coord1));
		termList.add(new ImportConcept("prefname3", coord3, coord1));
		termList.add(new ImportConcept("prefname4", coord4, coord2));

		Map<String, Object> termsAndFacet = new HashMap<>();
		termsAndFacet.put("facet", facetMap);
		termsAndFacet.put("concepts", termList);

		// --------- CREATE JSON AND SEND DATA --------
		String termsAndFacetBytes = ConceptsJsonSerializer.toJson(termsAndFacet);
		ConceptManager ftm = new ConceptManager();
		ftm.insertConcepts(graphDb, termsAndFacetBytes);

		// --------- MAKE TESTS ---------------

		// Is the facet there?
		IndexManager im = graphDb.index();
		try (Transaction tx = graphDb.beginTx()) {
			Node facet = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, FacetManager.FacetLabel.FACET, PROP_ID,
					"fid0");
			assertEquals("testfacet1", facet.getProperty(PROP_NAME));

			// Are the term Properties correct?
			Index<Node> termIdIndex = im.forNodes(ConceptConstants.INDEX_NAME);
			IndexHits<Node> terms = termIdIndex.query(PROP_ID, "tid*");
			assertEquals(4, terms.size());

			Node term1 = termIdIndex.get(PROP_ID, "tid0").getSingle();
			assertEquals("prefname1", term1.getProperty(PROP_PREF_NAME));
			assertEquals(Lists.newArrayList("desc of term1"),
					Arrays.asList((String[]) term1.getProperty(PROP_DESCRIPTIONS)));
			assertArrayEquals(new String[] { "CONCEPT1" }, (String[]) term1.getProperty(PROP_SRC_IDS));

			Node term2 = termIdIndex.get(PROP_ID, "tid1").getSingle();
			assertEquals("prefname2", term2.getProperty(PROP_PREF_NAME));
			assertEquals("orgId2", term2.getProperty(PROP_ORG_ID));
			assertArrayEquals(new String[] { "CONCEPT2" }, (String[]) term2.getProperty(PROP_SRC_IDS));

			Node term3 = termIdIndex.get(PROP_ID, "tid2").getSingle();
			assertEquals("prefname3", term3.getProperty(PROP_PREF_NAME));
			assertArrayEquals(new String[] { "CONCEPT3" }, (String[]) term3.getProperty(PROP_SRC_IDS));

			Node term4 = termIdIndex.get(PROP_ID, "tid3").getSingle();
			assertEquals("prefname4", term4.getProperty(PROP_PREF_NAME));
			assertArrayEquals(new String[] { "CONCEPT4" }, (String[]) term4.getProperty(PROP_SRC_IDS));

			// Are the relationships correct? Reminder, they should be:
			// Structure:
			// CONCEPT1
			// ----- CONCEPT2
			// ---------- CONCEPT4
			// ----- CONCEPT3
			// where there is only ONE relationship from CONCEPT1 to CONCEPT2 although
			// we
			// have added CONCEPT2 twice. This is tested automatically by using
			// "getSingleRelationShip".
			// For simplicity, we here ask the child for its parent because in
			// this
			// test, there is only one parent (in general, there might be
			// more!).
			assertEquals(term1,
					term2.getSingleRelationship(ConceptManager.EdgeTypes.IS_BROADER_THAN, Direction.INCOMING)
							.getStartNode());
			assertEquals(term1,
					term3.getSingleRelationship(ConceptManager.EdgeTypes.IS_BROADER_THAN, Direction.INCOMING)
							.getStartNode());
			assertEquals(term2,
					term4.getSingleRelationship(ConceptManager.EdgeTypes.IS_BROADER_THAN, Direction.INCOMING)
							.getStartNode());

			// Besides the default taxonomic relationships, there should be
			// specific relationships only valid for the
			// current facet.
			RelationshipType relBroaderThenInFacet = RelationshipType
					.withName(EdgeTypes.IS_BROADER_THAN.toString() + "_fid0");
			assertEquals(term1, term2.getSingleRelationship(relBroaderThenInFacet, Direction.INCOMING).getStartNode());
			assertEquals(term1, term3.getSingleRelationship(relBroaderThenInFacet, Direction.INCOMING).getStartNode());
			assertEquals(term2, term4.getSingleRelationship(relBroaderThenInFacet, Direction.INCOMING).getStartNode());

			PathFinder<Path> pathFinder = GraphAlgoFactory.shortestPath(PathExpanders.allTypesAndDirections(), 6);
			Node facetGroupsNode = FacetManager.getFacetGroupsNode(graphDb);
			Path path = pathFinder.findSinglePath(facetGroupsNode, term1);
			assertNotNull(path);
			path = pathFinder.findSinglePath(facetGroupsNode, term2);
			assertNotNull(path);
			path = pathFinder.findSinglePath(facetGroupsNode, term3);
			assertNotNull(path);
			path = pathFinder.findSinglePath(facetGroupsNode, term4);
            assertNotNull(path);

			tx.success();
		}
	}

	@Test
	public void testInsertTermIntoMultipleFacets() throws Exception {
		// Two facets will be created. A term will be added to both, then.
		// The first facet will be sent with the term as whole facet definition.
		// The second facet will be created beforehand and then the term will
		// just be added to it.
		ImportFacet facetMap = FacetManagerTest.getTestFacetMap(1);

		List<ImportConcept> termList = new ArrayList<>();
		termList.add(
				new ImportConcept("prefname1", "desc of term1", new ConceptCoordinates("CONCEPT1", "TEST_SOURCE", SRC)));

		// -------- SEND CONCEPT WITH FACET DEFINITION ------
		ConceptManager ftm = new ConceptManager();

		Map<String, Object> termsAndFacet = new HashMap<>();
		termsAndFacet.put("facet", facetMap);
		termsAndFacet.put("concepts", termList);
		String termsAndFacetJson = ConceptsJsonSerializer.toJson(termsAndFacet);
		ftm.insertConcepts(graphDb, termsAndFacetJson);

		// Create the 2nd facet separately.
		facetMap = FacetManagerTest.getTestFacetMap(2);
		String jsonFacet = ConceptsJsonSerializer.toJson(facetMap);
		FacetManager.createFacet(graphDb, new JSONObject(jsonFacet));

		// ---------- SEND CONCEPT ONLY WITH FACET ID --------
		facetMap = new ImportFacet("fid1");
		termsAndFacet.put("facet", facetMap);
		termsAndFacetJson = ConceptsJsonSerializer.toJson(termsAndFacet);
		ftm.insertConcepts(graphDb, termsAndFacetJson);

		// ------------------ MAKE TESTS ---------------

		try (Transaction tx = graphDb.beginTx()) {
			IndexManager im = graphDb.index();
			Index<Node> termIdIndex = im.forNodes(ConceptConstants.INDEX_NAME);
			Node term = termIdIndex.get(PROP_ID, "tid0").getSingle();
			List<String> fids = Lists.newArrayList((String[]) term.getProperty(PROP_FACETS));
			assertTrue(fids.contains("fid0"));
			assertTrue(fids.contains("fid1"));
			tx.success();
		}
	}

	@Test
	public void testInsertNoFacet() throws Exception {
		// In this test we will just ordinarily insert some terms with a facet -
		// but this facet will be set as a
		// "no-facet" and thus should be moves to the appropriate section of the
		// graph.

		ImportConcepts testTerms = getTestTerms(5);
		testTerms.getFacet().setNoFacet(true);

		ConceptManager ftm = new ConceptManager();
		ftm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms));

		try (Transaction tx = graphDb.beginTx()) {
			Node facetGroupsNode = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, NodeConstants.Labels.ROOT,
					NodeConstants.PROP_NAME, FacetConstants.NAME_FACET_GROUPS);
			assertNull("The facet groups node exists although it should not.", facetGroupsNode);
			Node noFacetGroupsNode = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, NodeConstants.Labels.ROOT,
					NodeConstants.PROP_NAME, FacetConstants.NAME_NO_FACET_GROUPS);
			assertNotNull("The no facet groups node does not exists although it should.", noFacetGroupsNode);

			Node facetGroupNode = NodeUtilities.getSingleOtherNode(noFacetGroupsNode,
					FacetManager.EdgeTypes.HAS_FACET_GROUP);
			Node facetNode = NodeUtilities.getSingleOtherNode(facetGroupNode, FacetManager.EdgeTypes.HAS_FACET);
			Iterator<Relationship> rootIt = facetNode.getRelationships(ConceptManager.EdgeTypes.HAS_ROOT_CONCEPT)
					.iterator();
			int rootCount = 0;
			while (rootIt.hasNext()) {
				@SuppressWarnings("unused")
				Relationship root = rootIt.next();
				rootCount++;
			}
			assertEquals("Wrong number of roots for the facet: ", new Integer(5), new Integer(rootCount));

            tx.success();
		}
	}

	@Test
	public void testMergeTermProperties() throws Exception {
		// We will insert the same term (identified by the same original ID)
		// multiple times with additional
		// information each time. At the end, the information that can be merged
		// should be complete.
		ImportFacet facetMap = FacetManagerTest.getImportFacet();

		// ------------ INSERT 1 ---------------

		ImportConcept concept = new ImportConcept("prefname1",
				new ConceptCoordinates("CONCEPT1", "TEST_SOURCE", "ORGID", "orgSrc1"));

		ConceptManager ftm = new ConceptManager();

		Map<String, Object> termsAndFacet = new HashMap<String, Object>();
		termsAndFacet.put("facet", facetMap);
		termsAndFacet.put("concepts", Lists.newArrayList(concept));
		String termsAndFacetJson = ConceptsJsonSerializer.toJson(termsAndFacet);
		ftm.insertConcepts(graphDb, termsAndFacetJson);

		// ------------ INSERT 2 ---------------
		concept = new ImportConcept("prefname1", "description1",
				new ConceptCoordinates("CONCEPT1", "TEST_SOURCE", "ORGID", "orgSrc1"));

		termsAndFacet.put("concepts", Lists.newArrayList(concept));
		termsAndFacetJson = ConceptsJsonSerializer.toJson(termsAndFacet);
		ftm.insertConcepts(graphDb, termsAndFacetJson);

		// ------------ INSERT 3 ---------------
		concept = new ImportConcept("prefname2", Collections.singletonList("syn1"),
				new ConceptCoordinates("CONCEPT2", "TEST_SOURCE", "ORGID", "orgSrc1"));

		termsAndFacet.put("concepts", Lists.newArrayList(concept));
		termsAndFacetJson = ConceptsJsonSerializer.toJson(termsAndFacet);
		ftm.insertConcepts(graphDb, termsAndFacetJson);

		// ------------ INSERT 4 ---------------
		concept = new ImportConcept("prefname3", Collections.singletonList("syn2"), "description2",
				new ConceptCoordinates("CONCEPT3", "TEST_SOURCE", "ORGID", "orgSrc1"));

		termsAndFacet.put("concepts", Lists.newArrayList(concept));
		termsAndFacetJson = ConceptsJsonSerializer.toJson(termsAndFacet);
		ftm.insertConcepts(graphDb, termsAndFacetJson);

		// ------------ MAKE TESTS ---------------
		try (Transaction tx = graphDb.beginTx()) {
			IndexManager im = graphDb.index();
			Index<Node> termIdIndex = im.forNodes(ConceptConstants.INDEX_NAME);
			IndexHits<Node> terms = termIdIndex.query(PROP_ID, "tid*");
			assertEquals(1, terms.size());
			// We only have one term, thus tid0.
			Node term = termIdIndex.get(PROP_ID, "tid0").getSingle();

			assertEquals("prefname1", term.getProperty(PROP_PREF_NAME));
			assertEquals("ORGID", term.getProperty(PROP_ORG_ID));

			String[] descs = (String[]) term.getProperty(PROP_DESCRIPTIONS);
			assertEquals(2, descs.length);
			Arrays.sort(descs);
			assertEquals(Lists.newArrayList("description1", "description2"), Arrays.asList(descs));
			List<String> synList = Lists.newArrayList((String[]) term.getProperty(PROP_SYNONYMS));
			assertTrue(synList.contains("syn1"));
			assertTrue(synList.contains("syn2"));
			List<String> srcIdList = Lists.newArrayList((String[]) term.getProperty(PROP_SRC_IDS));
			assertTrue(srcIdList.contains("CONCEPT1"));
			assertTrue(srcIdList.contains("CONCEPT2"));
			assertTrue(srcIdList.contains("CONCEPT3"));

			tx.success();
		}
	}

	@Test
	public void testMergeTermLabels() throws Exception {
		// Check if we can insert a concept and then insert the term again but
		// with an additional label so that the new labels gets added to the
		// existing concept
		ImportConcepts testTerms = getTestTerms(1);
		ConceptManager cm = new ConceptManager();
		cm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms));

		testTerms = getTestTerms(1);
		testTerms.setImportOptions(new ImportOptions());
		testTerms.getImportOptions().merge = true;
		testTerms.getConceptsAsList().get(0).addGeneralLabel("ANOTHER_LABEL");
		// just to make am more thorough test, we change the source coordinates (but NOT
		// the original coordinates)
		testTerms.getConceptsAsList().get(0).coordinates.sourceId = "somesrcid";
		testTerms.getConceptsAsList().get(0).coordinates.source = "somesrc";
		cm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms));

		try (Transaction tx = graphDb.beginTx()) {
			// Check that there is only one concept node
			ResourceIterator<Node> conceptNodes = graphDb.findNodes(ConceptLabel.CONCEPT);
			int counter = 0;
			while (conceptNodes.hasNext()) {
				@SuppressWarnings("unused")
				Node n = (Node) conceptNodes.next();
				++counter;
			}
			assertEquals(1, counter);
			// end check only one concept node

			// now check that there also is a node with ANOTHER_LABEL
			conceptNodes = graphDb.findNodes(Label.label("ANOTHER_LABEL"));
			counter = 0;
			while (conceptNodes.hasNext()) {
				@SuppressWarnings("unused")
				Node n = (Node) conceptNodes.next();
				++counter;
			}
			assertEquals(1, counter);
            tx.success();
		}
	}

	@Test
	public void testImportConceptMultipleTimes() throws Exception {
		List<ImportConcept> terms;
		ConceptManager ftm;
		terms = new ArrayList<>();
		ImportFacet importFacet;
		ftm = new ConceptManager();
		ImportConcepts importTermAndFacet;
		terms.add(new ImportConcept("name0", new ConceptCoordinates("source0", "TEST_SOURCE", SRC)));
		terms.add(new ImportConcept("name1", new ConceptCoordinates("source1", "TEST_SOURCE", SRC),
				new ConceptCoordinates("source0", "TEST_SOURCE", SRC)));
		importFacet = FacetManagerTest.getImportFacet();
		importTermAndFacet = new ImportConcepts(terms, importFacet);
		ftm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(importTermAndFacet));
		// Insert another time.
		ftm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(importTermAndFacet));

		try (Transaction tx = graphDb.beginTx()) {
			Index<Node> index = graphDb.index().forNodes(ConceptConstants.INDEX_NAME);
			// Would throw an exception if there were multiple terms found.
			index.get(PROP_SRC_IDS, "source0").getSingle();
			index.get(PROP_SRC_IDS, "source1").getSingle();
			tx.success();

		}
	}

	@Test
	public void testImportEdgeMultipleTimes() throws Exception {
		List<ImportConcept> terms;
		ConceptManager ftm;
		terms = new ArrayList<>();
		ImportFacet importFacet;
		ftm = new ConceptManager();
		ImportConcepts importTermAndFacet;

		terms.add(new ImportConcept("name0", new ConceptCoordinates("source0", "TEST_SOURCE", SRC)));
		terms.add(new ImportConcept("name1", new ConceptCoordinates("source1", "TEST_SOURCE", SRC),
				new ConceptCoordinates("source0", "TEST_SOURCE", SRC)));
		importFacet = FacetManagerTest.getImportFacet();
		importTermAndFacet = new ImportConcepts(terms, importFacet);
		ftm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(importTermAndFacet));
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, ConceptManager.ConceptLabel.CONCEPT,
					PROP_ID, NodeIDPrefixConstants.TERM + 1);
			assertNotNull(n1);
			Node n0 = NodeUtilities.getSingleOtherNode(n1, ConceptManager.EdgeTypes.IS_BROADER_THAN);
			assertEquals(NodeIDPrefixConstants.TERM + 0, n0.getProperty(PROP_ID));
		}

		terms.clear();
		terms.add(new ImportConcept("name0", new ConceptCoordinates("source0", "TEST_SOURCE", SRC)));
		terms.add(new ImportConcept("name1", new ConceptCoordinates("source1", "TEST_SOURCE", SRC),
				new ConceptCoordinates("source0", "TEST_SOURCE", SRC)));
		importFacet = FacetManagerTest.getImportFacet();
		importTermAndFacet = new ImportConcepts(terms, importFacet);
		ftm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(importTermAndFacet));
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, ConceptManager.ConceptLabel.CONCEPT,
					PROP_ID, NodeIDPrefixConstants.TERM + 1);
			Node n0 = NodeUtilities.getSingleOtherNode(n1, ConceptManager.EdgeTypes.IS_BROADER_THAN);
			assertEquals(NodeIDPrefixConstants.TERM + 0, n0.getProperty(PROP_ID));
            tx.success();
		}

		terms.clear();
		terms.add(new ImportConcept("name1", new ConceptCoordinates("source1", "TEST_SOURCE", SRC),
				new ConceptCoordinates("source0", "TEST_SOURCE", SRC)));
		importFacet = FacetManagerTest.getImportFacet();
		importTermAndFacet = new ImportConcepts(terms, importFacet);
		ftm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(importTermAndFacet));
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, ConceptManager.ConceptLabel.CONCEPT,
					PROP_ID, NodeIDPrefixConstants.TERM + 1);
			Node n0 = NodeUtilities.getSingleOtherNode(n1, ConceptManager.EdgeTypes.IS_BROADER_THAN);
			assertEquals(NodeIDPrefixConstants.TERM + 0, n0.getProperty(PROP_ID));
            tx.success();
		}

	}

	/**
	 * "Matching facet" means that in this test, there is a facet - and the test
	 * terms belong to that facet - that has the label "USE_FOR_SUGGESTIONS".
	 * 
	 * @throws JSONException
	 * @throws AggregateConceptInsertionException 
	 */
	@Test
	public void testPushAllTermsToSetMatchingFacet() throws Exception {
		// Some value initialization...
		int numTerms = 50;
		String setName = "PENDING_FOR_SUGGESTIONS";
		String facetLabelUseForSuggestions = "USE_FOR_SUGGESTIONS";
		String termLabelDoNotUseForSuggestions = "DO_NOT_USE_FOR_SUGGESTIONS";

		ImportConcepts testTerms = getTestTerms(numTerms + 2);
		// We set for two terms the general properties
		// "do not use for suggestions", "do not use for query dictionary"
		// that should be excluded then.
		testTerms.getConceptsAsList().get(numTerms).addGeneralLabel(termLabelDoNotUseForSuggestions);
		testTerms.getConceptsAsList().get(numTerms + 1).addGeneralLabel(termLabelDoNotUseForSuggestions);
		// We do set the correct property to the test facet so that the test
		// method call to pushAllTermsToSet() may refer to this property as a
		// constraint.
		// Map<String, Object> facetMap = (Map<String, Object>)
		// testTerms.get("facet");
		ImportFacet facetMap = testTerms.getFacet();
		facetMap.setLabels(new ArrayList<>());
		facetMap.addLabel(facetLabelUseForSuggestions);
		// facetMap.put(NodeConstants.PROP_GENERAL_LABELS, new String[] {
		// facetLabelUseForSuggestions });

		String termsAndFacetBytes = ConceptsJsonSerializer.toJson(testTerms);
		ConceptManager ftm = new ConceptManager();
		ftm.insertConcepts(graphDb, termsAndFacetBytes);

		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterator<Node> it = graphDb.findNodes(Label.label(setName));
			assertFalse("No nodes with the label", it.hasNext());

			// Now push the terms into the "pending for suggestion" set.
			PushConceptsToSetCommand cmd = new PushConceptsToSetCommand(setName);
			cmd.eligibleConceptDefinition = cmd.new ConceptSelectionDefinition();
			cmd.eligibleConceptDefinition.facetLabel = facetLabelUseForSuggestions;
			cmd.excludeConceptDefinition = cmd.new ConceptSelectionDefinition();
			cmd.excludeConceptDefinition.conceptLabel = termLabelDoNotUseForSuggestions;
			ftm.pushConceptsToSet(graphDb, ConceptsJsonSerializer.toJson(cmd), -1);

			// And check whether they have been successfully added to the set.
			it = graphDb.findNodes(Label.label(setName));
			assertTrue("There should be nodes with the label now", it.hasNext());
			int termCount = 0;
			while (it.hasNext()) {
				it.next();
				termCount++;
			}
			assertEquals("All terms have been returned", numTerms, termCount);

			tx.success();
		}
		//
		// termCount = 0;
		// while (it.hasNext()) {
		// Node node = it.next();
		// assertFalse("The label should have been removed from the node",
		// node.hasLabel(TermLabel.PENDING_FOR_SUGGESTIONS));
		// termCount++;
		// }
		// assertEquals("All terms have been returned", numTerms, termCount);
	}

	/**
	 * "Matching facet" means that in this test, there is NO facet that has the
	 * property "general labels" with the value "USE_FOR_SUGGESTIONS". Thus, no
	 * terms should be pushed into the set.
	 * 
	 * @throws JSONException
	 * @throws AggregateConceptInsertionException 
	 */
	@Test
	public void testPushAllTermstoSetNoMatchingFacet() throws Exception {
		int numTerms = 10;
		// It doesn't really matter what exact value we have here as we test
		// against a facet WITHOUT that value ;-)
		String facetLabelUseForSuggestions = "USE_FOR_SUGGESTIONS";
		String setName = "PENDING_FOR_SUGGESTIONS";

		ImportConcepts testTerms = getTestTerms(numTerms);

		String termsAndFacetBytes = ConceptsJsonSerializer.toJson(testTerms);
		ConceptManager ftm = new ConceptManager();
		ftm.insertConcepts(graphDb, termsAndFacetBytes);

		// Now try to push the terms into the "pending for suggestion" set -
		// which should have no effect because there is no facet with the
		// general property to be used for suggestions.
		PushConceptsToSetCommand cmd = new PushConceptsToSetCommand(setName);
		cmd.eligibleConceptDefinition = cmd.new ConceptSelectionDefinition(FacetConstants.PROP_LABELS,
				facetLabelUseForSuggestions);
		ftm.pushConceptsToSet(graphDb, ConceptsJsonSerializer.toJson(cmd), -1);

		// And check whether they have been successfully added to the set.
		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterator<Node> it = graphDb.findNodes(Label.label(setName));
			assertFalse("There should be no nodes with the label", it.hasNext());
			tx.success();
		}

	}

	@Test
	public void testPushAllTermsToSetWithoutFacetPropertyConstraint() throws Exception {
		int numTerms = 10;

		String setName = "PENDING_FOR_SUGGESTIONS";
		ImportConcepts testTerms = getTestTerms(numTerms);

		String termsAndFacetBytes = ConceptsJsonSerializer.toJson(testTerms);
		ConceptManager ftm = new ConceptManager();
		ftm.insertConcepts(graphDb, termsAndFacetBytes);

		// Now try to push the terms into the "pending for suggestion" set
		// without any restrictions.
		PushConceptsToSetCommand cmd = new PushConceptsToSetCommand(setName);
		ftm.pushConceptsToSet(graphDb, ConceptsJsonSerializer.toJson(cmd), -1);

		// And check whether they have been successfully added to the set.
		int termCount = countNodesWithLabel(Label.label(setName));
		assertEquals("All terms have been returned", numTerms, termCount);

		// Do the same as above but set empty values for the
		// PushTermsToSetCommand. This should be equivalent to
		// "no restrictions".
		setName = "other set";
		cmd = new PushConceptsToSetCommand(setName);
		cmd.eligibleConceptDefinition = cmd.new ConceptSelectionDefinition("", "", "", "");
		ftm.pushConceptsToSet(graphDb, ConceptsJsonSerializer.toJson(cmd), -1);

		// And check whether they have been successfully added to the set.
		termCount = countNodesWithLabel(Label.label(setName));
		assertEquals("All terms have been returned", numTerms, termCount);
	}

	@Test
	public void testPushAllTermsToSetWithLimit() throws Exception {
		int numTerms = 100;

		String setName = "PENDING_FOR_SUGGESTIONS";
		ImportConcepts testTerms = getTestTerms(numTerms);

		String termsAndFacetBytes = ConceptsJsonSerializer.toJson(testTerms);
		ConceptManager ftm = new ConceptManager();
		ftm.insertConcepts(graphDb, termsAndFacetBytes);

		// Now try to push the terms into the "pending for suggestion", but only
		// a part of the terms.
		PushConceptsToSetCommand cmd = new PushConceptsToSetCommand(setName);
		ftm.pushConceptsToSet(graphDb, ConceptsJsonSerializer.toJson(cmd), 42);

		// And check that only the specified number of terms is returned.
		int termCount = countNodesWithLabel(Label.label(setName));
		assertEquals("All terms have been returned", 42, termCount);
	}

	private int countNodesWithLabel(Label label) {
		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterator<Node> it = graphDb.findNodes(label);
			int nodeCount = 0;
			while (it.hasNext()) {
				it.next();
				nodeCount++;
			}
			tx.success();
			return nodeCount;
		}
	}

	private int countRelationships(Iterable<Relationship> iterable) {
		int count = 0;
		for (@SuppressWarnings("unused")
		Relationship rel : iterable) {
			++count;
		}
		return count;
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testPopTermsFromSet() throws Exception {
		int numPoppedTerms = 17;

		String setName = "PENDING_FOR_SUGGESTIONS";
		// First, import terms using the respective test method.
		testPushAllTermsToSetMatchingFacet();

		int nodesWithLabel = countNodesWithLabel(Label.label(setName));

		ConceptManager ftm = new ConceptManager();
		RecursiveMappingRepresentation poppedTerms = (RecursiveMappingRepresentation) ftm.popConceptsFromSet(graphDb,
				setName, numPoppedTerms);
		List<Node> termList = (List<Node>) poppedTerms.getUnderlyingMap().get(ConceptManager.RET_KEY_CONCEPTS);

		assertEquals("Number of popped terms", numPoppedTerms, termList.size());

		assertEquals("The returned terms should no longer have the label", nodesWithLabel - numPoppedTerms,
				countNodesWithLabel(Label.label(setName)));
	}

	@Test
	public void testOrganizeSynonyms() throws Exception {
		// In this test, we have a term that specifies its preferred term as a
		// synonym, too. The FacetConceptManager should detect this and remove
		// the
		// synonym that equals the preferred name.
		List<ImportConcept> termList = new ArrayList<>();
		termList.add(new ImportConcept("prefname", Arrays.asList("prefname", "othersynonym"), "desc of term",
				new ConceptCoordinates("CONCEPT", "TEST_SOURCE", SRC)));
		Map<String, Object> termsAndFacet = new HashMap<String, Object>();
		termsAndFacet.put("facet", FacetManagerTest.getImportFacet());
		termsAndFacet.put("concepts", termList);

		String termsJson = ConceptsJsonSerializer.toJson(termsAndFacet);
		ConceptManager ftt = new ConceptManager();

		ftt.insertConcepts(graphDb, termsJson);

		try (Transaction tx = graphDb.beginTx()) {
			Index<Node> termIndex = graphDb.index().forNodes(ConceptConstants.INDEX_NAME);
			Node term = termIndex.get(PROP_SRC_IDS, "CONCEPT").getSingle();
			assertEquals("Preferred name", "prefname", term.getProperty(PROP_PREF_NAME));
			assertEquals("Description", Lists.newArrayList("desc of term"),
					Arrays.asList((String[]) term.getProperty(PROP_DESCRIPTIONS)));
			assertArrayEquals("Source ID", new String[] { "CONCEPT" }, (String[]) term.getProperty(PROP_SRC_IDS));
			String[] synonyms = (String[]) term.getProperty(PROP_SYNONYMS);
			assertEquals("Number of synonyms", 1, synonyms.length);
			assertEquals("Synonym", "othersynonym", synonyms[0]);
			tx.success();
		}
	}

	@Test
	public void testAdditionalRelationships() throws Exception {
		// Multiple terms will be inserted that are "equal" according to the
		// preferred names. This will be expressed via
		// additional relationships other then the default taxonomic
		// relationship. Also, we will first insert one part of the equal terms
		// and observe that "hollow" terms are
		// created on behalf of the referenced other half. After inserting
		// this other half, there should be no more "hollow" terms.
		ImportConcepts testTermsAndFacet = getTestTerms(4);
		List<ImportConcept> terms = testTermsAndFacet.getConceptsAsList();
		// Now we just create relationships so that:
		// 0 equals 1
		// 0 equals 3
		// 1 equals 2
		// 2 equals 3
		// 0 2
		// |X|
		// 1 3
		// Note that the test uses "CONCEPT<number>" for source IDs and that we
		// have to define the relationships via source
		// IDs (we don't know the ultimate concept IDs yet).
		String termSource = terms.get(0).coordinates.originalSource;
		terms.get(0).addRelationship(new ImportConceptRelationship(new ConceptCoordinates("CONCEPT" + 1, termSource,false),
				ConceptManager.EdgeTypes.HAS_SAME_NAMES.name()));
		terms.get(0).addRelationship(new ImportConceptRelationship(new ConceptCoordinates("CONCEPT" + 3, termSource,false),
				ConceptManager.EdgeTypes.HAS_SAME_NAMES.name()));
		terms.get(1).addRelationship(new ImportConceptRelationship(new ConceptCoordinates("CONCEPT" + 2, termSource, false),
				ConceptManager.EdgeTypes.HAS_SAME_NAMES.name()));
		terms.get(2).addRelationship(new ImportConceptRelationship(new ConceptCoordinates("CONCEPT" + 3, termSource, false),
				ConceptManager.EdgeTypes.HAS_SAME_NAMES.name()));

		// Now we split the terms in two lists in order to check the behavior of
		// the "hollow" label assignment.
		ImportConcepts firstTerms = new ImportConcepts(terms.subList(0, 2), testTermsAndFacet.getFacet());
		ImportConcepts secondTerms = new ImportConcepts(terms.subList(2, 4), testTermsAndFacet.getFacet());

		ConceptManager ftt = new ConceptManager();

		// Insert the first half of terms.
		ftt.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(firstTerms));
		try (Transaction tx = graphDb.beginTx()) {
            final Iterator<Node> nodesIt = Stream.concat(graphDb.findNodes(ConceptLabel.CONCEPT).stream(), graphDb.findNodes(ConceptLabel.HOLLOW).stream()).iterator();

            int nodeCount = 0;
			while (nodesIt.hasNext()) {
				Node node = nodesIt.next();
				// Only the real terms have gotten an ID.
				if (node.hasProperty(PROP_ID))
					assertFalse("Not hollow", node.hasLabel(ConceptManager.ConceptLabel.HOLLOW));
				else
					assertTrue("Node is hollow", node.hasLabel(ConceptManager.ConceptLabel.HOLLOW));
				nodeCount++;
			}
			assertEquals("Number of terms", 4, nodeCount);
            tx.success();
		}

		// Now insert the other half of terms. After this we should still have 4
		// terms, but non of them should be
		// hollow and all should have an ID, a facet and a description.
		ftt.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(secondTerms));
		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterator<Node> nodesIt = graphDb.findNodes(ConceptManager.ConceptLabel.CONCEPT);
			while (nodesIt.hasNext()) {
				Node node = nodesIt.next();
				System.out.println(NodeUtilities.getNodePropertiesAsString(node));
			}
			assertEquals(4, ftt.getNumConcepts(graphDb));
			while (nodesIt.hasNext()) {
				Node node = nodesIt.next();
				// Only the real terms have gotten an ID.
				assertFalse("Term " + NodeUtilities.getNodePropertiesAsString(node) + " is hollow but it shouldn't be",
						node.hasLabel(ConceptManager.ConceptLabel.HOLLOW));
				assertTrue("Has an ID", node.hasProperty(PROP_ID));
				assertTrue("Has a facet", node.hasProperty(PROP_FACETS));
				assertTrue("Has a description", node.hasProperty(PROP_DESCRIPTIONS));

				// Check the correctness of the "equal names" relationships. For
				// simplicity, we restrict ourselves to
				// outgoing relationships to test here.
				Iterator<Relationship> relIt = node
						.getRelationships(Direction.OUTGOING, ConceptManager.EdgeTypes.HAS_SAME_NAMES).iterator();
				String id = (String) node.getProperty(PROP_ID);
				int numRels = 0;
				if (id.endsWith("0")) {
					while (relIt.hasNext()) {
						Relationship rel = relIt.next();
						String targetId = (String) rel.getEndNode().getProperty(PROP_ID);
						assertTrue(targetId.endsWith("1") || targetId.endsWith("3"));
						numRels++;
					}
					assertEquals("Number of relationships", 2, numRels);
				} else if (id.endsWith("1")) {
					while (relIt.hasNext()) {
						Relationship rel = relIt.next();
						String targetId = (String) rel.getEndNode().getProperty(PROP_ID);
						assertTrue(targetId.endsWith("2"));
						numRels++;
					}
					assertEquals("Number of relationships", 1, numRels);
				} else if (id.endsWith("2")) {
					while (relIt.hasNext()) {
						Relationship rel = relIt.next();
						String targetId = (String) rel.getEndNode().getProperty(PROP_ID);
						assertTrue(targetId.endsWith("3"));
						numRels++;
					}
					assertEquals("Number of relationships", 1, numRels);
				}
			}
		}
	}

	@Test
	public void testAdditionalRelationships2() throws Exception {
		ImportConcepts testTermsAndFacet = getTestTerms(4);
		List<ImportConcept> terms = testTermsAndFacet.getConceptsAsList();
		String termSource = terms.get(0).coordinates.originalSource;
		ImportConceptRelationship rel1 = new ImportConceptRelationship(new ConceptCoordinates("CONCEPT" + 1, termSource, true),
				ConceptManager.EdgeTypes.HAS_SAME_NAMES.name());
		rel1.addProperty("prop1", "value1");
		rel1.addProperty("prop2", "value2");
		ImportConceptRelationship rel2 = new ImportConceptRelationship(new ConceptCoordinates("CONCEPT" + 1, termSource,true),
				ConceptManager.EdgeTypes.HAS_SAME_NAMES.name());
		rel2.addProperty("prop1", "value1");
		rel2.addProperty("prop3", "value3");
		terms.get(0).addRelationship(rel1);
		terms.get(0).addRelationship(rel2);

		ConceptManager tm = new ConceptManager();
		tm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTermsAndFacet));
		tm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTermsAndFacet));

		try (Transaction tx = graphDb.beginTx()) {
			Node term0 = graphDb.findNode(ConceptLabel.CONCEPT, PROP_ID, NodeIDPrefixConstants.TERM + 0);
			Relationship relationship = term0.getSingleRelationship(ConceptManager.EdgeTypes.HAS_SAME_NAMES,
					Direction.OUTGOING);
			assertNotNull(relationship);
			assertTrue(relationship.hasProperty("prop1"));
			assertEquals("value1", relationship.getProperty("prop1"));
			assertTrue(relationship.hasProperty("prop2"));
			assertEquals("value2", relationship.getProperty("prop2"));
			assertTrue(relationship.hasProperty("prop3"));
			assertEquals("value3", relationship.getProperty("prop3"));
		}
	}

	@Test
	public void testInsertAggregateTerm() throws Exception {
		// Here we test the case where an aggregate term is explicitly imported
		// from external data as opposed to
		// computing it within the database.
		ImportConcepts testTerms = getTestTerms(5);
		List<ImportConcept> terms = testTerms.getConceptsAsList();
		// Add an aggregate term with terms 0-3 as elements. The term on
		// position 4 will stay alone.
		List<String> aggregateElementSrcIds = Lists.newArrayList("CONCEPT" + 0, "CONCEPT" + 1, "CONCEPT" + 2, "CONCEPT" + 3);
		List<String> aggregateElementSources = Lists.newArrayList(terms.get(0).coordinates.source,
				terms.get(1).coordinates.source, terms.get(2).coordinates.source, terms.get(3).coordinates.source);
		List<ConceptCoordinates> aggregateElementCoords = new ArrayList<>();
		for (int i = 0; i < aggregateElementSrcIds.size(); i++) {
			String id = aggregateElementSrcIds.get(i);
			String source = aggregateElementSources.get(i);
			aggregateElementCoords.add(new ConceptCoordinates(id, source, true));
		}
		terms.add(new ImportConcept(aggregateElementCoords,
				Lists.newArrayList(PROP_PREF_NAME, PROP_SYNONYMS, PROP_DESCRIPTIONS)));

		ConceptManager ftm = new ConceptManager();
		ftm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms));

		assertEquals("Number of actual terms", 5, countNodesWithLabel(ConceptManager.ConceptLabel.CONCEPT));
		assertEquals("Number of aggregate terms", 1, countNodesWithLabel(ConceptManager.ConceptLabel.AGGREGATE));

		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterator<Node> aggregateIt = graphDb.findNodes(ConceptManager.ConceptLabel.AGGREGATE);
			assertTrue("There is at least one aggregate term", aggregateIt.hasNext());
			Node aggregate = aggregateIt.next();
			assertFalse("There is no second aggregate term", aggregateIt.hasNext());
			Iterable<Relationship> elementRels = aggregate.getRelationships(ConceptManager.EdgeTypes.HAS_ELEMENT);
			int numElementRels = 0;
			for (Relationship elementRel : elementRels) {
				String[] termSrcIds = (String[]) elementRel.getEndNode().getProperty(PROP_SRC_IDS);
				assertTrue("Term is one of the defined aggregate elements",
						aggregateElementSrcIds.contains(termSrcIds[0]));
				numElementRels++;
			}
			assertEquals("There are 4 elements", 4, numElementRels);

			Node term = graphDb.index().forNodes(ConceptConstants.INDEX_NAME).get(PROP_SRC_IDS, "CONCEPT" + 4).getSingle();
			assertNotNull("Term on position 4 was inserted and found", term);
			Iterable<Relationship> relationships = term.getRelationships();
			int numRels = 0;
			for (Relationship rel : relationships) {
				numRels++;
				assertTrue(rel.getType().name().equals(ConceptManager.EdgeTypes.HAS_ROOT_CONCEPT.toString()));
			}
			assertEquals("Term on position 4 has exactly one relationship (HAS_ROOT)", 1, numRels);
			// Now let's copy the term properties into the aggregate.
			RecursiveMappingRepresentation report = (RecursiveMappingRepresentation) ftm
					.copyAggregateProperties(graphDb);
			Map<String, Object> reportMap = report.getUnderlyingMap();
			assertEquals("Number of aggregates", 1, reportMap.get(ConceptManager.RET_KEY_NUM_AGGREGATES));
			assertEquals("Number of element terms", 4, reportMap.get(ConceptManager.RET_KEY_NUM_ELEMENTS));
			assertEquals("Number of copied properties", 4 * 2, reportMap.get(ConceptManager.RET_KEY_NUM_PROPERTIES));

			assertFalse("Name of the aggregate has been copied",
					StringUtils.isBlank((String) aggregate.getProperty(PROP_PREF_NAME)));
			assertEquals("Descriptions have been copied", 4,
					((String[]) aggregate.getProperty(PROP_DESCRIPTIONS)).length);
			tx.success();
		}
	}

	@Test
	public void testAddAggregateAsHierarchyNode() throws Exception {
		// Insert an aggregate and check the relationships.

		ImportConcepts testTerms = getTestTerms(2);
		ImportConcept aggregate = new ImportConcept(
				Arrays.asList(new ConceptCoordinates("CONCEPT0", "TEST_DATA", true), new ConceptCoordinates("CONCEPT1", "TEST_DATA", true)),
                Collections.singletonList(PROP_PREF_NAME));
		aggregate.coordinates = new ConceptCoordinates("testagg", "TEST_DATA", SRC);
		aggregate.aggregateIncludeInHierarchy = true;
		testTerms.getConceptsAsList().add(aggregate);
		testTerms.getConceptsAsList().get(0).parentCoordinates = Collections.singletonList(new ConceptCoordinates("testagg", "TEST_DATA", SRC));
		testTerms.getConceptsAsList().get(1).parentCoordinates = Collections.singletonList(new ConceptCoordinates("testagg", "TEST_DATA", SRC));

		ConceptManager ftm = new ConceptManager();
		ftm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms));

		try (Transaction tx = graphDb.beginTx()) {
			int aggregateCount = countNodesWithLabel(ConceptLabel.AGGREGATE);
			assertEquals(1, aggregateCount);

			Node aggregateNode = NodeUtilities.getSingleNode(graphDb.findNodes(ConceptLabel.AGGREGATE));
			assertEquals(2, countRelationships(aggregateNode.getRelationships(EdgeTypes.HAS_ELEMENT)));
			assertEquals(2, countRelationships(aggregateNode.getRelationships(EdgeTypes.IS_BROADER_THAN)));
			assertEquals(1, countRelationships(aggregateNode.getRelationships(EdgeTypes.HAS_ROOT_CONCEPT)));

			Node term0 = graphDb.findNode(ConceptLabel.CONCEPT, PROP_ID, NodeIDPrefixConstants.TERM + 0);
			Relationship broaderThan = term0.getSingleRelationship(EdgeTypes.IS_BROADER_THAN, Direction.INCOMING);
			assertEquals(aggregateNode, broaderThan.getStartNode());

			Node term1 = graphDb.findNode(ConceptLabel.CONCEPT, PROP_ID, NodeIDPrefixConstants.TERM + 1);
			broaderThan = term1.getSingleRelationship(EdgeTypes.IS_BROADER_THAN, Direction.INCOMING);
			assertEquals(aggregateNode, broaderThan.getStartNode());
		}
	}

	@Test
	public void testAddAggregateAsHierarchyNode2() throws Exception {
		// Insert aggregate with an additional label and check that it has been
		// applied.

		ImportConcepts testTerms = getTestTerms(2);
		ImportConcept aggregate = new ImportConcept(
				Arrays.asList(new ConceptCoordinates("CONCEPT0", "TEST_DATA", true), new ConceptCoordinates("CONCEPT1", "TEST_DATA", true)),
				Arrays.asList(PROP_PREF_NAME));
		aggregate.coordinates = new ConceptCoordinates("testagg", "TEST_CONCEPT", SRC);
		aggregate.aggregateIncludeInHierarchy = true;
		aggregate.generalLabels = Arrays.asList("MY_COOL_AGGREGATE_LABEL");
		testTerms.getConceptsAsList().add(aggregate);
		testTerms.getConceptsAsList().get(0).parentCoordinates = Arrays
				.asList(new ConceptCoordinates("testagg", "TEST_CONCEPT", SRC));
		testTerms.getConceptsAsList().get(1).parentCoordinates = Arrays
				.asList(new ConceptCoordinates("testagg", "TEST_CONCEPT", SRC));

		ConceptManager ftm = new ConceptManager();
		ftm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms));

		try (Transaction tx = graphDb.beginTx()) {
			int aggregateCount = countNodesWithLabel(Label.label("MY_COOL_AGGREGATE_LABEL"));
			assertEquals(1, aggregateCount);
		}
	}

	@Test
	public void testBuildAggregate() throws Exception {
		// In this test, the database is scanned for terms with the same name
		// and synonyms. If some are found, an
		// aggregate node containing these nodes as elements will be created.
		// So first of all, let's build the terms. Two will have the exactly
		// same name and synonyms, one will have the
		// same name and no synonyms and one will be completely
		// different. Another two will have same names, without any synonyms.
		List<ImportConcept> terms = new ArrayList<>();

		terms.add(new ImportConcept("name1", Lists.newArrayList("syn1", "syn2"),
				new ConceptCoordinates("source1", "TEST_SOURCE", SRC)));
		terms.add(new ImportConcept("name1", Lists.newArrayList("syn1", "syn2"),
				new ConceptCoordinates("source2", "TEST_SOURCE", SRC)));
		terms.add(new ImportConcept("name1", new ConceptCoordinates("source3", "TEST_SOURCE", SRC)));
		terms.add(new ImportConcept("name2", new ConceptCoordinates("source4", "TEST_SOURCE", SRC)));
		terms.add(new ImportConcept("name3", new ConceptCoordinates("source5", "TEST_SOURCE", SRC)));
		terms.add(new ImportConcept("name3", new ConceptCoordinates("source6", "TEST_SOURCE", SRC)));
		// Lets shuffle the terms to be simulate the situation where terms are
		// added in random order.
		Collections.shuffle(terms);
		ImportFacet importFacet = FacetManagerTest.getImportFacet();
		ImportConcepts importTermAndFacet = new ImportConcepts(terms, importFacet);
		ConceptManager ftm = new ConceptManager();
		ftm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(importTermAndFacet));
		ftm.buildAggregatesByNameAndSynonyms(graphDb, ConceptConstants.PROP_LABELS,
				"[\"DO_NOT_USE_FOR_SUGGESTIONS\",\"DO_NOT_USE_FOR_QUERY_DICTIONARY\"]");

		// Now let's copy the term properties into the aggregate.
		RecursiveMappingRepresentation report = (RecursiveMappingRepresentation) ftm.copyAggregateProperties(graphDb);
		Map<String, Object> reportMap = report.getUnderlyingMap();
		assertEquals("Number of aggregates", 2, reportMap.get(ConceptManager.RET_KEY_NUM_AGGREGATES));
		assertEquals("Number of element terms", 5, reportMap.get(ConceptManager.RET_KEY_NUM_ELEMENTS));
		assertEquals("Number of copied properties", 7, reportMap.get(ConceptManager.RET_KEY_NUM_PROPERTIES));

		// Check whether the correct terms have been bound together.
		try (Transaction tx = graphDb.beginTx()) {
			TermNameAndSynonymComparator nameAndSynonymComparator = new TermNameAndSynonymComparator();
			ResourceIterable<Node> generalAggregates = () -> graphDb.findNodes(ConceptManager.ConceptLabel.AGGREGATE);
			for (Node aggregate : generalAggregates) {
				Iterable<Relationship> elementRels = aggregate.getRelationships(ConceptManager.EdgeTypes.HAS_ELEMENT);
				// Count relationships and store elements for comparison whether
				// they are actually equal named.
				int numRels = 0;
				List<Node> elements = new ArrayList<>();
				for (Relationship rel : elementRels) {
					Node term = rel.getEndNode();
					elements.add(term);
					List<String> generalLabels = Lists
							.newArrayList((String[]) term.getProperty(ConceptConstants.PROP_LABELS));
					assertTrue("Term is not used for suggestions",
							generalLabels.contains("DO_NOT_USE_FOR_SUGGESTIONS"));
					assertTrue("Term is not used for query dict",
							generalLabels.contains("DO_NOT_USE_FOR_QUERY_DICTIONARY"));
					numRels++;
				}
				// Are the elements equal in their names?
				Node equalNameElement = elements.get(0);
				for (int i = 1; i < elements.size(); i++) {
					Node term = elements.get(i);
					assertEquals("Name and synonyms are equal", 0,
							nameAndSynonymComparator.compare(equalNameElement, term));
				}
				// Check whether the aggregates have the correct number of
				// elements.
				String prefName = (String) aggregate.getProperty(ConceptConstants.PROP_PREF_NAME);
				if (prefName.equals("name1")) {
					assertEquals("Number of elements", 3, numRels);
				} else if (prefName.equals("name3")) {
					assertEquals("Number of elements", 2, numRels);
				} else {
					throw new IllegalStateException("This node should not be an aggregate.");
				}
			}
			tx.success();
		}
	}

	@Test
	public void testBuildMappingAggregate() throws Exception {
		ImportConcepts testTerms;
		ImportConcepts testTerms1;
		ImportConcepts testTerms2;
		ImportConcepts testTerms3;
		ImportConcepts testTerms4;
		ConceptManager ftm = new ConceptManager();
		// Create terms in DIFFERENT facets we will then map to each other
		testTerms = getTestTerms(1, 0);
		testTerms1 = getTestTerms(1, 1);
		testTerms2 = getTestTerms(1, 2);
		testTerms3 = getTestTerms(1, 3);
		testTerms4 = getTestTerms(1, 4);
		testTerms.getFacet().setName("facet0");
		testTerms1.getFacet().setName("facet1");
		testTerms2.getFacet().setName("facet2");
		testTerms3.getFacet().setName("facet3");
		testTerms4.getFacet().setName("facet4");
		testTerms1.getConceptsAsList().get(0).parentCoordinates = Arrays
				.asList(new ConceptCoordinates("CONCEPT0", "TEST_DATA", SRC));
		testTerms2.getConceptsAsList().get(0).parentCoordinates = Arrays
				.asList(new ConceptCoordinates("CONCEPT1", "TEST_DATA", SRC));
		testTerms3.getConceptsAsList().get(0).parentCoordinates = Arrays
				.asList(new ConceptCoordinates("CONCEPT2", "TEST_DATA", SRC));
		testTerms4.getConceptsAsList().get(0).parentCoordinates = Arrays
				.asList(new ConceptCoordinates("CONCEPT3", "TEST_DATA", SRC));
		ftm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms));
		ftm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms1));
		ftm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms2));
		ftm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms3));
		ftm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms4));

		List<ImportMapping> mappings = Lists.newArrayList(new ImportMapping("CONCEPT0", "CONCEPT1", "loom"));

		int numInsertedRels = ftm.insertMappings(graphDb, ConceptsJsonSerializer.toJson(mappings));
		assertEquals(1, numInsertedRels);

		ConceptAggregateBuilder.buildAggregatesForMappings(graphDb, Sets.newHashSet("loom"), ConceptLabel.CONCEPT,
				ConceptLabel.AGGREGATE);

		try (Transaction tx = graphDb.beginTx()) {
			// first check if everything is alright, we expect on aggregate with
			// two elements
			Node aggregate = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, ConceptLabel.AGGREGATE, PROP_ID,
					NodeIDPrefixConstants.AGGREGATE_TERM + 0);
			assertNotNull("Aggregate node with ID " + NodeIDPrefixConstants.AGGREGATE_TERM + "0 could not be found",
					aggregate);
			Iterable<Relationship> relationships = aggregate.getRelationships();
			int relCount = 0;
			for (Relationship rel : relationships) {
				assertEquals(EdgeTypes.HAS_ELEMENT.name(), rel.getType().name());
				++relCount;
			}
			assertEquals(2, relCount);

			// now: get the "children" of the aggregate (should be the elements)
			RecursiveMappingRepresentation response = (RecursiveMappingRepresentation) ftm.getChildrenOfConcepts(
					graphDb, "[" + NodeIDPrefixConstants.AGGREGATE_TERM + "0]", ConceptLabel.AGGREGATE.name());
			@SuppressWarnings("unchecked")
			Map<String, Object> relAndChildMap = (Map<String, Object>) response.getUnderlyingMap()
					.get(NodeIDPrefixConstants.AGGREGATE_TERM + 0);
			assertEquals(2, ((Map<?, ?>) relAndChildMap.get(ConceptManager.RET_KEY_RELTYPES)).size());
			assertEquals(2, ((Set<?>) relAndChildMap.get(ConceptManager.RET_KEY_CHILDREN)).size());
		}
	}

	@Test
	public void testHollowParent() throws Exception {
		// In this test, we will add a node with a single parent. However, we
		// won't add the parent itself, thus creating
		// a "hollow" parent node.
		// In a second step, we will add the former hollow term explicitly
		// together with another parent. We should see
		// how there are then no hollow terms left and the new
		// "grandparent" will become the root.
		ImportFacet importFacet = FacetManagerTest.getImportFacet();
		List<ImportConcept> terms = new ArrayList<>();
		terms.add(new ImportConcept("prefname1", Lists.newArrayList("syn1"), "desc1",
				new ConceptCoordinates("srcid1", "TEST_SOURCE", SRC),
				new ConceptCoordinates("parentid1", "TEST_SOURCE", SRC)));
		ImportConcepts termAndFacet = new ImportConcepts(terms, importFacet);
		// Allow hollow parents:
		termAndFacet.setImportOptions(new ImportOptions());
		termAndFacet.getImportOptions().doNotCreateHollowParents = false;
		ConceptManager ftm = new ConceptManager();
		RecursiveMappingRepresentation report = (RecursiveMappingRepresentation) ftm.insertConcepts(graphDb,
				ConceptsJsonSerializer.toJson(termAndFacet));
		Map<String, Object> reportMap = report.getUnderlyingMap();
		assertEquals("Number of inserted terms", 2, reportMap.get(ConceptManager.RET_KEY_NUM_CREATED_CONCEPTS));
		// we expect relations for the root term, broader than and broader than fid0
		assertEquals("Number of inserted relationships", 3, reportMap.get(ConceptManager.RET_KEY_NUM_CREATED_RELS));

		try (Transaction tx = graphDb.beginTx()) {
			Iterable<Node> allNodes = () -> graphDb.findNodes(ConceptManager.ConceptLabel.CONCEPT);
			int numNodes = 0;
			int numHollow = 0;
			int numRoots = 0;
			for (Node n : allNodes) {
				numNodes++;
				Iterable<Relationship> rels = n.getRelationships();
				int numRootTermRels = 0;
				int numBroaderRels = 0;
				for (Relationship rel : rels) {
					if (rel.getType().name().equals(ConceptManager.EdgeTypes.HAS_ROOT_CONCEPT.name())) {
						numRootTermRels++;
						numRoots++;
					} else if (rel.getType().name().equals(ConceptManager.EdgeTypes.IS_BROADER_THAN.name())) {
						numBroaderRels++;
					}
				}
				if (n.hasLabel(ConceptManager.ConceptLabel.HOLLOW)) {
					numHollow++;
					assertEquals("Hollow parent is the root term", 1, numRootTermRels);
					assertEquals("Hollow parent has one child", 1, numBroaderRels);
				} else {
					assertEquals("Non-hollow term is no facet root", 0, numRootTermRels);
					assertEquals("Non-hollow term has a parent", 1, numBroaderRels);
				}
			}
			assertEquals("Correct number of nodes", 2, numNodes);
			assertEquals("One node is hollow", 1, numHollow);
			assertEquals("There is one root", 1, numRoots);
			tx.success();
		}

		// Now we add the term that has been hollow before together with a
		// parent of its own. The new parent should
		// become the facet root, the former hollow term shouldn't be
		// hollow anymore.
		terms.clear();
		terms.add(new ImportConcept("prefname2", Lists.newArrayList("syn2"), "desc2",
				new ConceptCoordinates("parentid1", "TEST_SOURCE", SRC),
				new ConceptCoordinates("parentid2", "TEST_SOURCE", SRC)));
		terms.add(new ImportConcept("prefname3", new ConceptCoordinates("parentid2", "TEST_SOURCE", SRC)));
		// We need to replace the facet definition by the ID of the already
		// created facet because a new facet will be
		// created otherwise.
		termAndFacet.setFacet(new ImportFacet("fid0"));

		ftm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(termAndFacet));

		try (Transaction tx = graphDb.beginTx()) {
			Iterable<Node> allNodes = () -> graphDb.findNodes(ConceptManager.ConceptLabel.CONCEPT);
			int numNodes = 0;
			int numHollow = 0;
			int numRoots = 0;
			for (Node n : allNodes) {
				numNodes++;
				Iterable<Relationship> rels = n.getRelationships();
				int numRootTermRels = 0;
				int numBroaderRels = 0;
				for (Relationship rel : rels) {
					if (rel.getType().name().equals(ConceptManager.EdgeTypes.HAS_ROOT_CONCEPT.name())) {
						numRootTermRels++;
						numRoots++;
					} else if (rel.getType().name().equals(ConceptManager.EdgeTypes.IS_BROADER_THAN.name())) {
						numBroaderRels++;
					}
				}
				if (n.hasLabel(ConceptManager.ConceptLabel.HOLLOW))
					numHollow++;
				if (n.getProperty(PROP_PREF_NAME).equals("prefname1")) {
					assertEquals("Number of facet root relations", 0, numRootTermRels);
					assertEquals("Term has a parent", 1, numBroaderRels);
				} else if (n.getProperty(PROP_PREF_NAME).equals("prefname2")) {
					assertEquals("Number of facet root relations", 0, numRootTermRels);
					assertEquals("Number of incident BROADER THAN relationships", 2, numBroaderRels);
				} else {
					assertEquals("Term is the facet root", 1, numRootTermRels);
					assertEquals("Number of incident BROADER THAN relationships", 1, numBroaderRels);
				}
			}
			assertEquals("Correct number of nodes", 3, numNodes);
			assertEquals("No node is hollow", 0, numHollow);
			assertEquals("There is one root", 1, numRoots);
			tx.success();
		}

		// No make sure that when a hollow parent is referenced twice from the
		// same data, there is only one node
		// created.
		terms.clear();
		terms.add(new ImportConcept("prefname2", Lists.newArrayList("syn2"), "desc2",
				new ConceptCoordinates("parentid1", "TEST_SOURCE", SRC),
				new ConceptCoordinates("parentid42", "TEST_SOURCE", SRC)));
		terms.add(new ImportConcept("prefname5", Lists.newArrayList("syn2"), "desc2",
				new ConceptCoordinates("parentid8", "TEST_SOURCE", SRC),
				new ConceptCoordinates("parentid42", "TEST_SOURCE", SRC)));
		termAndFacet.setFacet(new ImportFacet("fid0"));

		ftm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(termAndFacet));

		try (Transaction tx = graphDb.beginTx()) {
			Index<Node> index = graphDb.index().forNodes(ConceptConstants.INDEX_NAME);
			// Would throw an exception if there were multiple terms found.
			Node hollowParent = index.get(PROP_SRC_IDS, "parentid42").getSingle();
			assertNotNull(hollowParent);
			assertTrue(hollowParent.hasLabel(ConceptManager.ConceptLabel.HOLLOW));
		}
	}

	@Test
	public void testNonFacetGroupCommand() throws Exception {
		// We will test whether it works correctly to sort terms into the
		// "non-facet" facet group according to a
		// particular criterium, e.g. "no parent".
		// Terms 0 and 1 will get a parent and thus should be treated normally.
		// Terms 2 and 3 don't get a parent and should become "no-facet" terms.
		ImportConcepts testTerms = getTestTerms(4);
		ImportOptions options = new ImportOptions();
		testTerms.setImportOptions(options);
		AddToNonFacetGroupCommand cmd = new AddToNonFacetGroupCommand();
		cmd.addParentCriterium(AddToNonFacetGroupCommand.ParentCriterium.NO_PARENT);
		options.noFacetCmd = cmd;
		List<ImportConcept> terms = testTerms.getConceptsAsList();
		// Here, we just give any parent so that the non-facet-group-command
		// does not trigger.
		List<ConceptCoordinates> parentSrcIds = Lists
				.newArrayList(new ConceptCoordinates("nonExistingParent", "TEST_DATA", SRC));
		terms.get(0).parentCoordinates = parentSrcIds;
		terms.get(1).parentCoordinates = parentSrcIds;
		options.cutParents = Arrays.asList("nonExistingParent");
		// We activate this so we can test parent cutting appropriately. If it
		// would be off, the child terms of
		// non-existing parents get to be the facet roots no matter what.
		options.doNotCreateHollowParents = true;

		ConceptManager ftm = new ConceptManager();
		ftm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms));

		try (Transaction tx = graphDb.beginTx()) {
			Node noFacetGroups = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, NodeConstants.Labels.ROOT,
					PROP_NAME, NAME_NO_FACET_GROUPS);
			assertNotNull("No-Facet group not found.", noFacetGroups);
			Iterator<Relationship> it = noFacetGroups.getRelationships(FacetManager.EdgeTypes.HAS_FACET_GROUP)
					.iterator();
			assertTrue("There is no no-facet group.", it.hasNext());
			Node noFacetGroup = it.next().getEndNode();
			assertFalse("There is a second no-facet group.", it.hasNext());
			Node noFacet = NodeUtilities.getSingleOtherNode(noFacetGroup, FacetManager.EdgeTypes.HAS_FACET);
			Iterator<Relationship> termRelIt = noFacet.getRelationships(ConceptManager.EdgeTypes.HAS_ROOT_CONCEPT)
					.iterator();
			assertTrue("There is no no-facet term.", termRelIt.hasNext());
			int i = 0;
			while (termRelIt.hasNext()) {
				Node noFacetTerm = termRelIt.next().getEndNode();
				String termName = (String) noFacetTerm.getProperty(PROP_PREF_NAME);
				// It should be the third of fourth of our test terms.
				assertTrue("Term name was " + termName, termName.equals("prefname2") || termName.equals("prefname3"));
				i++;
			}
			assertEquals("Number of no-facet terms.", 2, i);

			// Assert that the "not existing parent" of term 0 and 1 has been
			// cut, making terms 0 and1 the root terms of
			// the facet, although hollow terms are allowed.
			Node facet = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, FacetManager.FacetLabel.FACET, PROP_ID,
					"fid0");
			Iterator<Relationship> facetTerms = facet.getRelationships(ConceptManager.EdgeTypes.HAS_ROOT_CONCEPT)
					.iterator();
			while (facetTerms.hasNext()) {
				Node facetTerm = facetTerms.next().getEndNode();
				String termName = (String) facetTerm.getProperty(PROP_PREF_NAME);
				assertTrue("Term name was " + termName, termName.equals("prefname0") || termName.equals("prefname1"));

			}
			tx.success();
		}

	}

	@SuppressWarnings("unchecked")
	@Test
	public void testGetPathsFromFacetroots() throws Exception {
		// In this test we want to see that we get back all paths at once for
		// multiple nodes. In the following, source1
		// and source2 are the facet roots, all other nodes are direct or
		// indirect descendants of them.
		List<ImportConcept> terms = new ArrayList<>();
		ConceptCoordinates coord1 = new ConceptCoordinates("source1", "TEST_SOURCE", SRC);
		ConceptCoordinates coord2 = new ConceptCoordinates("source2", "TEST_SOURCE", SRC);
		ConceptCoordinates coord3 = new ConceptCoordinates("source3", "TEST_SOURCE", SRC);
		ConceptCoordinates coord4 = new ConceptCoordinates("source4", "TEST_SOURCE", SRC);
		ConceptCoordinates coord5 = new ConceptCoordinates("source5", "TEST_SOURCE", SRC);
		ConceptCoordinates coord6 = new ConceptCoordinates("source6", "TEST_SOURCE", SRC);
		terms.add(new ImportConcept("name1", coord1));
		terms.add(new ImportConcept("name2", coord2));
		terms.add(new ImportConcept("name3", coord3, Lists.newArrayList(coord1, coord2)));
		terms.add(new ImportConcept("name4", coord4, coord3));
		terms.add(new ImportConcept("name5", coord5, coord3));
		terms.add(new ImportConcept("name6", coord6, coord5));
		ImportFacet importFacet = FacetManagerTest.getImportFacet();
		ImportConcepts importTermAndFacet = new ImportConcepts(terms, importFacet);
		ConceptManager ftm = new ConceptManager();
		ftm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(importTermAndFacet));

		RecursiveMappingRepresentation pathsFromFacetroots;
		List<String[]> paths;

		pathsFromFacetroots = (RecursiveMappingRepresentation) ftm.getPathsFromFacetroots(graphDb,
				ConceptsJsonSerializer.toJson(Lists.newArrayList("source1", "source3")), ConceptConstants.PROP_SRC_IDS,
				false, "");
		paths = (List<String[]>) pathsFromFacetroots.getUnderlyingMap().get(ConceptManager.RET_KEY_PATHS);
		for (String[] path : paths) {
			// We expect one path with a single node, i.e. source1 and two paths
			// with two nodes respectively, [source1,
			// source3] and [source2, source3]
			// We must compare to node ids. Remember that node IDs are number
			// sequentially in order of appearance in the
			// input data.
			if (path.length == 1)
				assertEquals(NodeIDPrefixConstants.TERM + 0, path[0]);
			else {
                assertEquals(2, path.length);
				// First element is one of the roots
				assertTrue(path[0].equals(NodeIDPrefixConstants.TERM + 0)
						|| path[0].equals(NodeIDPrefixConstants.TERM + 1));
				// Second element is source3
				assertEquals(NodeIDPrefixConstants.TERM + 2, path[1]);
			}
		}
		assertEquals("Wrong number of paths", 3, paths.size());

		pathsFromFacetroots = (RecursiveMappingRepresentation) ftm.getPathsFromFacetroots(graphDb,
				ConceptsJsonSerializer.toJson(Lists.newArrayList("source4", "source6")), ConceptConstants.PROP_SRC_IDS,
				false, "");
		paths = (List<String[]>) pathsFromFacetroots.getUnderlyingMap().get(ConceptManager.RET_KEY_PATHS);
		String[] expectedPath1 = new String[] { NodeIDPrefixConstants.TERM + 0, NodeIDPrefixConstants.TERM + 2,
				NodeIDPrefixConstants.TERM + 3 };
		String[] expectedPath2 = new String[] { NodeIDPrefixConstants.TERM + 1, NodeIDPrefixConstants.TERM + 2,
				NodeIDPrefixConstants.TERM + 3 };
		String[] expectedPath3 = new String[] { NodeIDPrefixConstants.TERM + 0, NodeIDPrefixConstants.TERM + 2,
				NodeIDPrefixConstants.TERM + 4, NodeIDPrefixConstants.TERM + 5 };
		String[] expectedPath4 = new String[] { NodeIDPrefixConstants.TERM + 1, NodeIDPrefixConstants.TERM + 2,
				NodeIDPrefixConstants.TERM + 4, NodeIDPrefixConstants.TERM + 5 };
		for (String[] path : paths) {
			// We expect paths from source4 and source 6 up to the roots, i.e.
			// [source1, source3, source4]
			// [source2, source3, source4]
			// [source1, source3, source5, source6]
			// [source2, source3, source5, source6]
			// and as term IDs:
			// [tid0, tid2, tid3]
			// [tid1, tid2, tid3]
			// [tid0, tid2, tid4, tid5]
			// [tid1, tid2, tid4, tid5]
			assertTrue(Arrays.equals(expectedPath1, path) || Arrays.equals(expectedPath2, path)
					|| Arrays.equals(expectedPath3, path) || Arrays.equals(expectedPath4, path));
		}
		assertEquals("Wrong number of paths", 4, paths.size());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testGetPathsFromFacetrootsInSpecificFacet() throws Exception {
		// In this test we want to see that we get back the right path when only
		// counting in nodes from a specific facet
		// in path finding.
		// The following is a simple chain.
		List<ImportConcept> terms = new ArrayList<>();
		ConceptCoordinates coord1 = new ConceptCoordinates("source1", "TEST_SOURCE", SRC);
		ConceptCoordinates coord2 = new ConceptCoordinates("source2", "TEST_SOURCE", SRC);
		ConceptCoordinates coord3 = new ConceptCoordinates("source3", "TEST_SOURCE", SRC);
		ConceptCoordinates coord4 = new ConceptCoordinates("source4", "TEST_SOURCE", SRC);
		ConceptCoordinates coord5 = new ConceptCoordinates("source5", "TEST_SOURCE", SRC);
		ConceptCoordinates coord6 = new ConceptCoordinates("source6", "TEST_SOURCE", SRC);
		terms.add(new ImportConcept("name1", coord1));
		terms.add(new ImportConcept("name2", coord2, coord1));
		terms.add(new ImportConcept("name3", coord3, coord2));
		terms.add(new ImportConcept("name4", coord4, coord3));
		terms.add(new ImportConcept("name5", coord5, coord4));
		for (ImportConcept term : terms)
			term.coordinates.source = "TEST_SOURCE";
		ImportFacet importFacet = FacetManagerTest.getImportFacet();
		ImportConcepts importTermAndFacet = new ImportConcepts(terms, importFacet);
		ConceptManager ftm = new ConceptManager();
		ftm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(importTermAndFacet));

		// Now we insert two terms in another facet that would open another path
		// from source5 to source1 if we would
		// follow the general IS_BROADER_THAN relationship instead of the
		// facet-specific relationship. Note that source5
		// is added a second time which will create the relationship to source
		// 6, thus:
		// source5 --> source6 --> source1 and that similarly source1 was added
		// as a root term for the new facet.
		terms = new ArrayList<>();
		terms.add(new ImportConcept("name1", coord1));
		terms.add(new ImportConcept("name6", coord6, coord1));
		terms.add(new ImportConcept("name5", coord5, coord6));
		for (ImportConcept term : terms)
			term.coordinates.source = "TEST_SOURCE";
		importFacet = FacetManagerTest.getImportFacet();
		importFacet.setName("otherfacet");
		importTermAndFacet = new ImportConcepts(terms, importFacet);
		ftm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(importTermAndFacet));

		RecursiveMappingRepresentation pathsFromFacetroots;
		List<String[]> paths;

		pathsFromFacetroots = (RecursiveMappingRepresentation) ftm.getPathsFromFacetroots(graphDb,
				ConceptsJsonSerializer.toJson(Lists.newArrayList("source5")), ConceptConstants.PROP_SRC_IDS, false, "");
		paths = (List<String[]>) pathsFromFacetroots.getUnderlyingMap().get(ConceptManager.RET_KEY_PATHS);
		for (String[] path : paths) {
			// We expect two paths, both chains that have been defined by the
			// two inserts above:
			// [tid0, tid1, tid2, tid3, tid4]
			// [tid0, tid5, tid4]
			assertTrue(5 == path.length || 3 == path.length);
		}
		assertEquals("Wrong number of paths", 2, paths.size());

		pathsFromFacetroots = (RecursiveMappingRepresentation) ftm.getPathsFromFacetroots(graphDb,
				ConceptsJsonSerializer.toJson(Lists.newArrayList("source5")), ConceptConstants.PROP_SRC_IDS, false,
				"fid0");
		paths = (List<String[]>) pathsFromFacetroots.getUnderlyingMap().get(ConceptManager.RET_KEY_PATHS);
		assertEquals("Wrong number of paths", 1, paths.size());
		assertArrayEquals(new String[] { "tid0", "tid1", "tid2", "tid3", "tid4" }, paths.get(0));

		pathsFromFacetroots = (RecursiveMappingRepresentation) ftm.getPathsFromFacetroots(graphDb,
				ConceptsJsonSerializer.toJson(Lists.newArrayList("source5")), ConceptConstants.PROP_SRC_IDS, false,
				"fid1");
		paths = (List<String[]>) pathsFromFacetroots.getUnderlyingMap().get(ConceptManager.RET_KEY_PATHS);
		assertEquals("Wrong number of paths", 1, paths.size());
		assertArrayEquals(new String[] { "tid0", "tid5", "tid4" }, paths.get(0));

	}

	@Test
	public void testUpdateChildrenInformation() throws Exception {
		// In this test we check whether the array property of in which facet a
		// term has children is computed correctly.
		// For this, we do two concept imports to create two facets. We will have
		// four terms where the latter three are
		// children of the first.
		// Thus, this first term should have children in both facets.
		ImportConcepts testTerms;
		ConceptManager ftm = new ConceptManager();
		testTerms = getTestTerms(2);
		testTerms.getConceptsAsList().get(1).parentCoordinates = Arrays
				.asList(new ConceptCoordinates("CONCEPT0", "TEST_DATA", SRC));
		ftm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms));

		// Insert terms for the second facet. We have to adjust some values here
		// to actually get four terms and not just
		// two because the duplicate source IDs would be detected.
		testTerms = getTestTerms(2);
		testTerms.getFacet().setName("secondfacet");
		testTerms.getConceptsAsList().get(0).coordinates.sourceId = "CONCEPT2";
		testTerms.getConceptsAsList().get(0).coordinates.originalId = "CONCEPT2";
		testTerms.getConceptsAsList().get(1).coordinates.sourceId = "CONCEPT3";
		testTerms.getConceptsAsList().get(1).coordinates.originalId = "CONCEPT3";
		testTerms.getConceptsAsList().get(0).parentCoordinates = Arrays
				.asList(new ConceptCoordinates("CONCEPT0", "TEST_DATA", SRC));
		testTerms.getConceptsAsList().get(1).parentCoordinates = Arrays
				.asList(new ConceptCoordinates("CONCEPT0", "TEST_DATA", SRC));
		ftm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms));

		ftm.updateChildrenInformation(graphDb);

		try (Transaction tx = graphDb.beginTx()) {
			Node rootNode = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, ConceptLabel.CONCEPT, PROP_ID,
					NodeIDPrefixConstants.TERM + 0);
			List<String> facetsWithChildren = Lists
					.newArrayList((String[]) rootNode.getProperty(ConceptConstants.PROP_CHILDREN_IN_FACETS));
			assertTrue(facetsWithChildren.contains(NodeIDPrefixConstants.FACET + 0));
			assertTrue(facetsWithChildren.contains(NodeIDPrefixConstants.FACET + 1));
			assertEquals(new Integer(2), new Integer(facetsWithChildren.size()));
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testGetTermChildren() throws Exception {
		ImportConcepts testTerms;
		ConceptManager ftm = new ConceptManager();
		testTerms = getTestTerms(5);
		testTerms.getConceptsAsList().get(1).parentCoordinates = Arrays
				.asList(new ConceptCoordinates("CONCEPT0", "TEST_DATA", SRC));
		testTerms.getConceptsAsList().get(2).parentCoordinates = Arrays
				.asList(new ConceptCoordinates("CONCEPT0", "TEST_DATA", SRC));
		testTerms.getConceptsAsList().get(3).parentCoordinates = Arrays
				.asList(new ConceptCoordinates("CONCEPT0", "TEST_DATA", SRC));
		testTerms.getConceptsAsList().get(4).parentCoordinates = Arrays
				.asList(new ConceptCoordinates("CONCEPT3", "TEST_DATA", SRC));
		ftm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms));

		try (Transaction tx = graphDb.beginTx()) {
			// Get the children of a single node.
			RecursiveMappingRepresentation childrenRepr = (RecursiveMappingRepresentation) ftm
					.getChildrenOfConcepts(graphDb, "[\"tid0\"]]", null);
			Map<String, Object> childMap = childrenRepr.getUnderlyingMap();
			// We asked for only one node, thus there should be one result
			assertEquals(new Integer(1), new Integer(childMap.size()));
			Map<String, Object> term0Children = (Map<String, Object>) childMap.get(NodeIDPrefixConstants.TERM + 0);
			// This result divides into two elements, namely the relationship
			// type mapping and the nodes themselves.
			assertEquals(new Integer(2), new Integer(term0Children.size()));
			Map<String, Object> reltypeMap = (Map<String, Object>) term0Children.get(ConceptManager.RET_KEY_RELTYPES);
			assertEquals(new Integer(3), new Integer(reltypeMap.size()));
			assertTrue(reltypeMap.keySet().contains(NodeIDPrefixConstants.TERM + 1));
			assertTrue(reltypeMap.keySet().contains(NodeIDPrefixConstants.TERM + 2));
			assertTrue(reltypeMap.keySet().contains(NodeIDPrefixConstants.TERM + 3));
			Set<Node> children = (Set<Node>) term0Children.get(ConceptManager.RET_KEY_CHILDREN);
			assertEquals(new Integer(3), new Integer(children.size()));
			Set<String> childIds = new HashSet<>();
			for (Node child : children)
				childIds.add((String) child.getProperty(PROP_ID));
			assertTrue(childIds.contains(NodeIDPrefixConstants.TERM + 1));
			assertTrue(childIds.contains(NodeIDPrefixConstants.TERM + 2));
			assertTrue(childIds.contains(NodeIDPrefixConstants.TERM + 3));

			// We get the children of three nodes, where two of queried nodes
			// are the same. This shouldn't change the
			// result, i.e. there should be two elements in the result map.
			childrenRepr = (RecursiveMappingRepresentation) ftm.getChildrenOfConcepts(graphDb,
					"[\"tid0\",\"tid3\",\"tid3\"]]", null);
			childMap = childrenRepr.getUnderlyingMap();
			// We asked for three nodes' children, however two were equal thus
			// there should be two elements.
			assertEquals(new Integer(2), new Integer(childMap.size()));

		}
	}

	@Test
	public void testAddMappings() throws Exception {
		ImportConcepts testTerms;
		ImportConcepts testTerms1;
		ImportConcepts testTerms2;
		ImportConcepts testTerms3;
		ImportConcepts testTerms4;
		ConceptManager ftm = new ConceptManager();
		// Create terms in DIFFERENT facets we will then map to each other
		testTerms = getTestTerms(1, 0);
		testTerms1 = getTestTerms(1, 1);
		testTerms2 = getTestTerms(1, 2);
		testTerms3 = getTestTerms(1, 3);
		testTerms4 = getTestTerms(1, 4);
		testTerms.getFacet().setName("facet0");
		testTerms1.getFacet().setName("facet1");
		testTerms2.getFacet().setName("facet2");
		testTerms3.getFacet().setName("facet3");
		testTerms4.getFacet().setName("facet4");
		testTerms1.getConceptsAsList().get(0).parentCoordinates = Arrays
				.asList(new ConceptCoordinates("CONCEPT0", "TEST_DATA", SRC));
		testTerms2.getConceptsAsList().get(0).parentCoordinates = Arrays
				.asList(new ConceptCoordinates("CONCEPT1", "TEST_DATA", SRC));
		testTerms3.getConceptsAsList().get(0).parentCoordinates = Arrays
				.asList(new ConceptCoordinates("CONCEPT2", "TEST_DATA", SRC));
		testTerms4.getConceptsAsList().get(0).parentCoordinates = Arrays
				.asList(new ConceptCoordinates("CONCEPT3", "TEST_DATA", SRC));
		ftm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms));
		ftm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms1));
		ftm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms2));
		ftm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms3));
		ftm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms4));

		List<ImportMapping> mappings = Lists.newArrayList(new ImportMapping("CONCEPT0", "CONCEPT1", "loom"),
				new ImportMapping("CONCEPT3", "CONCEPT2", "same_uris"));

		int numInsertedRels = ftm.insertMappings(graphDb, ConceptsJsonSerializer.toJson(mappings));
		assertEquals(2, numInsertedRels);

		try (Transaction tx = graphDb.beginTx()) {
			Node term0 = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, ConceptManager.ConceptLabel.CONCEPT,
					PROP_ID, NodeIDPrefixConstants.TERM + 0);
			Iterable<Relationship> relationships = term0.getRelationships(ConceptManager.EdgeTypes.IS_MAPPED_TO);
			int relCounter = 0;
			for (Relationship rel : relationships) {
				Node otherNode = rel.getOtherNode(term0);
				assertEquals(NodeIDPrefixConstants.TERM + 1, otherNode.getProperty(PROP_ID));
				relCounter++;
			}
			assertEquals(1, relCounter);

			Node term1 = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, ConceptManager.ConceptLabel.CONCEPT,
					PROP_ID, NodeIDPrefixConstants.TERM + 1);
			relationships = term1.getRelationships(ConceptManager.EdgeTypes.IS_MAPPED_TO);
			relCounter = 0;
			for (Relationship rel : relationships) {
				Node otherNode = rel.getOtherNode(term1);
				assertEquals(NodeIDPrefixConstants.TERM + 0, otherNode.getProperty(PROP_ID));
				relCounter++;
			}
			assertEquals(1, relCounter);
            tx.success();
		}

		// Check that there are no duplicates created.
		numInsertedRels = ftm.insertMappings(graphDb, ConceptsJsonSerializer.toJson(mappings));
		assertEquals(0, numInsertedRels);

		// Even if we add a mapping with a new type between two classes that
		// already are mapped, there shouldn't be
		// created a new relationship.
		// We add the exactly same mapping twice, again to assure that
		// duplicates are avoided.
		// We also add a mapping where one term does not exist.
		mappings = Lists.newArrayList(new ImportMapping("CONCEPT0", "CONCEPT1", "new_type"),
				new ImportMapping("CONCEPT0", "CONCEPT1", "new_type"),
				new ImportMapping("CONCEPT_DOES_NOT_EXIST", "CONCEPT1", "new_type"));
		numInsertedRels = ftm.insertMappings(graphDb, ConceptsJsonSerializer.toJson(mappings));
		assertEquals(0, numInsertedRels);
		// But the relationship now should know both mapping types.
		try (Transaction tx = graphDb.beginTx()) {
			Node term0 = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, ConceptManager.ConceptLabel.CONCEPT,
					PROP_ID, NodeIDPrefixConstants.TERM + 0);
			Iterable<Relationship> relationships = term0.getRelationships(ConceptManager.EdgeTypes.IS_MAPPED_TO);
			int relCounter = 0;
			for (Relationship rel : relationships) {
				Node otherNode = rel.getOtherNode(term0);
				assertEquals(NodeIDPrefixConstants.TERM + 1, otherNode.getProperty(PROP_ID));
				String[] mappingTypes = (String[]) rel.getProperty(ConceptRelationConstants.PROP_MAPPING_TYPE);
				List<String> mappingTypesList = Arrays.asList(mappingTypes);
				assertEquals(2, mappingTypesList.size());
				// Check the mapping types
				assertTrue(mappingTypesList.contains("loom"));
				assertTrue(mappingTypesList.contains("new_type"));
				relCounter++;
			}
			assertEquals(1, relCounter);
		}
	}

	@Test
	public void testAddMappingsInSameFacet() throws Exception {
		ImportConcepts testTerms;
		ConceptManager ftm = new ConceptManager();
		// Create terms in THE SAME facets we will then map to each other
		testTerms = getTestTerms(5);
		testTerms.getConceptsAsList().get(1).parentCoordinates = Arrays
				.asList(new ConceptCoordinates("CONCEPT0", "TEST_DATA", SRC));
		testTerms.getConceptsAsList().get(2).parentCoordinates = Arrays
				.asList(new ConceptCoordinates("CONCEPT1", "TEST_DATA", SRC));
		testTerms.getConceptsAsList().get(3).parentCoordinates = Arrays
				.asList(new ConceptCoordinates("CONCEPT2", "TEST_DATA", SRC));
		testTerms.getConceptsAsList().get(4).parentCoordinates = Arrays
				.asList(new ConceptCoordinates("CONCEPT3", "TEST_DATA", SRC));
		ftm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms));

		List<ImportMapping> mappings = Lists.newArrayList(new ImportMapping("CONCEPT0", "CONCEPT1", "loom"),
				new ImportMapping("CONCEPT3", "CONCEPT2", "same_uris"));

		// We have a loom mapping between CONCEPT0 and CONCEPT1 which should be
		// filtered out since both terms appear in the same facet.
		int numInsertedRels = ftm.insertMappings(graphDb, ConceptsJsonSerializer.toJson(mappings));
		assertEquals(1, numInsertedRels);
	}

	@Test
	public void testMergeTerms() throws Exception {
		// Here, we will insert some terms as normal. Then, we will insert some
		// terms anew, with new property values
		// that should then be merged. We won't define a new facet, we just want
		// to add new information to existing
		// terms.
		ImportConcepts testTerms = getTestTerms(4);
		// add a fifth term that has the first term as a parent
		testTerms.getConceptsAsList()
				.add(new ImportConcept("someterm", new ConceptCoordinates("somesrcid", "somesource", SRC),
						new ConceptCoordinates("CONCEPT0", "TEST_DATA", SRC)));
		testTerms.getConceptsAsList().get(testTerms.getConceptsAsList().size() - 1).coordinates.source = "somesource";
		ConceptManager tm = new ConceptManager();
		// first insert.
		tm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms));

		// now again get two test terms. Those will be identical to the first
		// two test terms above.
		testTerms = getTestTerms(2);
		// IMPORTANT: this is the key in this test: We do NOT insert a facet
		// which should be not necessary because we
		// only insert terms that are already in the database.
		testTerms.setFacet(null);
		// Give new property values to the first term, leave the second
		// unchanged.
		testTerms.getConceptsAsList().get(0).generalLabels = Lists.newArrayList("newlabel1");
		testTerms.getConceptsAsList().get(0).descriptions = Lists.newArrayList("newdescription1");
		testTerms.getConceptsAsList().get(0).prefName = "newprefname1";
		// re-insert the additional term from above but with a description and a
		// synonym, without a parent; we should
		// not need it since the term is already known.
		ImportConcept term = new ImportConcept("somesrcid", Arrays.asList("newsynonym2"), "newdesc2",
				new ConceptCoordinates("somesrcid", "somesource", SRC));
		testTerms.getConceptsAsList().add(term);
		// second insert, duplicate terms should now be merged.
		tm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms));

		// Test the success of the merging.
		try (Transaction tx = graphDb.beginTx()) {
			// See that we really only have a single facet, we havn't added a
			// second one.
			ResourceIterator<Node> facets = graphDb.findNodes(FacetManager.FacetLabel.FACET);
			int facetCounter = 0;
			for (@SuppressWarnings("unused")
			Node facet : (Iterable<Node>) () -> facets) {
				facetCounter++;
			}
			assertEquals(1, facetCounter);

			// Now test that the merged-in properties - and labels - are
			// actually there.
			Node mergedTerm = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, Label.label("newlabel1"), PROP_ID,
					"tid0");
			// have we found the term? Then it got its new label.
			assertNotNull(mergedTerm);
			List<String> descriptions = Lists.newArrayList((String[]) mergedTerm.getProperty(PROP_DESCRIPTIONS));
			assertTrue(descriptions.contains("desc of term0"));
			assertTrue(descriptions.contains("newdescription1"));

			Node facet = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, FacetManager.FacetLabel.FACET,
					FacetConstants.PROP_ID, "fid0");
			Iterable<Relationship> rootTermRelations = facet.getRelationships(ConceptManager.EdgeTypes.HAS_ROOT_CONCEPT);
			int rootTermCounter = 0;
			for (@SuppressWarnings("unused")
			Relationship rel : rootTermRelations) {
				rootTermCounter++;
			}
			// There should be no more facet roots, even though in the second
			// run we re-inserted a term and not give him
			// a parent; this normally causes the term to be a root term, but
			// not if it did exist before.
			assertEquals(4, rootTermCounter);
		}
	}

	@Test
	public void testIntegerProperty() throws Exception {
		List<Map<String, Object>> terms = new ArrayList<>();
		Map<String, Object> term = new HashMap<>();
		term.put(PROP_PREF_NAME, "testPrefName");
		term.put("someIntegerProperty", 42);
		term.put("someIntegerArrayProperty", new Integer[] { 23, 98 });

		Map<String, String> coordinates = new HashMap<>();
		coordinates.put(CoordinateConstants.SOURCE_ID, "testSrcId");
		coordinates.put(CoordinateConstants.SOURCE, "TEST_SOURCE");
		term.put(ConceptConstants.COORDINATES, coordinates);
		terms.add(term);

		Map<String, Object> termsMap = new HashMap<>();
		termsMap.put(ConceptManager.KEY_FACET, FacetManagerTest.getImportFacet());
		termsMap.put(ConceptManager.KEY_CONCEPTS, terms);

		ConceptManager tm = new ConceptManager();
		tm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(termsMap));

		try (Transaction tx = graphDb.beginTx()) {
			Node termNode = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, ConceptManager.ConceptLabel.CONCEPT,
					PROP_ID, NodeIDPrefixConstants.TERM + 0);
			assertEquals(42, termNode.getProperty("someIntegerProperty"));
			// the order should have been maintained
			assertArrayEquals(new int[] { 23, 98 }, (int[]) termNode.getProperty("someIntegerArrayProperty"));
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testGetAllFacetRoots() throws Exception {
		ConceptManager tm = new ConceptManager();
		ImportConcepts testTerms = getTestTerms(3);
		tm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms));
		// Insert two times so we have two facets
		testTerms.getFacet().setName("secondfacet");
		tm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms));
		RecursiveMappingRepresentation facetRoots = (RecursiveMappingRepresentation) tm
				.getFacetRoots(graphDb,
						ConceptsJsonSerializer.toJson(
								Lists.newArrayList(NodeIDPrefixConstants.FACET + 0, NodeIDPrefixConstants.FACET + 1)),
						null, 0);
		Map<String, Object> map = facetRoots.getUnderlyingMap();

		try (Transaction tx = graphDb.beginTx()) {
			// Roots of two facets
			assertEquals(2, map.size());
			List<Node> roots = (List<Node>) map.get(NodeIDPrefixConstants.FACET + 0);
			List<String> rootIds = new ArrayList<>();
			for (Node root : roots)
				rootIds.add((String) root.getProperty(PROP_ID));

			assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 0));
			assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 1));
			assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 2));

			roots = (List<Node>) map.get(NodeIDPrefixConstants.FACET + 1);
			rootIds = new ArrayList<>();
			for (Node root : roots)
				rootIds.add((String) root.getProperty(PROP_ID));
			assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 0));
			assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 1));
			assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 2));
		}
	}

	@Test
	public void testGetFacetRootsWithLimit() throws Exception {
		// the exact same test as testGetAllFacetRoots() but with a limit on
		// maximum roots
		ConceptManager tm = new ConceptManager();
		ImportConcepts testTerms = getTestTerms(3);
		tm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms));
		// Insert two times so we have two facets
		tm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms));
		RecursiveMappingRepresentation facetRoots = (RecursiveMappingRepresentation) tm
				.getFacetRoots(graphDb,
						ConceptsJsonSerializer.toJson(
								Lists.newArrayList(NodeIDPrefixConstants.FACET + 0, NodeIDPrefixConstants.FACET + 1)),
						null, 2);
		Map<String, Object> map = facetRoots.getUnderlyingMap();

		try (Transaction tx = graphDb.beginTx()) {
			// Roots of two facets
			assertEquals(0, map.size());
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testGetSpecificFacetRoots() throws Exception {
		ConceptManager tm = new ConceptManager();
		ImportConcepts testTerms = getTestTerms(3);
		// Insert three times so we have three facets
		tm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms));
		testTerms.getFacet().setName("secondfacet");
		tm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms));
		testTerms.getFacet().setName("thirdfacet");
		tm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms));

		Map<String, List<String>> requestedRoots = new HashMap<>();
		requestedRoots.put(NodeIDPrefixConstants.FACET + 0, Lists.newArrayList(NodeIDPrefixConstants.TERM + 0));
		requestedRoots.put(NodeIDPrefixConstants.FACET + 1,
				Lists.newArrayList(NodeIDPrefixConstants.TERM + 1, NodeIDPrefixConstants.TERM + 2));
		// for the third facet, we want all roots returned

		RecursiveMappingRepresentation facetRoots = (RecursiveMappingRepresentation) tm.getFacetRoots(graphDb,
				ConceptsJsonSerializer.toJson(Lists.newArrayList(NodeIDPrefixConstants.FACET + 0,
						NodeIDPrefixConstants.FACET + 1, NodeIDPrefixConstants.FACET + 2)),
				ConceptsJsonSerializer.toJson(requestedRoots), 0);
		Map<String, Object> map = facetRoots.getUnderlyingMap();

		try (Transaction tx = graphDb.beginTx()) {
			// Roots of two facets
			assertEquals(3, map.size());
			List<Node> roots = (List<Node>) map.get(NodeIDPrefixConstants.FACET + 0);
			List<String> rootIds = new ArrayList<>();
			for (Node root : roots)
				rootIds.add((String) root.getProperty(PROP_ID));

			assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 0));
			assertFalse(rootIds.contains(NodeIDPrefixConstants.TERM + 1));
			assertFalse(rootIds.contains(NodeIDPrefixConstants.TERM + 2));

			roots = (List<Node>) map.get(NodeIDPrefixConstants.FACET + 1);
			rootIds = new ArrayList<>();
			for (Node root : roots)
				rootIds.add((String) root.getProperty(PROP_ID));
			assertFalse(rootIds.contains(NodeIDPrefixConstants.TERM + 0));
			assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 1));
			assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 2));

			roots = (List<Node>) map.get(NodeIDPrefixConstants.FACET + 2);
			rootIds = new ArrayList<>();
			for (Node root : roots)
				rootIds.add((String) root.getProperty(PROP_ID));
			assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 0));
			assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 1));
			assertTrue(rootIds.contains(NodeIDPrefixConstants.TERM + 2));
		}
	}

	// @Test
	// public void testAddWritingVariants() throws Exception {
	// ImportConceptAndFacet testTerms = getTestTerms(1);
	// // add a synonym for which equivalent writing variants should be removed
	// testTerms.terms.get(0).synonyms = Arrays.asList("beclin 1");
	// ConceptManager tm = new ConceptManager();
	// tm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms));
	//
	// Map<String, Object> variants = new HashMap<>();
	// Map<String, Integer> variantCounts = new HashMap<>();
	// variantCounts.put("prefname0", 3);
	// variantCounts.put("var1", 4);
	// variantCounts.put("var2", 2);
	// variantCounts.put("var3", 1);
	// variantCounts.put("vAR3", 1);
	// variantCounts.put("Beclin 1", 2);
	// variants.put(NodeIDPrefixConstants.TERM + 0, variantCounts);
	// tm.addWritingVariants(graphDb, JsonSerializer.toJson(variants), null);
	//
	// try (Transaction tx = graphDb.beginTx()) {
	// // we should have a single writing variants node
	// ResourceIterator<Node> writingVariantNodes =
	// graphDb.findNodes(MorphoLabel.WRITING_VARIANT);
	// assertTrue(writingVariantNodes.hasNext());
	// Node writingVariantsNode = writingVariantNodes.next();
	// assertFalse(writingVariantNodes.hasNext());
	// Node term = graphDb.findNode(TermLabel.CONCEPT, PROP_ID,
	// NodeIDPrefixConstants.TERM + 0);
	// assertNotNull(term);
	// Node singleOtherNode = NodeUtilities.getSingleOtherNode(term,
	// EdgeTypes.HAS_VARIANTS);
	// assertEquals(singleOtherNode, writingVariantsNode);
	// assertEquals(4, writingVariantsNode.getProperty("var1"));
	// assertEquals(2, writingVariantsNode.getProperty("var2"));
	// // we cannot tell which writing will be chosen by the algorithm; but
	// // the two should be collapsed to one because they only differ in
	// // case
	// assertTrue(writingVariantsNode.hasProperty("var3") ||
	// writingVariantsNode.hasProperty("vAR3"));
	// if (writingVariantsNode.hasProperty("var3"))
	// assertEquals(2, writingVariantsNode.getProperty("var3"));
	// if (writingVariantsNode.hasProperty("vAR3"))
	// assertEquals(2, writingVariantsNode.getProperty("vAR3"));
	//// assertFalse(writingVariantsNode.hasProperty("Beclin 1"));
	//// assertFalse(writingVariantsNode.hasProperty("prefname0"));
	// }
	// }

	@Test
	public void testAddFacetTwice() throws Exception {
		// Two equal facets. We want to see that they are not imported twice
		ImportConcepts facet1 = getTestTerms(1);
		ImportConcepts facet2 = getTestTerms(1);

		ConceptManager tm = new ConceptManager();
		tm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(facet1));
		tm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(facet2));

		try (Transaction tx = graphDb.beginTx()) {
			assertEquals(1, countNodesWithLabel(FacetLabel.FACET));
		}
	}

	@Test
	public void testAddWritingVariants() throws Exception {
		{
			ImportConcepts testTerms = getTestTerms(1);
			ConceptManager tm = new ConceptManager();
			tm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms));

			Map<String, Map<String, Map<String, Integer>>> variantCountsByTermIdPerDoc = new HashMap<>();
			Map<String, Map<String, Integer>> variantCountsPerDoc = new HashMap<>();
			Map<String, Integer> variantCounts = new HashMap<>();
			variantCounts.put("var1", 1);
			variantCounts.put("var2", 2);
			variantCountsPerDoc.put("doc1", variantCounts);
			variantCounts = new HashMap<>();
			variantCounts.put("var1", 3);
			variantCountsPerDoc.put("doc2", variantCounts);
			variantCountsByTermIdPerDoc.put(NodeIDPrefixConstants.TERM + 0, variantCountsPerDoc);
			tm.addWritingVariants(graphDb, ConceptsJsonSerializer.toJson(variantCountsByTermIdPerDoc), null);
		}

		try (Transaction tx = graphDb.beginTx()) {
			Node variantsNode = graphDb.findNode(MorphoLabel.WRITING_VARIANT, MorphoConstants.PROP_ID, "var1");
			assertNotNull(variantsNode);
			Iterable<Relationship> relationships = variantsNode.getRelationships(Direction.INCOMING,
					EdgeTypes.HAS_ELEMENT);
			List<Relationship> relList = new ArrayList<>();
			for (Relationship rel : relationships)
				relList.add(rel);
			// there is only a single term in this test so there should be a
			// single relationship to this writing variant
			assertEquals(1, relList.size());
			Relationship hasWritingVariantRel = relList.get(0);
			String[] docs = (String[]) hasWritingVariantRel.getProperty(MorphoRelationConstants.PROP_DOCS);
			assertTrue(docs.length >= 2);
			assertEquals("doc1", docs[0]);
			assertEquals("doc2", docs[1]);

			int[] counts = (int[]) hasWritingVariantRel.getProperty(MorphoRelationConstants.PROP_COUNTS);
			assertEquals(docs.length, counts.length);
			assertEquals(1, counts[0]);
			assertEquals(3, counts[1]);
		}
	}

	@Test
	public void testAddWritingVariants2() throws Exception {
		{
			ImportConcepts testTerms = getTestTerms(1);
			ConceptManager tm = new ConceptManager();
			tm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms));

			Map<String, Map<String, Map<String, Integer>>> variantCountsByTermIdPerDoc = new HashMap<>();
			Map<String, Map<String, Integer>> variantCountsPerDoc = new HashMap<>();
			Map<String, Integer> variantCounts = new HashMap<>();
			variantCounts.put("var1", 1);
			variantCountsPerDoc.put("doc1", variantCounts);
			variantCounts = new HashMap<>();
			variantCounts.put("var1", 2);
			variantCountsPerDoc.put("doc2", variantCounts);
			variantCounts = new HashMap<>();
			variantCounts.put("var1", 3);
			variantCountsPerDoc.put("doc3", variantCounts);
			variantCounts = new HashMap<>();
			variantCounts.put("var1", 4);
			variantCountsPerDoc.put("doc4", variantCounts);
			variantCounts = new HashMap<>();
			variantCounts.put("var1", 5);
			variantCountsPerDoc.put("doc5", variantCounts);
			variantCounts = new HashMap<>();
			variantCounts.put("var1", 6);
			variantCountsPerDoc.put("doc6", variantCounts);
			variantCounts = new HashMap<>();
			variantCounts.put("var1", 7);
			variantCountsPerDoc.put("doc7", variantCounts);
			variantCounts = new HashMap<>();
			variantCounts.put("var1", 8);
			variantCountsPerDoc.put("doc8", variantCounts);
			variantCountsByTermIdPerDoc.put(NodeIDPrefixConstants.TERM + 0, variantCountsPerDoc);
			tm.addWritingVariants(graphDb, ConceptsJsonSerializer.toJson(variantCountsByTermIdPerDoc), null);
		}

		try (Transaction tx = graphDb.beginTx()) {
			Node variantsNode = graphDb.findNode(MorphoLabel.WRITING_VARIANT, MorphoConstants.PROP_ID, "var1");
			assertNotNull(variantsNode);
			Iterable<Relationship> relationships = variantsNode.getRelationships(Direction.INCOMING,
					EdgeTypes.HAS_ELEMENT);
			List<Relationship> relList = new ArrayList<>();
			for (Relationship rel : relationships)
				relList.add(rel);
			// there is only a single term in this test so there should be a
			// single relationship to this writing variant
			assertEquals(1, relList.size());
			Relationship hasWritingVariantRel = relList.get(0);
			String[] docs = (String[]) hasWritingVariantRel.getProperty(MorphoRelationConstants.PROP_DOCS);
			assertTrue(docs.length >= 8);
			System.out.println(PropertyUtilities.getNodePropertiesAsString(hasWritingVariantRel));
			assertEquals("doc1", docs[0]);
			assertEquals("doc2", docs[1]);
			assertEquals("doc3", docs[2]);
			assertEquals("doc4", docs[3]);
			assertEquals("doc5", docs[4]);
			assertEquals("doc6", docs[5]);
			assertEquals("doc7", docs[6]);
			assertEquals("doc8", docs[7]);

			int[] counts = (int[]) hasWritingVariantRel.getProperty(MorphoRelationConstants.PROP_COUNTS);
			assertEquals(docs.length, counts.length);
			assertEquals(1, counts[0]);
			assertEquals(2, counts[1]);
			assertEquals(3, counts[2]);
			assertEquals(4, counts[3]);
			assertEquals(5, counts[4]);
			assertEquals(6, counts[5]);
			assertEquals(7, counts[6]);
			assertEquals(8, counts[7]);
		}
	}

	@Test
	public void testUniqueSourceIds() throws Exception {
		ImportConcepts testTerms = getTestTerms(2);
		// first, remove original ID and source because otherwise the original
		// ID checks will take over
		testTerms.getConceptsAsList().get(0).coordinates.originalId = null;
		testTerms.getConceptsAsList().get(0).coordinates.originalSource = null;
		testTerms.getConceptsAsList().get(1).coordinates.originalId = null;
		testTerms.getConceptsAsList().get(1).coordinates.originalSource = null;

		// now set terms that should be equal by source ID despite having a
		// different source
		testTerms.getConceptsAsList().get(0).coordinates.uniqueSourceId = true;
		testTerms.getConceptsAsList().get(0).coordinates.sourceId = "id0";
		testTerms.getConceptsAsList().get(0).coordinates.source = "source0";
		testTerms.getConceptsAsList().get(1).coordinates.uniqueSourceId = true;
		testTerms.getConceptsAsList().get(1).coordinates.sourceId = "id0";
		testTerms.getConceptsAsList().get(1).coordinates.source = "source1";

		ConceptManager tm = new ConceptManager();
		tm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(testTerms));
	}

	public static void printNodeLabels(Node n) {
		for (Label l : n.getLabels()) {
			System.out.println(l.name());
		}
	}

	/**
	 * Returns such a map:
	 * <tt>{"facet":&lt;facetMap&gt;, "terms":&lt;list of one map for each term&gt;}</tt>
	 * 
	 * @param amount
	 *            The amount of terms to generate.
	 * @return
	 */
	public static ImportConcepts getTestTerms(int amount) {
		return getTestTerms(amount, 0);
	}

	/**
	 * Returns such a map:
	 * <tt>{"facet":&lt;facetMap&gt;, "terms":&lt;list of one map for each term&gt;}</tt>
	 * 
	 * @param amount
	 *            The amount of terms to generate.
	 * @return
	 */
	public static ImportConcepts getTestTerms(int amount, int startAt) {
		List<ImportConcept> termList = new ArrayList<>(amount);
		for (int i = startAt; i < amount + startAt; i++) {
			ConceptCoordinates coordinates = new ConceptCoordinates("CONCEPT" + i, "TEST_DATA", "CONCEPT" + i, "TEST_DATA",
					false);
			ImportConcept term = new ImportConcept("prefname" + i, "desc of term" + i, coordinates);
			termList.add(term);
		}

		return new ImportConcepts(termList, FacetManagerTest.getImportFacet());
	}

}
