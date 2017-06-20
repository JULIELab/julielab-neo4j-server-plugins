package de.julielab.neo4j.plugins;

import static de.julielab.neo4j.plugins.constants.semedico.FacetConstants.NAME_NO_FACET_GROUPS;
import static de.julielab.neo4j.plugins.constants.semedico.NodeConstants.PROP_ID;
import static de.julielab.neo4j.plugins.constants.semedico.NodeConstants.PROP_NAME;
import static de.julielab.neo4j.plugins.constants.semedico.TermConstants.PARENT_SRC_IDS;
import static de.julielab.neo4j.plugins.constants.semedico.TermConstants.PROP_DESCRIPTIONS;
import static de.julielab.neo4j.plugins.constants.semedico.TermConstants.PROP_FACETS;
import static de.julielab.neo4j.plugins.constants.semedico.TermConstants.PROP_ORG_ID;
import static de.julielab.neo4j.plugins.constants.semedico.TermConstants.PROP_ORG_SRC;
import static de.julielab.neo4j.plugins.constants.semedico.TermConstants.PROP_PREF_NAME;
import static de.julielab.neo4j.plugins.constants.semedico.TermConstants.PROP_SOURCES;
import static de.julielab.neo4j.plugins.constants.semedico.TermConstants.PROP_SRC_IDS;
import static de.julielab.neo4j.plugins.constants.semedico.TermConstants.PROP_SYNONYMS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.shell.util.json.JSONException;
import org.neo4j.shell.util.json.JSONObject;
import org.neo4j.tooling.GlobalGraphOperations;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.Gson;

import de.julielab.neo4j.plugins.FacetManager.FacetLabel;
import de.julielab.neo4j.plugins.TermManager.EdgeTypes;
import de.julielab.neo4j.plugins.TermManager.MorphoLabel;
import de.julielab.neo4j.plugins.TermManager.TermLabel;
import de.julielab.neo4j.plugins.auxiliaries.NodeUtilities;
import de.julielab.neo4j.plugins.auxiliaries.PropertyUtilities;
import de.julielab.neo4j.plugins.auxiliaries.RecursiveMappingRepresentation;
import de.julielab.neo4j.plugins.auxiliaries.semedico.TermAggregateBuilder;
import de.julielab.neo4j.plugins.auxiliaries.semedico.TermNameAndSynonymComparator;
import de.julielab.neo4j.plugins.constants.semedico.FacetConstants;
import de.julielab.neo4j.plugins.constants.semedico.MorphoConstants;
import de.julielab.neo4j.plugins.constants.semedico.MorphoRelationConstants;
import de.julielab.neo4j.plugins.constants.semedico.NodeConstants;
import de.julielab.neo4j.plugins.constants.semedico.NodeIDPrefixConstants;
import de.julielab.neo4j.plugins.constants.semedico.TermConstants;
import de.julielab.neo4j.plugins.constants.semedico.TermRelationConstants;
import de.julielab.neo4j.plugins.datarepresentation.AddToNonFacetGroupCommand;
import de.julielab.neo4j.plugins.datarepresentation.ImportFacet;
import de.julielab.neo4j.plugins.datarepresentation.ImportFacetTermRelationship;
import de.julielab.neo4j.plugins.datarepresentation.ImportMapping;
import de.julielab.neo4j.plugins.datarepresentation.ImportOptions;
import de.julielab.neo4j.plugins.datarepresentation.ImportTerm;
import de.julielab.neo4j.plugins.datarepresentation.ImportTermAndFacet;
import de.julielab.neo4j.plugins.datarepresentation.JsonSerializer;
import de.julielab.neo4j.plugins.datarepresentation.PushTermsToSetCommand;
import de.julielab.neo4j.plugins.datarepresentation.TermCoordinates;
import de.julielab.neo4j.plugins.test.TestUtilities;

public class TermManagerTest {

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
	public void testOriginalIdMerging() throws Exception {
		// This test checks whether multiple, different sources and terms with
		// the same origin (same original ID and
		// original source) are stored correctly.
		TermManager tm = new TermManager();

		ImportTermAndFacet testTerms;
		testTerms = getTestTerms(1);
		testTerms.terms.get(0).originalId = "orgId";
		testTerms.terms.get(0).originalSource = "src1";
		testTerms.terms.get(0).source = "src1";
		tm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms));

		testTerms = getTestTerms(1);
		testTerms.terms.get(0).originalId = "orgId";
		testTerms.terms.get(0).originalSource = "src1";
		testTerms.terms.get(0).source = "src2";
		tm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms));

		testTerms = getTestTerms(1);
		testTerms.terms.get(0).sourceId = "TERM42";
		testTerms.terms.get(0).originalId = "orgId";
		testTerms.terms.get(0).originalSource = "src1";
		testTerms.terms.get(0).source = null;
		tm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms));

		try (Transaction tx = graphDb.beginTx()) {
			Node term = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, TermManager.TermLabel.TERM, PROP_ID,
					NodeIDPrefixConstants.TERM + 0);
			assertNotNull(term);
			assertEquals("orgId", term.getProperty(PROP_ORG_ID));
			String[] sourceIds = (String[]) term.getProperty(PROP_SRC_IDS);
			String[] sources = (String[]) term.getProperty(TermConstants.PROP_SOURCES);
			assertArrayEquals(new String[] { "src1", "src2", "<unknown>" }, sources);
			assertArrayEquals(new String[] { "TERM0", "TERM0", "TERM42" }, sourceIds);
		}

	}

	@Test
	public void testMergeOnOriginalIdWithoutSourceId() throws Exception {
		// This test assures that pure term merging works by addressing terms
		// via their original ID without
		// specification of their source ID.
		TermManager tm = new TermManager();

		ImportTermAndFacet testTerms;
		testTerms = getTestTerms(1);
		testTerms.terms.get(0).originalId = "orgId";
		testTerms.terms.get(0).originalSource = "src1";
		testTerms.terms.get(0).source = "someSource";
		testTerms.terms.get(0).descriptions = null;
		tm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms));

		try (Transaction tx = graphDb.beginTx()) {
			Node term = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, TermManager.TermLabel.TERM, PROP_ID,
					NodeIDPrefixConstants.TERM + 0);
			assertNotNull(term);
			assertFalse(term.hasProperty(PROP_DESCRIPTIONS));
		}

		// Now add a description, only by knowing the term's original ID.
		testTerms = getTestTerms(1);
		testTerms.terms.get(0).originalId = "orgId";
		testTerms.terms.get(0).originalSource = "src1";
		testTerms.terms.get(0).sourceId = null;
		testTerms.terms.get(0).descriptions = Lists.newArrayList("desc");
		ImportOptions importOptions = new ImportOptions();
		importOptions.merge = true;
		testTerms.importOptions = importOptions;
		tm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms));

		try (Transaction tx = graphDb.beginTx()) {
			Node term = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, TermManager.TermLabel.TERM, PROP_ID,
					NodeIDPrefixConstants.TERM + 0);
			assertNotNull(term);
			assertTrue(term.hasProperty(PROP_DESCRIPTIONS));
			assertArrayEquals(new String[] { "desc" }, (String[]) term.getProperty(PROP_DESCRIPTIONS));
			assertArrayEquals(new String[] { "someSource" }, (String[]) term.getProperty(PROP_SOURCES));
		}

	}

	@Test
	public void testSameOrgIdDifferentSource() throws Exception {
		// This test assures that two terms with the same original ID are still
		// different if the original sources
		// differ.
		TermManager tm = new TermManager();

		ImportTermAndFacet testTerms;
		testTerms = getTestTerms(1);
		testTerms.terms.get(0).originalId = "orgId";
		testTerms.terms.get(0).originalSource = "src1";
		// we also have to set the source ID and source of at least one term to
		// a different value or we will get an exception telling us that our
		// data is inconsistent (because source ID and source would match but
		// the original ID and original source wouldn't)
		testTerms.terms.get(0).sourceId = "anothersourceid";
		testTerms.terms.get(0).source = "anothersource";
		tm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms));

		testTerms = getTestTerms(1);
		testTerms.terms.get(0).originalId = "orgId";
		testTerms.terms.get(0).originalSource = "src2";
		tm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms));

		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterable<Node> terms = GlobalGraphOperations.at(graphDb).getAllNodesWithLabel(TermLabel.TERM);
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
		}

	}

	/**
	 * Tests the import of a (small) set of terms together with their complete
	 * facet definition. The facet should be created and the terms be created
	 * and connected to it.
	 * 
	 * @throws JSONException
	 * @throws IllegalAccessException
	 * @throws NoSuchFieldException
	 * @throws IllegalArgumentException
	 * @throws SecurityException
	 */
	@Test
	public void testImportTermsWithFacetDefinition() throws JSONException, SecurityException, IllegalArgumentException,
			NoSuchFieldException, IllegalAccessException {
		testTermImportWithOrWithoutFacetDefinition(true);
	}

	@Test
	public void testImportTermsWithoutFacetDefinition() throws JSONException, SecurityException,
			IllegalArgumentException, NoSuchFieldException, IllegalAccessException {
		// Here, create the facet "manually" and then do the term import. The
		// import method "knows" which facet Id to use (there will only be this
		// one facet, so its fid0...)
		ImportFacet facetMap = FacetManagerTest.getImportFacet();
		Gson gson = new Gson();
		String jsonFacet = gson.toJson(facetMap);
		FacetManager.createFacet(graphDb, new JSONObject(jsonFacet));

		// Do the term import and tests.
		testTermImportWithOrWithoutFacetDefinition(false);
	}

	private void testTermImportWithOrWithoutFacetDefinition(boolean withFacetDefinition) throws JSONException {
		// ----------- THE FACET --------------
		ImportFacet facetMap;
		if (withFacetDefinition) {
			facetMap = FacetManagerTest.getImportFacet();
		} else {
			// The facet has already been created and only its ID has been
			// given.
			facetMap = new ImportFacet("fid0");
		}

		// ----------- THE TERMS ---------------
		// Structure:
		// TERM1
		// ===== TERM2
		// ---------- TERM4
		// ----- TERM3
		// Note the ===== relationship: We will insert term2 twice but we expect
		// that only ONE relationship is created in the end, i.e. the duplicate
		// is recognized.
		List<Map<String, Object>> termList = new ArrayList<>();
		Map<String, Object> termMap = new HashMap<>();
		termMap.put(PROP_PREF_NAME, "prefname1");
		termMap.put(PROP_DESCRIPTIONS, Lists.newArrayList("desc of term1"));
		termMap.put(PROP_SRC_IDS, "TERM1");
		termList.add(termMap);
		termMap = new HashMap<>();
		termMap.put(PROP_PREF_NAME, "prefname2");
		termMap.put(PROP_ORG_ID, "orgId2");
		termMap.put(PROP_ORG_SRC, "orgSrc1");
		termMap.put(PROP_SRC_IDS, "TERM2");
		termMap.put(PARENT_SRC_IDS, Lists.newArrayList("TERM1"));
		termList.add(termMap);
		// duplicate of term 2 to test relationship de-duplication.
		termMap = new HashMap<>();
		termMap.put(PROP_PREF_NAME, "prefname2");
		termMap.put(PROP_ORG_ID, "orgId2");
		termMap.put(PROP_ORG_SRC, "orgSrc1");
		termMap.put(PROP_SRC_IDS, "TERM2");
		termMap.put(PARENT_SRC_IDS, Lists.newArrayList("TERM1"));
		termList.add(termMap);
		termMap = new HashMap<>();
		termMap.put(PROP_PREF_NAME, "prefname3");
		termMap.put(PROP_SRC_IDS, "TERM3");
		termMap.put(PARENT_SRC_IDS, Lists.newArrayList("TERM1"));
		termList.add(termMap);
		termMap = new HashMap<>();
		termMap.put(PROP_PREF_NAME, "prefname4");
		termMap.put(PROP_SRC_IDS, "TERM4");
		termMap.put(PARENT_SRC_IDS, Lists.newArrayList("TERM2"));
		termList.add(termMap);

		Map<String, Object> termsAndFacet = new HashMap<String, Object>();
		termsAndFacet.put("facet", facetMap);
		termsAndFacet.put("terms", termList);

		// --------- CREATE JSON AND SEND DATA --------
		Gson gson = new Gson();
		String termsAndFacetBytes = gson.toJson(termsAndFacet);
		TermManager ftm = new TermManager();
		ftm.insertFacetTerms(graphDb, termsAndFacetBytes);

		// --------- MAKE TESTS ---------------

		// Is the facet there?
		IndexManager im = graphDb.index();
		try (Transaction tx = graphDb.beginTx()) {
			Node facet = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, FacetManager.FacetLabel.FACET, PROP_ID,
					"fid0");
			assertEquals("testfacet1", facet.getProperty(PROP_NAME));

			// Are the term Properties correct?
			Index<Node> termIdIndex = im.forNodes(TermConstants.INDEX_NAME);
			IndexHits<Node> terms = termIdIndex.query(PROP_ID, "tid*");
			assertEquals(4, terms.size());

			Node term1 = termIdIndex.get(PROP_ID, "tid0").getSingle();
			assertEquals("prefname1", term1.getProperty(PROP_PREF_NAME));
			assertEquals(Lists.newArrayList("desc of term1"),
					Arrays.asList((String[]) term1.getProperty(PROP_DESCRIPTIONS)));
			assertArrayEquals(new String[] { "TERM1" }, (String[]) term1.getProperty(PROP_SRC_IDS));

			Node term2 = termIdIndex.get(PROP_ID, "tid1").getSingle();
			assertEquals("prefname2", term2.getProperty(PROP_PREF_NAME));
			assertEquals("orgId2", term2.getProperty(PROP_ORG_ID));
			assertArrayEquals(new String[] { "TERM2" }, (String[]) term2.getProperty(PROP_SRC_IDS));

			Node term3 = termIdIndex.get(PROP_ID, "tid2").getSingle();
			assertEquals("prefname3", term3.getProperty(PROP_PREF_NAME));
			assertArrayEquals(new String[] { "TERM3" }, (String[]) term3.getProperty(PROP_SRC_IDS));

			Node term4 = termIdIndex.get(PROP_ID, "tid3").getSingle();
			assertEquals("prefname4", term4.getProperty(PROP_PREF_NAME));
			assertArrayEquals(new String[] { "TERM4" }, (String[]) term4.getProperty(PROP_SRC_IDS));

			// Are the relationships correct? Reminder, they should be:
			// Structure:
			// TERM1
			// ----- TERM2
			// ---------- TERM4
			// ----- TERM3
			// where there is only ONE relationship from TERM1 to TERM2 although
			// we
			// have added TERM2 twice. This is tested automatically by using
			// "getSingleRelationShip".
			// For simplicity, we here ask the child for its parent because in
			// this
			// test, there is only one parent (in general, there might be
			// more!).
			assertEquals(term1, term2.getSingleRelationship(TermManager.EdgeTypes.IS_BROADER_THAN, Direction.INCOMING)
					.getStartNode());
			assertEquals(term1, term3.getSingleRelationship(TermManager.EdgeTypes.IS_BROADER_THAN, Direction.INCOMING)
					.getStartNode());
			assertEquals(term2, term4.getSingleRelationship(TermManager.EdgeTypes.IS_BROADER_THAN, Direction.INCOMING)
					.getStartNode());

			// Besides the default taxonomic relationships, there should be
			// specific relationships only valid for the
			// current facet.
			DynamicRelationshipType relBroaderThenInFacet = DynamicRelationshipType
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

			tx.success();
		}
	}

	@Test
	public void testInsertTermIntoMultipleFacets() throws JSONException {
		// Two facets will be created. A term will be added to both, then.
		// The first facet will be sent with the term as whole facet definition.
		// The second facet will be created beforehand and then the term will
		// just be added to it.
		ImportFacet facetMap = FacetManagerTest.getTestFacetMap(1);

		List<Map<String, Object>> termList = new ArrayList<Map<String, Object>>();
		Map<String, Object> termMap = new HashMap<>();
		termMap.put(PROP_PREF_NAME, "prefname1");
		termMap.put(PROP_DESCRIPTIONS, Lists.newArrayList("desc of term1"));
		termMap.put(PROP_SRC_IDS, "TERM1");
		termMap.put(TermConstants.PROP_SOURCES, "TEST_SOURCE");
		termList.add(termMap);

		// -------- SEND TERM WITH FACET DEFINITION ------
		Gson gson = new Gson();
		TermManager ftm = new TermManager();

		Map<String, Object> termsAndFacet = new HashMap<String, Object>();
		termsAndFacet.put("facet", facetMap);
		termsAndFacet.put("terms", termList);
		String termsAndFacetJson = gson.toJson(termsAndFacet);
		ftm.insertFacetTerms(graphDb, termsAndFacetJson);

		// Create the 2nd facet separately.
		facetMap = FacetManagerTest.getTestFacetMap(2);
		String jsonFacet = gson.toJson(facetMap);
		FacetManager.createFacet(graphDb, new JSONObject(jsonFacet));

		// ---------- SEND TERM ONLY WITH FACET ID --------
		facetMap = new ImportFacet("fid1");
		termsAndFacet.put("facet", facetMap);
		termsAndFacetJson = gson.toJson(termsAndFacet);
		ftm.insertFacetTerms(graphDb, termsAndFacetJson);

		// ------------------ MAKE TESTS ---------------

		try (Transaction tx = graphDb.beginTx()) {
			IndexManager im = graphDb.index();
			Index<Node> termIdIndex = im.forNodes(TermConstants.INDEX_NAME);
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

		ImportTermAndFacet testTerms = getTestTerms(5);
		testTerms.facet.noFacet = true;

		TermManager ftm = new TermManager();
		ftm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms));

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
			Iterator<Relationship> rootIt = facetNode.getRelationships(TermManager.EdgeTypes.HAS_ROOT_TERM).iterator();
			int rootCount = 0;
			while (rootIt.hasNext()) {
				@SuppressWarnings("unused")
				Relationship root = rootIt.next();
				rootCount++;
			}
			assertEquals("Wrong number of roots for the facet: ", new Integer(5), new Integer(rootCount));
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testMergeTermProperties() throws JSONException {
		// We will insert the same term (identified by the same original ID)
		// multiple times with additional
		// information each time. At the end, the information that can be merged
		// should be complete.
		ImportFacet facetMap = FacetManagerTest.getImportFacet();

		// ------------ INSERT 1 ---------------

		Map<String, Object> termMap = new HashMap<String, Object>();
		termMap.put(PROP_PREF_NAME, "prefname1");
		termMap.put(PROP_SRC_IDS, "TERM1");
		termMap.put(PROP_ORG_ID, "ORGID");
		termMap.put(PROP_ORG_SRC, "orgSrc1");

		Gson gson = new Gson();
		TermManager ftm = new TermManager();

		Map<String, Object> termsAndFacet = new HashMap<String, Object>();
		termsAndFacet.put("facet", facetMap);
		termsAndFacet.put("terms", Lists.newArrayList(termMap));
		String termsAndFacetJson = gson.toJson(termsAndFacet);
		ftm.insertFacetTerms(graphDb, termsAndFacetJson);

		// ------------ INSERT 2 ---------------
		termMap = new HashMap<String, Object>();
		termMap.put(PROP_PREF_NAME, "prefname1");
		termMap.put(PROP_DESCRIPTIONS, Lists.newArrayList("description1"));
		termMap.put(PROP_SRC_IDS, "TERM1");
		termMap.put(PROP_ORG_ID, "ORGID");
		termMap.put(PROP_ORG_SRC, "orgSrc1");

		termsAndFacet.put("terms", Lists.newArrayList(termMap));
		termsAndFacetJson = gson.toJson(termsAndFacet);
		ftm.insertFacetTerms(graphDb, termsAndFacetJson);

		// ------------ INSERT 3 ---------------
		termMap = new HashMap<String, Object>();
		termMap.put(PROP_PREF_NAME, "prefname2");
		termMap.put(PROP_SYNONYMS, Lists.newArrayList("syn1"));
		termMap.put(PROP_SRC_IDS, "TERM2");
		termMap.put(PROP_ORG_ID, "ORGID");
		termMap.put(PROP_ORG_SRC, "orgSrc1");

		termsAndFacet.put("terms", Lists.newArrayList(termMap));
		termsAndFacetJson = gson.toJson(termsAndFacet);
		ftm.insertFacetTerms(graphDb, termsAndFacetJson);

		// ------------ INSERT 4 ---------------
		termMap = new HashMap<String, Object>();
		termMap.put(PROP_PREF_NAME, "prefname3");
		termMap.put(PROP_SYNONYMS, Lists.newArrayList("syn2"));
		termMap.put(PROP_DESCRIPTIONS, Lists.newArrayList("description2"));
		termMap.put(PROP_SRC_IDS, "TERM3");
		termMap.put(PROP_ORG_ID, "ORGID");
		termMap.put(PROP_ORG_SRC, "orgSrc1");

		termsAndFacet.put("terms", Lists.newArrayList(termMap));
		termsAndFacetJson = gson.toJson(termsAndFacet);
		ftm.insertFacetTerms(graphDb, termsAndFacetJson);

		// ------------ MAKE TESTS ---------------
		try (Transaction tx = graphDb.beginTx()) {
			IndexManager im = graphDb.index();
			Index<Node> termIdIndex = im.forNodes(TermConstants.INDEX_NAME);
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
			assertTrue(srcIdList.contains("TERM1"));
			assertTrue(srcIdList.contains("TERM2"));
			assertTrue(srcIdList.contains("TERM3"));

			tx.success();
		}
	}

	@Test
	public void testImportTermMultipleTimes() throws Exception {
		List<ImportTerm> terms;
		TermManager ftm;
		terms = new ArrayList<>();
		ImportFacet importFacet;
		ftm = new TermManager();
		ImportTermAndFacet importTermAndFacet;

		terms.add(new ImportTerm("name0", "source0"));
		terms.add(new ImportTerm("name1", "source1", (String) null, null, Lists.newArrayList("source0")));
		// we have to specify the sources, otherwise there will be multiple
		// inserts
		terms.get(0).source = "TEST_SOURCE";
		terms.get(1).source = "TEST_SOURCE";
		importFacet = FacetManagerTest.getImportFacet();
		importTermAndFacet = new ImportTermAndFacet(terms, importFacet);
		ftm.insertFacetTerms(graphDb, JsonSerializer.toJson(importTermAndFacet));
		// Insert another time.
		ftm.insertFacetTerms(graphDb, JsonSerializer.toJson(importTermAndFacet));

		try (Transaction tx = graphDb.beginTx()) {
			Index<Node> index = graphDb.index().forNodes(TermConstants.INDEX_NAME);
			// Would throw an exception if there were multiple terms found.
			index.get(PROP_SRC_IDS, "source0").getSingle();
			index.get(PROP_SRC_IDS, "source1").getSingle();
		}
	}

	@Test
	public void testImportEdgeMultipleTimes() throws Exception {
		List<ImportTerm> terms;
		TermManager ftm;
		terms = new ArrayList<>();
		ImportFacet importFacet;
		ftm = new TermManager();
		ImportTermAndFacet importTermAndFacet;

		terms.add(new ImportTerm("name0", "source0"));
		terms.add(new ImportTerm("name1", "source1", (String) null, null, Lists.newArrayList("source0")));
		// we have to specify the sources, otherwise there will be multiple
		// inserts
		terms.get(0).source = "TEST_SOURCE";
		terms.get(1).source = "TEST_SOURCE";
		importFacet = FacetManagerTest.getImportFacet();
		importTermAndFacet = new ImportTermAndFacet(terms, importFacet);
		ftm.insertFacetTerms(graphDb, JsonSerializer.toJson(importTermAndFacet));
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, TermManager.TermLabel.TERM, PROP_ID,
					NodeIDPrefixConstants.TERM + 1);
			Node n0 = NodeUtilities.getSingleOtherNode(n1, TermManager.EdgeTypes.IS_BROADER_THAN);
			assertEquals(NodeIDPrefixConstants.TERM + 0, n0.getProperty(PROP_ID));
		}

		terms.clear();
		terms.add(new ImportTerm("name0", "source0"));
		terms.add(new ImportTerm("name1", "source1", (String) null, null, Lists.newArrayList("source0")));
		// we have to specify the sources, otherwise there will be multiple
		// inserts
		terms.get(0).source = "TEST_SOURCE";
		terms.get(1).source = "TEST_SOURCE";
		importFacet = FacetManagerTest.getImportFacet();
		importTermAndFacet = new ImportTermAndFacet(terms, importFacet);
		ftm.insertFacetTerms(graphDb, JsonSerializer.toJson(importTermAndFacet));
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, TermManager.TermLabel.TERM, PROP_ID,
					NodeIDPrefixConstants.TERM + 1);
			Node n0 = NodeUtilities.getSingleOtherNode(n1, TermManager.EdgeTypes.IS_BROADER_THAN);
			assertEquals(NodeIDPrefixConstants.TERM + 0, n0.getProperty(PROP_ID));
		}

		terms.clear();
		terms.add(new ImportTerm("name1", "source1", (String) null, null, Lists.newArrayList("source0")));
		importFacet = FacetManagerTest.getImportFacet();
		importTermAndFacet = new ImportTermAndFacet(terms, importFacet);
		ftm.insertFacetTerms(graphDb, JsonSerializer.toJson(importTermAndFacet));
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, TermManager.TermLabel.TERM, PROP_ID,
					NodeIDPrefixConstants.TERM + 1);
			Node n0 = NodeUtilities.getSingleOtherNode(n1, TermManager.EdgeTypes.IS_BROADER_THAN);
			assertEquals(NodeIDPrefixConstants.TERM + 0, n0.getProperty(PROP_ID));
		}

	}

	/**
	 * "Matching facet" means that in this test, there is a facet - and the test
	 * terms belong to that facet - that has the label "USE_FOR_SUGGESTIONS".
	 * 
	 * @throws JSONException
	 */
	@Test
	public void testPushAllTermsToSetMatchingFacet() throws JSONException {
		// Some value initialization...
		int numTerms = 50;
		String setName = "PENDING_FOR_SUGGESTIONS";
		String facetLabelUseForSuggestions = "USE_FOR_SUGGESTIONS";
		String termLabelDoNotUseForSuggestions = "DO_NOT_USE_FOR_SUGGESTIONS";

		ImportTermAndFacet testTerms = getTestTerms(numTerms + 2);
		// We set for two terms the general properties
		// "do not use for suggestions", "do not use for query dictionary"
		// that should be excluded then.
		testTerms.terms.get(numTerms).addGeneralLabel(termLabelDoNotUseForSuggestions);
		testTerms.terms.get(numTerms + 1).addGeneralLabel(termLabelDoNotUseForSuggestions);
		// We do set the correct property to the test facet so that the test
		// method call to pushAllTermsToSet() may refer to this property as a
		// constraint.
		// Map<String, Object> facetMap = (Map<String, Object>)
		// testTerms.get("facet");
		ImportFacet facetMap = testTerms.facet;
		facetMap.addGeneralLabel(facetLabelUseForSuggestions);
		// facetMap.put(NodeConstants.PROP_GENERAL_LABELS, new String[] {
		// facetLabelUseForSuggestions });
		Gson gson = new Gson();
		String termsAndFacetBytes = gson.toJson(testTerms);
		TermManager ftm = new TermManager();
		ftm.insertFacetTerms(graphDb, termsAndFacetBytes);

		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterator<Node> it = GlobalGraphOperations.at(graphDb)
					.getAllNodesWithLabel(DynamicLabel.label(setName)).iterator();
			assertFalse("No nodes with the label", it.hasNext());

			// Now push the terms into the "pending for suggestion" set.
			PushTermsToSetCommand cmd = new PushTermsToSetCommand(setName);
			cmd.eligibleTermDefinition = cmd.new TermSelectionDefinition();
			cmd.eligibleTermDefinition.facetLabel = facetLabelUseForSuggestions;
			cmd.excludeTermDefinition = cmd.new TermSelectionDefinition();
			cmd.excludeTermDefinition.termLabel = termLabelDoNotUseForSuggestions;
			ftm.pushTermsToSet(graphDb, JsonSerializer.toJson(cmd), -1);

			// And check whether they have been successfully added to the set.
			it = GlobalGraphOperations.at(graphDb).getAllNodesWithLabel(DynamicLabel.label(setName)).iterator();
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
	 */
	@Test
	public void testPushAllTermstoSetNoMatchingFacet() throws JSONException {
		int numTerms = 10;
		// It doesn't really matter what exact value we have here as we test
		// against a facet WITHOUT that value ;-)
		String facetLabelUseForSuggestions = "USE_FOR_SUGGESTIONS";
		String setName = "PENDING_FOR_SUGGESTIONS";

		ImportTermAndFacet testTerms = getTestTerms(numTerms);
		Gson gson = new Gson();
		String termsAndFacetBytes = gson.toJson(testTerms);
		TermManager ftm = new TermManager();
		ftm.insertFacetTerms(graphDb, termsAndFacetBytes);

		// Now try to push the terms into the "pending for suggestion" set -
		// which should have no effect because there is no facet with the
		// general property to be used for suggestions.
		PushTermsToSetCommand cmd = new PushTermsToSetCommand(setName);
		cmd.eligibleTermDefinition = cmd.new TermSelectionDefinition(FacetConstants.PROP_GENERAL_LABELS,
				facetLabelUseForSuggestions);
		ftm.pushTermsToSet(graphDb, JsonSerializer.toJson(cmd), -1);

		// And check whether they have been successfully added to the set.
		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterator<Node> it = GlobalGraphOperations.at(graphDb)
					.getAllNodesWithLabel(DynamicLabel.label(setName)).iterator();
			assertFalse("There should be no nodes with the label", it.hasNext());
			tx.success();
		}

	}

	@Test
	public void testPushAllTermsToSetWithoutFacetPropertyConstraint() throws JSONException {
		int numTerms = 10;

		String setName = "PENDING_FOR_SUGGESTIONS";
		ImportTermAndFacet testTerms = getTestTerms(numTerms);
		Gson gson = new Gson();
		String termsAndFacetBytes = gson.toJson(testTerms);
		TermManager ftm = new TermManager();
		ftm.insertFacetTerms(graphDb, termsAndFacetBytes);

		// Now try to push the terms into the "pending for suggestion" set
		// without any restrictions.
		PushTermsToSetCommand cmd = new PushTermsToSetCommand(setName);
		ftm.pushTermsToSet(graphDb, JsonSerializer.toJson(cmd), -1);

		// And check whether they have been successfully added to the set.
		int termCount = countNodesWithLabel(DynamicLabel.label(setName));
		assertEquals("All terms have been returned", numTerms, termCount);

		// Do the same as above but set empty values for the
		// PushTermsToSetCommand. This should be equivalent to
		// "no restrictions".
		setName = "other set";
		cmd = new PushTermsToSetCommand(setName);
		cmd.eligibleTermDefinition = cmd.new TermSelectionDefinition("", "", "", "");
		ftm.pushTermsToSet(graphDb, JsonSerializer.toJson(cmd), -1);

		// And check whether they have been successfully added to the set.
		termCount = countNodesWithLabel(DynamicLabel.label(setName));
		assertEquals("All terms have been returned", numTerms, termCount);
	}

	@Test
	public void testPushAllTermsToSetWithLimit() throws JSONException {
		int numTerms = 100;

		String setName = "PENDING_FOR_SUGGESTIONS";
		ImportTermAndFacet testTerms = getTestTerms(numTerms);
		Gson gson = new Gson();
		String termsAndFacetBytes = gson.toJson(testTerms);
		TermManager ftm = new TermManager();
		ftm.insertFacetTerms(graphDb, termsAndFacetBytes);

		// Now try to push the terms into the "pending for suggestion", but only
		// a part of the terms.
		PushTermsToSetCommand cmd = new PushTermsToSetCommand(setName);
		ftm.pushTermsToSet(graphDb, JsonSerializer.toJson(cmd), 42);

		// And check that only the specified number of terms is returned.
		int termCount = countNodesWithLabel(DynamicLabel.label(setName));
		assertEquals("All terms have been returned", 42, termCount);
	}

	private int countNodesWithLabel(Label label) {
		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterator<Node> it = GlobalGraphOperations.at(graphDb).getAllNodesWithLabel(label).iterator();
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
	public void testPopTermsFromSet() throws JSONException {
		int numPoppedTerms = 17;

		String setName = "PENDING_FOR_SUGGESTIONS";
		// First, import terms using the respective test method.
		testPushAllTermsToSetMatchingFacet();

		int nodesWithLabel = countNodesWithLabel(DynamicLabel.label(setName));

		TermManager ftm = new TermManager();
		RecursiveMappingRepresentation poppedTerms = (RecursiveMappingRepresentation) ftm.popTermsFromSet(graphDb,
				setName, numPoppedTerms);
		List<Node> termList = (List<Node>) poppedTerms.getUnderlyingMap().get(TermManager.RET_KEY_TERMS);

		assertEquals("Number of popped terms", numPoppedTerms, termList.size());

		assertEquals("The returned terms should no longer have the label", nodesWithLabel - numPoppedTerms,
				countNodesWithLabel(DynamicLabel.label(setName)));
	}

	@Test
	public void testOrganizeSynonyms() throws JSONException {
		// In this test, we have a term that specifies its preferred term as a
		// synonym, too. The FacetTermManager should detect this and remove the
		// synonym that equals the preferred name.
		List<Map<String, Object>> termList = new ArrayList<>();
		Map<String, Object> termMap = new HashMap<>();
		termMap.put(PROP_PREF_NAME, "prefname");
		termMap.put(PROP_DESCRIPTIONS, Lists.newArrayList("desc of term"));
		termMap.put(PROP_SRC_IDS, "TERM");
		termMap.put(PROP_SYNONYMS, Lists.newArrayList("prefname", "othersynonym"));
		termList.add(termMap);
		Map<String, Object> termsAndFacet = new HashMap<String, Object>();
		termsAndFacet.put("facet", FacetManagerTest.getImportFacet());
		termsAndFacet.put("terms", termList);

		Gson gson = new Gson();
		String termsJson = gson.toJson(termsAndFacet);
		TermManager ftt = new TermManager();

		ftt.insertFacetTerms(graphDb, termsJson);

		try (Transaction tx = graphDb.beginTx()) {
			Index<Node> termIndex = graphDb.index().forNodes(TermConstants.INDEX_NAME);
			Node term = termIndex.get(PROP_SRC_IDS, "TERM").getSingle();
			assertEquals("Preferred name", "prefname", term.getProperty(PROP_PREF_NAME));
			assertEquals("Description", Lists.newArrayList("desc of term"),
					Arrays.asList((String[]) term.getProperty(PROP_DESCRIPTIONS)));
			assertArrayEquals("Source ID", new String[] { "TERM" }, (String[]) term.getProperty(PROP_SRC_IDS));
			String[] synonyms = (String[]) term.getProperty(PROP_SYNONYMS);
			assertEquals("Number of synonyms", 1, synonyms.length);
			assertEquals("Synonym", "othersynonym", synonyms[0]);
			tx.success();
		}
	}

	@Test
	public void testAdditionalRelationships() throws JSONException {
		// Multiple terms will be inserted that are "equal" according to the
		// preferred names. This will be expressed via
		// additional relationships other then the default taxonomic
		// relationship. Also, we will first insert one part of the equal terms
		// and observe that "hollow" terms are
		// created on behalf of the referenced other half. After inserting
		// this other half, there should be no more "hollow" terms.
		ImportTermAndFacet testTermsAndFacet = getTestTerms(4);
		List<ImportTerm> terms = testTermsAndFacet.terms;
		// Now we just create relationships so that:
		// 0 equals 1
		// 0 equals 3
		// 1 equals 2
		// 2 equals 3
		// 0 2
		// |X|
		// 1 3
		// Note that the test uses "TERM<number>" for source IDs and that we
		// have to define the relationships via source
		// IDs (we don't know the ultimate term IDs yet).
		String termSource = terms.get(0).originalSource;
		terms.get(0).addRelationship(new ImportFacetTermRelationship("TERM" + 1, termSource,
				TermManager.EdgeTypes.HAS_SAME_NAMES.name(), TermConstants.IdType.ORIGINAL_SOURCE));
		terms.get(0).addRelationship(new ImportFacetTermRelationship("TERM" + 3, termSource,
				TermManager.EdgeTypes.HAS_SAME_NAMES.name(), TermConstants.IdType.ORIGINAL_SOURCE));
		terms.get(1).addRelationship(new ImportFacetTermRelationship("TERM" + 2, termSource,
				TermManager.EdgeTypes.HAS_SAME_NAMES.name(), TermConstants.IdType.ORIGINAL_SOURCE));
		terms.get(2).addRelationship(new ImportFacetTermRelationship("TERM" + 3, termSource,
				TermManager.EdgeTypes.HAS_SAME_NAMES.name(), TermConstants.IdType.ORIGINAL_SOURCE));

		// Now we split the terms in two lists in order to check the behavior of
		// the "hollow" label assignment.
		ImportTermAndFacet firstTerms = new ImportTermAndFacet(terms.subList(0, 2), testTermsAndFacet.facet);
		ImportTermAndFacet secondTerms = new ImportTermAndFacet(terms.subList(2, 4), testTermsAndFacet.facet);

		Gson gson = new Gson();
		TermManager ftt = new TermManager();

		// Insert the first half of terms.
		ftt.insertFacetTerms(graphDb, gson.toJson(firstTerms));
		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterator<Node> nodesIt = GlobalGraphOperations.at(graphDb)
					.getAllNodesWithLabel(TermManager.TermLabel.TERM).iterator();
			int nodeCount = 0;
			while (nodesIt.hasNext()) {
				Node node = nodesIt.next();
				// Only the real terms have gotten an ID.
				if (node.hasProperty(PROP_ID))
					assertFalse("Not hollow", node.hasLabel(TermManager.TermLabel.HOLLOW));
				else
					assertTrue("Node is hollow", node.hasLabel(TermManager.TermLabel.HOLLOW));
				nodeCount++;
			}
			assertEquals("Number of terms", 4, nodeCount);
		}

		// Now insert the other half of terms. After this we should still have 4
		// terms, but non of them should be
		// hollow and all should have an ID, a facet and a description.
		ftt.insertFacetTerms(graphDb, gson.toJson(secondTerms));
		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterator<Node> nodesIt = GlobalGraphOperations.at(graphDb)
					.getAllNodesWithLabel(TermManager.TermLabel.TERM).iterator();
			assertEquals(4, ftt.getNumTerms(graphDb));
			while (nodesIt.hasNext()) {
				Node node = nodesIt.next();
				// Only the real terms have gotten an ID.
				assertFalse("Term " + NodeUtilities.getNodePropertiesAsString(node) + " is hollow but it shouldn't be",
						node.hasLabel(TermManager.TermLabel.HOLLOW));
				assertTrue("Has an ID", node.hasProperty(PROP_ID));
				assertTrue("Has a facet", node.hasProperty(PROP_FACETS));
				assertTrue("Has a description", node.hasProperty(PROP_DESCRIPTIONS));

				// Check the correctness of the "equal names" relationships. For
				// simplicity, we restrict ourselves to
				// outgoing relationships to test here.
				Iterator<Relationship> relIt = node
						.getRelationships(Direction.OUTGOING, TermManager.EdgeTypes.HAS_SAME_NAMES).iterator();
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
	public void testAdditionalRelationships2() throws JSONException {
		ImportTermAndFacet testTermsAndFacet = getTestTerms(4);
		List<ImportTerm> terms = testTermsAndFacet.terms;
		String termSource = terms.get(0).originalSource;
		ImportFacetTermRelationship rel1 = new ImportFacetTermRelationship("TERM" + 1, termSource,
				TermManager.EdgeTypes.HAS_SAME_NAMES.name(), TermConstants.IdType.ORIGINAL_SOURCE);
		rel1.addProperty("prop1", "value1");
		rel1.addProperty("prop2", "value2");
		ImportFacetTermRelationship rel2 = new ImportFacetTermRelationship("TERM" + 1, termSource,
				TermManager.EdgeTypes.HAS_SAME_NAMES.name(), TermConstants.IdType.ORIGINAL_SOURCE);
		rel2.addProperty("prop1", "value1");
		rel2.addProperty("prop3", "value3");
		terms.get(0).addRelationship(rel1);
		terms.get(0).addRelationship(rel2);

		TermManager tm = new TermManager();
		tm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTermsAndFacet));
		tm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTermsAndFacet));

		try (Transaction tx = graphDb.beginTx()) {
			Node term0 = graphDb.findNode(TermLabel.TERM, PROP_ID, NodeIDPrefixConstants.TERM + 0);
			Relationship relationship = term0.getSingleRelationship(TermManager.EdgeTypes.HAS_SAME_NAMES,
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
	public void testInsertAggregateTerm() throws JSONException {
		// Here we test the case where an aggregate term is explicitly imported
		// from external data as opposed to
		// computing it within the database.
		ImportTermAndFacet testTerms = getTestTerms(5);
		List<ImportTerm> terms = testTerms.terms;
		// Add an aggregate term with terms 0-3 as elements. The term on
		// position 4 will stay alone.
		List<String> aggregateElementSrcIds = Lists.newArrayList("TERM" + 0, "TERM" + 1, "TERM" + 2, "TERM" + 3);
		List<String> aggregateElementSources = Lists.newArrayList(terms.get(0).source, terms.get(1).source,
				terms.get(2).source, terms.get(3).source);
		List<TermCoordinates> aggregateElementCoords = new ArrayList<>();
		for (int i = 0; i < aggregateElementSrcIds.size(); i++) {
			String id = aggregateElementSrcIds.get(i);
			String source = aggregateElementSources.get(i);
			aggregateElementCoords.add(new TermCoordinates(id, source));
		}
		terms.add(new ImportTerm(aggregateElementCoords,
				Lists.newArrayList(PROP_PREF_NAME, PROP_SYNONYMS, PROP_DESCRIPTIONS)));

		Gson gson = new Gson();
		TermManager ftm = new TermManager();
		ftm.insertFacetTerms(graphDb, gson.toJson(testTerms));

		assertEquals("Number of actual terms", 5, countNodesWithLabel(TermManager.TermLabel.TERM));
		assertEquals("Number of aggregate terms", 1, countNodesWithLabel(TermManager.TermLabel.AGGREGATE));

		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterator<Node> aggregateIt = GlobalGraphOperations.at(graphDb)
					.getAllNodesWithLabel(TermManager.TermLabel.AGGREGATE).iterator();
			assertTrue("There is at least one aggregate term", aggregateIt.hasNext());
			Node aggregate = aggregateIt.next();
			assertFalse("There is no second aggregate term", aggregateIt.hasNext());
			Iterable<Relationship> elementRels = aggregate.getRelationships(TermManager.EdgeTypes.HAS_ELEMENT);
			int numElementRels = 0;
			for (Relationship elementRel : elementRels) {
				String[] termSrcIds = (String[]) elementRel.getEndNode().getProperty(PROP_SRC_IDS);
				assertTrue("Term is one of the defined aggregate elements",
						aggregateElementSrcIds.contains(termSrcIds[0]));
				numElementRels++;
			}
			assertEquals("There are 4 elements", 4, numElementRels);

			Node term = graphDb.index().forNodes(TermConstants.INDEX_NAME).get(PROP_SRC_IDS, "TERM" + 4).getSingle();
			assertNotNull("Term on position 4 was inserted and found", term);
			Iterable<Relationship> relationships = term.getRelationships();
			int numRels = 0;
			for (Relationship rel : relationships) {
				numRels++;
				assertTrue(rel.getType().name().equals(TermManager.EdgeTypes.HAS_ROOT_TERM.toString()));
			}
			assertEquals("Term on position 4 has exactly one relationship (HAS_ROOT)", 1, numRels);
			// Now let's copy the term properties into the aggregate.
			RecursiveMappingRepresentation report = (RecursiveMappingRepresentation) ftm
					.copyAggregateProperties(graphDb);
			Map<String, Object> reportMap = report.getUnderlyingMap();
			assertEquals("Number of aggregates", 1, reportMap.get(TermManager.RET_KEY_NUM_AGGREGATES));
			assertEquals("Number of element terms", 4, reportMap.get(TermManager.RET_KEY_NUM_ELEMENTS));
			assertEquals("Number of copied properties", 4 * 2, reportMap.get(TermManager.RET_KEY_NUM_PROPERTIES));

			assertFalse("Name of the aggregate has been copied",
					StringUtils.isBlank((String) aggregate.getProperty(PROP_PREF_NAME)));
			assertEquals("Descriptions have been copied", 4,
					((String[]) aggregate.getProperty(PROP_DESCRIPTIONS)).length);
			tx.success();
		}
	}

	@Test
	public void testAddAggregateAsHierarchyNode() throws JSONException {
		// Insert an aggregate and check the relationships.

		ImportTermAndFacet testTerms = getTestTerms(2);
		ImportTerm aggregate = new ImportTerm(
				Arrays.asList(new TermCoordinates("TERM0", "TEST_DATA"), new TermCoordinates("TERM1", "TEST_DATA")),
				Arrays.asList(PROP_PREF_NAME));
		aggregate.source = "TEST_TERMS";
		aggregate.sourceId = "testagg";
		aggregate.aggregateIncludeInHierarchy = true;
		testTerms.terms.add(aggregate);
		testTerms.terms.get(0).parentSrcIds = Arrays.asList("testagg");
		testTerms.terms.get(1).parentSrcIds = Arrays.asList("testagg");

		Gson gson = new Gson();
		TermManager ftm = new TermManager();
		ftm.insertFacetTerms(graphDb, gson.toJson(testTerms));

		try (Transaction tx = graphDb.beginTx()) {
			int aggregateCount = countNodesWithLabel(TermLabel.AGGREGATE);
			assertEquals(1, aggregateCount);

			Node aggregateNode = NodeUtilities.getSingleNode(graphDb.findNodes(TermLabel.AGGREGATE));
			assertEquals(2, countRelationships(aggregateNode.getRelationships(EdgeTypes.HAS_ELEMENT)));
			assertEquals(2, countRelationships(aggregateNode.getRelationships(EdgeTypes.IS_BROADER_THAN)));
			assertEquals(1, countRelationships(aggregateNode.getRelationships(EdgeTypes.HAS_ROOT_TERM)));

			Node term0 = graphDb.findNode(TermLabel.TERM, PROP_ID, NodeIDPrefixConstants.TERM + 0);
			Relationship broaderThan = term0.getSingleRelationship(EdgeTypes.IS_BROADER_THAN, Direction.INCOMING);
			assertEquals(aggregateNode, broaderThan.getStartNode());

			Node term1 = graphDb.findNode(TermLabel.TERM, PROP_ID, NodeIDPrefixConstants.TERM + 1);
			broaderThan = term1.getSingleRelationship(EdgeTypes.IS_BROADER_THAN, Direction.INCOMING);
			assertEquals(aggregateNode, broaderThan.getStartNode());
		}
	}

	@Test
	public void testAddAggregateAsHierarchyNode2() throws JSONException {
		// Insert aggregate with an additional label and check that it has been
		// applied.

		ImportTermAndFacet testTerms = getTestTerms(2);
		ImportTerm aggregate = new ImportTerm(
				Arrays.asList(new TermCoordinates("TERM0", "TEST_DATA"), new TermCoordinates("TERM1", "TEST_DATA")),
				Arrays.asList(PROP_PREF_NAME));
		aggregate.source = "TEST_TERMS";
		aggregate.sourceId = "testagg";
		aggregate.aggregateIncludeInHierarchy = true;
		aggregate.generalLabels = Arrays.asList("MY_COOL_AGGREGATE_LABEL");
		testTerms.terms.add(aggregate);
		testTerms.terms.get(0).parentSrcIds = Arrays.asList("testagg");
		testTerms.terms.get(1).parentSrcIds = Arrays.asList("testagg");

		Gson gson = new Gson();
		TermManager ftm = new TermManager();
		ftm.insertFacetTerms(graphDb, gson.toJson(testTerms));

		try (Transaction tx = graphDb.beginTx()) {
			int aggregateCount = countNodesWithLabel(DynamicLabel.label("MY_COOL_AGGREGATE_LABEL"));
			assertEquals(1, aggregateCount);
		}
	}

	@Test
	public void testBuildAggregate() throws JSONException {
		// In this test, the database is scanned for terms with the same name
		// and synonyms. If some are found, an
		// aggregate node containing these nodes as elements will be created.
		// So first of all, let's build the terms. Two will have the exactly
		// same name and synonyms, one will have the
		// same name and no synonyms and one will be completely
		// different. Another two will have same names, without any synonyms.
		List<ImportTerm> terms = new ArrayList<>();
		terms.add(new ImportTerm("name1", "source1", Lists.newArrayList("syn1", "syn2")));
		terms.add(new ImportTerm("name1", "source2", Lists.newArrayList("syn1", "syn2")));
		terms.add(new ImportTerm("name1", "source3"));
		terms.add(new ImportTerm("name2", "source4"));
		terms.add(new ImportTerm("name3", "source5"));
		terms.add(new ImportTerm("name3", "source6"));
		// Lets shuffle the terms to be simulate the situation where terms are
		// added in random order.
		Collections.shuffle(terms);
		ImportFacet importFacet = FacetManagerTest.getImportFacet();
		ImportTermAndFacet importTermAndFacet = new ImportTermAndFacet(terms, importFacet);
		TermManager ftm = new TermManager();
		ftm.insertFacetTerms(graphDb, JsonSerializer.toJson(importTermAndFacet));
		ftm.buildAggregatesByNameAndSynonyms(graphDb, TermConstants.PROP_GENERAL_LABELS,
				"[\"DO_NOT_USE_FOR_SUGGESTIONS\",\"DO_NOT_USE_FOR_QUERY_DICTIONARY\"]");

		// Now let's copy the term properties into the aggregate.
		RecursiveMappingRepresentation report = (RecursiveMappingRepresentation) ftm.copyAggregateProperties(graphDb);
		Map<String, Object> reportMap = report.getUnderlyingMap();
		assertEquals("Number of aggregates", 2, reportMap.get(TermManager.RET_KEY_NUM_AGGREGATES));
		assertEquals("Number of element terms", 5, reportMap.get(TermManager.RET_KEY_NUM_ELEMENTS));
		assertEquals("Number of copied properties", 7, reportMap.get(TermManager.RET_KEY_NUM_PROPERTIES));

		// Check whether the correct terms have been bound together.
		try (Transaction tx = graphDb.beginTx()) {
			TermNameAndSynonymComparator nameAndSynonymComparator = new TermNameAndSynonymComparator();
			ResourceIterable<Node> generalAggregates = GlobalGraphOperations.at(graphDb)
					.getAllNodesWithLabel(TermManager.TermLabel.AGGREGATE);
			for (Node aggregate : generalAggregates) {
				Iterable<Relationship> elementRels = aggregate.getRelationships(TermManager.EdgeTypes.HAS_ELEMENT);
				// Count relationships and store elements for comparison whether
				// they are actually equal named.
				int numRels = 0;
				List<Node> elements = new ArrayList<>();
				for (Relationship rel : elementRels) {
					Node term = rel.getEndNode();
					elements.add(term);
					List<String> generalLabels = Lists
							.newArrayList((String[]) term.getProperty(TermConstants.PROP_GENERAL_LABELS));
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
				String prefName = (String) aggregate.getProperty(TermConstants.PROP_PREF_NAME);
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
		ImportTermAndFacet testTerms;
		ImportTermAndFacet testTerms1;
		ImportTermAndFacet testTerms2;
		ImportTermAndFacet testTerms3;
		ImportTermAndFacet testTerms4;
		TermManager ftm = new TermManager();
		// Create terms in DIFFERENT facets we will then map to each other
		testTerms = getTestTerms(1, 0);
		testTerms1 = getTestTerms(1, 1);
		testTerms2 = getTestTerms(1, 2);
		testTerms3 = getTestTerms(1, 3);
		testTerms4 = getTestTerms(1, 4);
		testTerms.facet.name = "facet0";
		testTerms1.facet.name = "facet1";
		testTerms2.facet.name = "facet2";
		testTerms3.facet.name = "facet3";
		testTerms4.facet.name = "facet4";
		testTerms1.terms.get(0).parentSrcIds = Lists.newArrayList("TERM0");
		testTerms2.terms.get(0).parentSrcIds = Lists.newArrayList("TERM1");
		testTerms3.terms.get(0).parentSrcIds = Lists.newArrayList("TERM2");
		testTerms4.terms.get(0).parentSrcIds = Lists.newArrayList("TERM3");
		ftm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms));
		ftm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms1));
		ftm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms2));
		ftm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms3));
		ftm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms4));

		List<ImportMapping> mappings = Lists.newArrayList(new ImportMapping("TERM0", "TERM1", "loom"));

		int numInsertedRels = ftm.insertMappings(graphDb, JsonSerializer.toJson(mappings));
		assertEquals(1, numInsertedRels);

		TermAggregateBuilder.buildAggregatesForMappings(graphDb, Sets.newHashSet("loom"), TermLabel.TERM,
				TermLabel.AGGREGATE);

		try (Transaction tx = graphDb.beginTx()) {
			// first check if everything is alright, we expect on aggregate with
			// two elements
			Node aggregate = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, TermLabel.AGGREGATE, PROP_ID,
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
			RecursiveMappingRepresentation response = (RecursiveMappingRepresentation) ftm.getChildrenOfTerms(graphDb,
					"[" + NodeIDPrefixConstants.AGGREGATE_TERM + "0]", TermLabel.AGGREGATE.name());
			@SuppressWarnings("unchecked")
			Map<String, Object> relAndChildMap = (Map<String, Object>) response.getUnderlyingMap()
					.get(NodeIDPrefixConstants.AGGREGATE_TERM + 0);
			assertEquals(2, ((Map<?, ?>) relAndChildMap.get(TermManager.RET_KEY_RELTYPES)).size());
			assertEquals(2, ((Set<?>) relAndChildMap.get(TermManager.RET_KEY_CHILDREN)).size());
		}
	}

	@Test
	public void testHollowParent() throws JSONException {
		// In this test, we will add a node with a single parent. However, we
		// won't add the parent itself, thus creating
		// a "hollow" parent node.
		// In a second step, we will add the former hollow term explicitly
		// together with another parent. We should see
		// how there are then now hollow terms left and the new
		// "grandparent" will become the root.
		ImportFacet importFacet = FacetManagerTest.getImportFacet();
		List<ImportTerm> terms = new ArrayList<>();
		terms.add(new ImportTerm("prefname1", "srcid1", "desc1", Lists.newArrayList("syn1"),
				Lists.newArrayList("parentid1")));
		// we have to specify the sources, otherwise there will be multiple
		// inserts
		terms.get(0).source = "TEST_SOURCE";
		ImportTermAndFacet termAndFacet = new ImportTermAndFacet(terms, importFacet);
		// Allow hollow parents:
		termAndFacet.importOptions = new ImportOptions();
		termAndFacet.importOptions.createHollowParents = true;
		TermManager ftm = new TermManager();
		RecursiveMappingRepresentation report = (RecursiveMappingRepresentation) ftm.insertFacetTerms(graphDb,
				JsonSerializer.toJson(termAndFacet));
		Map<String, Object> reportMap = report.getUnderlyingMap();
		assertEquals("Number of inserted terms", 2, reportMap.get(TermManager.RET_KEY_NUM_CREATED_TERMS));
		assertEquals("Number of inserted relationships", 3, reportMap.get(TermManager.RET_KEY_NUM_CREATED_RELS));

		try (Transaction tx = graphDb.beginTx()) {
			Iterable<Node> allNodes = GlobalGraphOperations.at(graphDb)
					.getAllNodesWithLabel(TermManager.TermLabel.TERM);
			int numNodes = 0;
			int numHollow = 0;
			int numRoots = 0;
			for (Node n : allNodes) {
				numNodes++;
				Iterable<Relationship> rels = n.getRelationships();
				int numRootTermRels = 0;
				int numBroaderRels = 0;
				for (Relationship rel : rels) {
					if (rel.getType().name().equals(TermManager.EdgeTypes.HAS_ROOT_TERM.name())) {
						numRootTermRels++;
						numRoots++;
					} else if (rel.getType().name().equals(TermManager.EdgeTypes.IS_BROADER_THAN.name())) {
						numBroaderRels++;
					}
				}
				if (n.hasLabel(TermManager.TermLabel.HOLLOW)) {
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
		terms.add(new ImportTerm("prefname2", "parentid1", "desc2", Lists.newArrayList("syn2"),
				Lists.newArrayList("parentid2")));
		terms.add(new ImportTerm("prefname3", "parentid2"));
		// we have to specify the sources, otherwise there will be multiple
		// inserts
		terms.get(0).source = "TEST_SOURCE";
		terms.get(1).source = "TEST_SOURCE";
		// We need to replace the facet definition by the ID of the already
		// created facet because a new facet will be
		// created otherwise.
		termAndFacet.facet = new ImportFacet("fid0");

		ftm.insertFacetTerms(graphDb, JsonSerializer.toJson(termAndFacet));

		try (Transaction tx = graphDb.beginTx()) {
			Iterable<Node> allNodes = GlobalGraphOperations.at(graphDb)
					.getAllNodesWithLabel(TermManager.TermLabel.TERM);
			int numNodes = 0;
			int numHollow = 0;
			int numRoots = 0;
			for (Node n : allNodes) {
				numNodes++;
				Iterable<Relationship> rels = n.getRelationships();
				int numRootTermRels = 0;
				int numBroaderRels = 0;
				for (Relationship rel : rels) {
					if (rel.getType().name().equals(TermManager.EdgeTypes.HAS_ROOT_TERM.name())) {
						numRootTermRels++;
						numRoots++;
					} else if (rel.getType().name().equals(TermManager.EdgeTypes.IS_BROADER_THAN.name())) {
						numBroaderRels++;
					}
				}
				if (n.hasLabel(TermManager.TermLabel.HOLLOW))
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
		terms.add(new ImportTerm("prefname2", "parentid1", "desc2", Lists.newArrayList("syn2"),
				Lists.newArrayList("parentid42")));
		terms.add(new ImportTerm("prefname5", "parentid8", "desc2", Lists.newArrayList("syn2"),
				Lists.newArrayList("parentid42")));
		termAndFacet.facet = new ImportFacet("fid0");

		ftm.insertFacetTerms(graphDb, JsonSerializer.toJson(termAndFacet));

		try (Transaction tx = graphDb.beginTx()) {
			Index<Node> index = graphDb.index().forNodes(TermConstants.INDEX_NAME);
			// Would throw an exception if there were multiple terms found.
			Node hollowParent = index.get(PROP_SRC_IDS, "parentid42").getSingle();
			assertNotNull(hollowParent);
			assertTrue(hollowParent.hasLabel(TermManager.TermLabel.HOLLOW));
		}
	}

	@Test
	public void testNonFacetGroupCommand() throws JSONException {
		// We will test whether it works correctly to sort terms into the
		// "non-facet" facet group according to a
		// particular criterium, e.g. "no parent".
		// Terms 0 and 1 will get a parent and thus should be treated normally.
		// Terms 2 and 3 don't get a parent and should become "no-facet" terms.
		ImportTermAndFacet testTerms = getTestTerms(4);
		ImportOptions options = new ImportOptions();
		testTerms.importOptions = options;
		AddToNonFacetGroupCommand cmd = new AddToNonFacetGroupCommand();
		cmd.addParentCriterium(AddToNonFacetGroupCommand.ParentCriterium.NO_PARENT);
		options.noFacetCmd = cmd;
		List<ImportTerm> terms = testTerms.terms;
		// Here, we just give any parent so that the non-facet-group-command
		// does not trigger.
		List<String> parentSrcIds = Lists.newArrayList("nonExistingParent");
		terms.get(0).parentSrcIds = parentSrcIds;
		terms.get(1).parentSrcIds = parentSrcIds;
		options.cutParents = parentSrcIds;
		// We activate this so we can test parent cutting appropriately. If it
		// would be off, the child terms of
		// non-existing parents get to be the facet roots no matter what.
		options.createHollowParents = true;

		TermManager ftm = new TermManager();
		ftm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms));

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
			Iterator<Relationship> termRelIt = noFacet.getRelationships(TermManager.EdgeTypes.HAS_ROOT_TERM).iterator();
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
			Iterator<Relationship> facetTerms = facet.getRelationships(TermManager.EdgeTypes.HAS_ROOT_TERM).iterator();
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
		List<ImportTerm> terms = new ArrayList<>();
		terms.add(new ImportTerm("name1", "source1"));
		terms.add(new ImportTerm("name2", "source2"));
		terms.add(new ImportTerm("name3", "source3", (String) null, null, Lists.newArrayList("source1", "source2")));
		terms.add(new ImportTerm("name4", "source4", (String) null, null, Lists.newArrayList("source3")));
		terms.add(new ImportTerm("name5", "source5", (String) null, null, Lists.newArrayList("source3")));
		terms.add(new ImportTerm("name6", "source6", (String) null, null, Lists.newArrayList("source5")));
		// terms.add(new ImportFacetTerm("name1", "source0"));
		// terms.add(new ImportFacetTerm("name2", "source1"));
		// terms.add(new ImportFacetTerm("name3", "source2", null, null,
		// Lists.newArrayList("source0")));
		// terms.add(new ImportFacetTerm("name4", "source3", null, null,
		// Lists.newArrayList("source0")));
		// terms.add(new ImportFacetTerm("name5", "source4", null, null,
		// Lists.newArrayList("source2", "source3")));
		// terms.add(new ImportFacetTerm("name6", "source5", null, null,
		// Lists.newArrayList("source4")));
		ImportFacet importFacet = FacetManagerTest.getImportFacet();
		ImportTermAndFacet importTermAndFacet = new ImportTermAndFacet(terms, importFacet);
		TermManager ftm = new TermManager();
		ftm.insertFacetTerms(graphDb, JsonSerializer.toJson(importTermAndFacet));

		Gson gson = new Gson();
		RecursiveMappingRepresentation pathsFromFacetroots;
		List<String[]> paths;

		pathsFromFacetroots = (RecursiveMappingRepresentation) ftm.getPathsFromFacetroots(graphDb,
				gson.toJson(Lists.newArrayList("source1", "source3")), TermConstants.PROP_SRC_IDS, false, "");
		paths = (List<String[]>) pathsFromFacetroots.getUnderlyingMap().get(TermManager.RET_KEY_PATHS);
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
				assertTrue(path.length == 2);
				// First element is one of the roots
				assertTrue(path[0].equals(NodeIDPrefixConstants.TERM + 0)
						|| path[0].equals(NodeIDPrefixConstants.TERM + 1));
				// Second element is source3
				assertEquals(NodeIDPrefixConstants.TERM + 2, path[1]);
			}
		}
		assertEquals("Wrong number of paths", 3, paths.size());

		pathsFromFacetroots = (RecursiveMappingRepresentation) ftm.getPathsFromFacetroots(graphDb,
				gson.toJson(Lists.newArrayList("source4", "source6")), TermConstants.PROP_SRC_IDS, false, "");
		paths = (List<String[]>) pathsFromFacetroots.getUnderlyingMap().get(TermManager.RET_KEY_PATHS);
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
		List<ImportTerm> terms = new ArrayList<>();
		terms.add(new ImportTerm("name1", "source1"));
		terms.add(new ImportTerm("name2", "source2", (String) null, null, Lists.newArrayList("source1")));
		terms.add(new ImportTerm("name3", "source3", (String) null, null, Lists.newArrayList("source2")));
		terms.add(new ImportTerm("name4", "source4", (String) null, null, Lists.newArrayList("source3")));
		terms.add(new ImportTerm("name5", "source5", (String) null, null, Lists.newArrayList("source4")));
		for (ImportTerm term : terms)
			term.source = "TEST_SOURCE";
		ImportFacet importFacet = FacetManagerTest.getImportFacet();
		ImportTermAndFacet importTermAndFacet = new ImportTermAndFacet(terms, importFacet);
		TermManager ftm = new TermManager();
		ftm.insertFacetTerms(graphDb, JsonSerializer.toJson(importTermAndFacet));

		// Now we insert two terms in another facet that would open another path
		// from source5 to source1 if we would
		// follow the general IS_BROADER_THAN relationship instead of the
		// facet-specific relationship. Note that source5
		// is added a second time which will create the relationship to source
		// 6, thus:
		// source5 --> source6 --> source1 and that similarly source1 was added
		// as a root term for the new facet.
		terms = new ArrayList<>();
		terms.add(new ImportTerm("name1", "source1"));
		terms.add(new ImportTerm("name6", "source6", (String) null, null, Lists.newArrayList("source1")));
		terms.add(new ImportTerm("name5", "source5", (String) null, null, Lists.newArrayList("source6")));
		for (ImportTerm term : terms)
			term.source = "TEST_SOURCE";
		importFacet = FacetManagerTest.getImportFacet();
		importFacet.name = "otherfacet";
		importTermAndFacet = new ImportTermAndFacet(terms, importFacet);
		ftm.insertFacetTerms(graphDb, JsonSerializer.toJson(importTermAndFacet));

		Gson gson = new Gson();
		RecursiveMappingRepresentation pathsFromFacetroots;
		List<String[]> paths;

		pathsFromFacetroots = (RecursiveMappingRepresentation) ftm.getPathsFromFacetroots(graphDb,
				gson.toJson(Lists.newArrayList("source5")), TermConstants.PROP_SRC_IDS, false, "");
		paths = (List<String[]>) pathsFromFacetroots.getUnderlyingMap().get(TermManager.RET_KEY_PATHS);
		for (String[] path : paths) {
			// We expect two paths, both chains that have been defined by the
			// two inserts above:
			// [tid0, tid1, tid2, tid3, tid4]
			// [tid0, tid5, tid4]
			assertTrue(5 == path.length || 3 == path.length);
		}
		assertEquals("Wrong number of paths", 2, paths.size());

		pathsFromFacetroots = (RecursiveMappingRepresentation) ftm.getPathsFromFacetroots(graphDb,
				gson.toJson(Lists.newArrayList("source5")), TermConstants.PROP_SRC_IDS, false, "fid0");
		paths = (List<String[]>) pathsFromFacetroots.getUnderlyingMap().get(TermManager.RET_KEY_PATHS);
		assertEquals("Wrong number of paths", 1, paths.size());
		assertArrayEquals(new String[] { "tid0", "tid1", "tid2", "tid3", "tid4" }, paths.get(0));

		pathsFromFacetroots = (RecursiveMappingRepresentation) ftm.getPathsFromFacetroots(graphDb,
				gson.toJson(Lists.newArrayList("source5")), TermConstants.PROP_SRC_IDS, false, "fid1");
		paths = (List<String[]>) pathsFromFacetroots.getUnderlyingMap().get(TermManager.RET_KEY_PATHS);
		assertEquals("Wrong number of paths", 1, paths.size());
		assertArrayEquals(new String[] { "tid0", "tid5", "tid4" }, paths.get(0));

	}

	@Test
	public void testUpdateChildrenInformation() throws Exception {
		// In this test we check whether the array property of in which facet a
		// term has children is computed correctly.
		// For this, we do two term imports to create two facets. We will have
		// four terms where the latter three are
		// children of the first.
		// Thus, this first term should have children in both facets.
		ImportTermAndFacet testTerms;
		TermManager ftm = new TermManager();
		testTerms = getTestTerms(2);
		testTerms.terms.get(1).parentSrcIds = Lists.newArrayList("TERM0");
		ftm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms));

		// Insert terms for the second facet. We have to adjust some values here
		// to actually get four terms and not just
		// two because the duplicate source IDs would be detected.
		testTerms = getTestTerms(2);
		testTerms.facet.name = "secondfacet";
		testTerms.terms.get(0).sourceId = "TERM2";
		testTerms.terms.get(1).sourceId = "TERM3";
		testTerms.terms.get(0).parentSrcIds = Lists.newArrayList("TERM0");
		testTerms.terms.get(1).parentSrcIds = Lists.newArrayList("TERM0");
		ftm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms));

		ftm.updateChildrenInformation(graphDb);

		try (Transaction tx = graphDb.beginTx()) {
			Node rootNode = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, TermLabel.TERM, PROP_ID,
					NodeIDPrefixConstants.TERM + 0);
			List<String> facetsWithChildren = Lists
					.newArrayList((String[]) rootNode.getProperty(TermConstants.PROP_CHILDREN_IN_FACETS));
			assertTrue(facetsWithChildren.contains(NodeIDPrefixConstants.FACET + 0));
			assertTrue(facetsWithChildren.contains(NodeIDPrefixConstants.FACET + 1));
			assertEquals(new Integer(2), new Integer(facetsWithChildren.size()));
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testGetTermChildren() throws Exception {
		ImportTermAndFacet testTerms;
		TermManager ftm = new TermManager();
		testTerms = getTestTerms(5);
		testTerms.terms.get(1).parentSrcIds = Lists.newArrayList("TERM0");
		testTerms.terms.get(2).parentSrcIds = Lists.newArrayList("TERM0");
		testTerms.terms.get(3).parentSrcIds = Lists.newArrayList("TERM0");
		testTerms.terms.get(4).parentSrcIds = Lists.newArrayList("TERM3");
		ftm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms));

		try (Transaction tx = graphDb.beginTx()) {
			// Get the children of a single node.
			RecursiveMappingRepresentation childrenRepr = (RecursiveMappingRepresentation) ftm
					.getChildrenOfTerms(graphDb, "[\"tid0\"]]", null);
			Map<String, Object> childMap = childrenRepr.getUnderlyingMap();
			// We asked for only one node, thus there should be one result
			assertEquals(new Integer(1), new Integer(childMap.size()));
			Map<String, Object> term0Children = (Map<String, Object>) childMap.get(NodeIDPrefixConstants.TERM + 0);
			// This result divides into two elements, namely the relationship
			// type mapping and the nodes themselves.
			assertEquals(new Integer(2), new Integer(term0Children.size()));
			Map<String, Object> reltypeMap = (Map<String, Object>) term0Children.get(TermManager.RET_KEY_RELTYPES);
			assertEquals(new Integer(3), new Integer(reltypeMap.size()));
			assertTrue(reltypeMap.keySet().contains(NodeIDPrefixConstants.TERM + 1));
			assertTrue(reltypeMap.keySet().contains(NodeIDPrefixConstants.TERM + 2));
			assertTrue(reltypeMap.keySet().contains(NodeIDPrefixConstants.TERM + 3));
			Set<Node> children = (Set<Node>) term0Children.get(TermManager.RET_KEY_CHILDREN);
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
			childrenRepr = (RecursiveMappingRepresentation) ftm.getChildrenOfTerms(graphDb,
					"[\"tid0\",\"tid3\",\"tid3\"]]", null);
			childMap = childrenRepr.getUnderlyingMap();
			// We asked for three nodes' children, however two were equal thus
			// there should be two elements.
			assertEquals(new Integer(2), new Integer(childMap.size()));

		}
	}

	@Test
	public void testAddMappings() throws Exception {
		ImportTermAndFacet testTerms;
		ImportTermAndFacet testTerms1;
		ImportTermAndFacet testTerms2;
		ImportTermAndFacet testTerms3;
		ImportTermAndFacet testTerms4;
		TermManager ftm = new TermManager();
		// Create terms in DIFFERENT facets we will then map to each other
		testTerms = getTestTerms(1, 0);
		testTerms1 = getTestTerms(1, 1);
		testTerms2 = getTestTerms(1, 2);
		testTerms3 = getTestTerms(1, 3);
		testTerms4 = getTestTerms(1, 4);
		testTerms.facet.name = "facet0";
		testTerms1.facet.name = "facet1";
		testTerms2.facet.name = "facet2";
		testTerms3.facet.name = "facet3";
		testTerms4.facet.name = "facet4";
		testTerms1.terms.get(0).parentSrcIds = Lists.newArrayList("TERM0");
		testTerms2.terms.get(0).parentSrcIds = Lists.newArrayList("TERM1");
		testTerms3.terms.get(0).parentSrcIds = Lists.newArrayList("TERM2");
		testTerms4.terms.get(0).parentSrcIds = Lists.newArrayList("TERM3");
		ftm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms));
		ftm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms1));
		ftm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms2));
		ftm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms3));
		ftm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms4));

		List<ImportMapping> mappings = Lists.newArrayList(new ImportMapping("TERM0", "TERM1", "loom"),
				new ImportMapping("TERM3", "TERM2", "same_uris"));

		int numInsertedRels = ftm.insertMappings(graphDb, JsonSerializer.toJson(mappings));
		assertEquals(2, numInsertedRels);

		try (Transaction tx = graphDb.beginTx()) {
			Node term0 = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, TermManager.TermLabel.TERM, PROP_ID,
					NodeIDPrefixConstants.TERM + 0);
			Iterable<Relationship> relationships = term0.getRelationships(TermManager.EdgeTypes.IS_MAPPED_TO);
			int relCounter = 0;
			for (Relationship rel : relationships) {
				Node otherNode = rel.getOtherNode(term0);
				assertEquals(NodeIDPrefixConstants.TERM + 1, otherNode.getProperty(PROP_ID));
				relCounter++;
			}
			assertEquals(1, relCounter);

			Node term1 = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, TermManager.TermLabel.TERM, PROP_ID,
					NodeIDPrefixConstants.TERM + 1);
			relationships = term1.getRelationships(TermManager.EdgeTypes.IS_MAPPED_TO);
			relCounter = 0;
			for (Relationship rel : relationships) {
				Node otherNode = rel.getOtherNode(term1);
				assertEquals(NodeIDPrefixConstants.TERM + 0, otherNode.getProperty(PROP_ID));
				relCounter++;
			}
			assertEquals(1, relCounter);
		}

		// Check that there are no duplicates created.
		numInsertedRels = ftm.insertMappings(graphDb, JsonSerializer.toJson(mappings));
		assertEquals(0, numInsertedRels);

		// Even if we add a mapping with a new type between two classes that
		// already are mapped, there shouldn't be
		// created a new relationship.
		// We add the exactly same mapping twice, again to assure that
		// duplicates are avoided.
		// We also add a mapping where one term does not exist.
		mappings = Lists.newArrayList(new ImportMapping("TERM0", "TERM1", "new_type"),
				new ImportMapping("TERM0", "TERM1", "new_type"),
				new ImportMapping("TERM_DOES_NOT_EXIST", "TERM1", "new_type"));
		numInsertedRels = ftm.insertMappings(graphDb, JsonSerializer.toJson(mappings));
		assertEquals(0, numInsertedRels);
		// But the relationship now should know both mapping types.
		try (Transaction tx = graphDb.beginTx()) {
			Node term0 = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, TermManager.TermLabel.TERM, PROP_ID,
					NodeIDPrefixConstants.TERM + 0);
			Iterable<Relationship> relationships = term0.getRelationships(TermManager.EdgeTypes.IS_MAPPED_TO);
			int relCounter = 0;
			for (Relationship rel : relationships) {
				Node otherNode = rel.getOtherNode(term0);
				assertEquals(NodeIDPrefixConstants.TERM + 1, otherNode.getProperty(PROP_ID));
				String[] mappingTypes = (String[]) rel.getProperty(TermRelationConstants.PROP_MAPPING_TYPE);
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
		ImportTermAndFacet testTerms;
		TermManager ftm = new TermManager();
		// Create terms in THE SAME facets we will then map to each other
		testTerms = getTestTerms(5);
		testTerms.terms.get(1).parentSrcIds = Lists.newArrayList("TERM0");
		testTerms.terms.get(2).parentSrcIds = Lists.newArrayList("TERM1");
		testTerms.terms.get(3).parentSrcIds = Lists.newArrayList("TERM2");
		testTerms.terms.get(4).parentSrcIds = Lists.newArrayList("TERM3");
		ftm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms));

		List<ImportMapping> mappings = Lists.newArrayList(new ImportMapping("TERM0", "TERM1", "loom"),
				new ImportMapping("TERM3", "TERM2", "same_uris"));

		// We have a loom mapping between TERM0 and TERM1 which should be
		// filtered out since both terms appear in the same facet.
		int numInsertedRels = ftm.insertMappings(graphDb, JsonSerializer.toJson(mappings));
		assertEquals(1, numInsertedRels);
	}

	@Test
	public void testMergeTerms() throws Exception {
		// Here, we will insert some terms as normal. Then, we will insert some
		// terms anew, with new property values
		// that should then be merged. We won't define a new facet, we just want
		// to add new information to existing
		// terms.
		ImportTermAndFacet testTerms = getTestTerms(4);
		// add a fifth term that has the first term as a parent
		testTerms.terms.add(new ImportTerm("someterm", "somesrcid", (List<String>) null, (List<String>) null,
				Lists.newArrayList("TERM0")));
		testTerms.terms.get(testTerms.terms.size() - 1).source = "somesource";
		TermManager tm = new TermManager();
		// first insert.
		tm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms));

		// now again get two test terms. Those will be identical to the first
		// two test terms above.
		testTerms = getTestTerms(2);
		// IMPORTANT: this is the key in this test: We do NOT insert a facet
		// which should be not necessary because we
		// only insert terms that are already in the database.
		testTerms.facet = null;
		// Give new property values to the first term, leave the second
		// unchanged.
		testTerms.terms.get(0).generalLabels = Lists.newArrayList("newlabel1");
		testTerms.terms.get(0).descriptions = Lists.newArrayList("newdescription1");
		testTerms.terms.get(0).prefName = "newprefname1";
		// re-insert the additional term from above but with a description and a
		// synonym, without a parent; we should
		// not need it since the term is already known.
		ImportTerm term = new ImportTerm("somesrcid");
		term.descriptions = Lists.newArrayList("newdesc2");
		term.synonyms = Lists.newArrayList("newsynonym2");
		term.source = "somesource";
		testTerms.terms.add(term);
		// second insert, duplicate terms should now be merged.
		tm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms));

		// Test the success of the merging.
		try (Transaction tx = graphDb.beginTx()) {
			// See that we really only have a single facet, we havn't added a
			// second one.
			ResourceIterable<Node> facets = GlobalGraphOperations.at(graphDb)
					.getAllNodesWithLabel(FacetManager.FacetLabel.FACET);
			int facetCounter = 0;
			for (@SuppressWarnings("unused")
			Node facet : facets) {
				facetCounter++;
			}
			assertEquals(1, facetCounter);

			// Now test that the merged-in properties - and labels - are
			// actually there.
			Node mergedTerm = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, DynamicLabel.label("newlabel1"),
					PROP_ID, "tid0");
			// have we found the term? Then it got its new label.
			assertNotNull(mergedTerm);
			List<String> descriptions = Lists.newArrayList((String[]) mergedTerm.getProperty(PROP_DESCRIPTIONS));
			assertTrue(descriptions.contains("desc of term0"));
			assertTrue(descriptions.contains("newdescription1"));

			Node facet = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, FacetManager.FacetLabel.FACET,
					FacetConstants.PROP_ID, "fid0");
			Iterable<Relationship> rootTermRelations = facet.getRelationships(TermManager.EdgeTypes.HAS_ROOT_TERM);
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
		term.put(PROP_SRC_IDS, "testSrcId");
		term.put("someIntegerProperty", 42);
		term.put("someIntegerArrayProperty", new Integer[] { 23, 98 });
		terms.add(term);

		Map<String, Object> termsMap = new HashMap<>();
		termsMap.put(TermManager.KEY_FACET, FacetManagerTest.getImportFacet());
		termsMap.put(TermManager.KEY_TERMS, terms);

		TermManager tm = new TermManager();
		tm.insertFacetTerms(graphDb, JsonSerializer.toJson(termsMap));

		try (Transaction tx = graphDb.beginTx()) {
			Node termNode = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, TermManager.TermLabel.TERM, PROP_ID,
					NodeIDPrefixConstants.TERM + 0);
			assertEquals(42, termNode.getProperty("someIntegerProperty"));
			// the order should have been maintained
			assertArrayEquals(new int[] { 23, 98 }, (int[]) termNode.getProperty("someIntegerArrayProperty"));
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testGetAllFacetRoots() throws Exception {
		TermManager tm = new TermManager();
		ImportTermAndFacet testTerms = getTestTerms(3);
		tm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms));
		// Insert two times so we have two facets
		testTerms.facet.name = "secondfacet";
		tm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms));
		RecursiveMappingRepresentation facetRoots = (RecursiveMappingRepresentation) tm
				.getFacetRoots(graphDb,
						JsonSerializer.toJson(
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
		TermManager tm = new TermManager();
		ImportTermAndFacet testTerms = getTestTerms(3);
		tm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms));
		// Insert two times so we have two facets
		tm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms));
		RecursiveMappingRepresentation facetRoots = (RecursiveMappingRepresentation) tm
				.getFacetRoots(graphDb,
						JsonSerializer.toJson(
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
		TermManager tm = new TermManager();
		ImportTermAndFacet testTerms = getTestTerms(3);
		// Insert three times so we have three facets
		tm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms));
		testTerms.facet.name = "secondfacet";
		tm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms));
		testTerms.facet.name = "thirdfacet";
		tm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms));

		Map<String, List<String>> requestedRoots = new HashMap<>();
		requestedRoots.put(NodeIDPrefixConstants.FACET + 0, Lists.newArrayList(NodeIDPrefixConstants.TERM + 0));
		requestedRoots.put(NodeIDPrefixConstants.FACET + 1,
				Lists.newArrayList(NodeIDPrefixConstants.TERM + 1, NodeIDPrefixConstants.TERM + 2));
		// for the third facet, we want all roots returned

		RecursiveMappingRepresentation facetRoots = (RecursiveMappingRepresentation) tm.getFacetRoots(graphDb,
				JsonSerializer.toJson(Lists.newArrayList(NodeIDPrefixConstants.FACET + 0,
						NodeIDPrefixConstants.FACET + 1, NodeIDPrefixConstants.FACET + 2)),
				JsonSerializer.toJson(requestedRoots), 0);
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
	// ImportTermAndFacet testTerms = getTestTerms(1);
	// // add a synonym for which equivalent writing variants should be removed
	// testTerms.terms.get(0).synonyms = Arrays.asList("beclin 1");
	// TermManager tm = new TermManager();
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
	// Node term = graphDb.findNode(TermLabel.TERM, PROP_ID,
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
		ImportTermAndFacet facet1 = getTestTerms(1);
		ImportTermAndFacet facet2 = getTestTerms(1);

		TermManager tm = new TermManager();
		tm.insertFacetTerms(graphDb, JsonSerializer.toJson(facet1));
		tm.insertFacetTerms(graphDb, JsonSerializer.toJson(facet2));

		try (Transaction tx = graphDb.beginTx()) {
			assertEquals(1, countNodesWithLabel(FacetLabel.FACET));
		}
	}

	@Test
	public void testAddWritingVariants() throws Exception {
		{
			ImportTermAndFacet testTerms = getTestTerms(1);
			TermManager tm = new TermManager();
			tm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms));

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
			tm.addWritingVariants(graphDb, JsonSerializer.toJson(variantCountsByTermIdPerDoc), null);
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
			ImportTermAndFacet testTerms = getTestTerms(1);
			TermManager tm = new TermManager();
			tm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms));

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
			tm.addWritingVariants(graphDb, JsonSerializer.toJson(variantCountsByTermIdPerDoc), null);
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
	public void testUniqueSourceIds() throws JSONException {
		ImportTermAndFacet testTerms = getTestTerms(2);
		// first, remove original ID and source because otherwise the original ID checks will take over
		testTerms.terms.get(0).originalId = null;
		testTerms.terms.get(0).originalSource = null;
		testTerms.terms.get(1).originalId = null;
		testTerms.terms.get(1).originalSource = null;
		
		// now set terms that should be equal by source ID despite having a differenz source
		testTerms.terms.get(0).uniqueSourceId = true;
		testTerms.terms.get(0).sourceId = "id0";
		testTerms.terms.get(0).source = "source0";
		testTerms.terms.get(1).uniqueSourceId = true;
		testTerms.terms.get(1).sourceId = "id0";
		testTerms.terms.get(1).source = "source1";
		
		TermManager tm = new TermManager();
		tm.insertFacetTerms(graphDb, JsonSerializer.toJson(testTerms));
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
	public static ImportTermAndFacet getTestTerms(int amount) {
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
	public static ImportTermAndFacet getTestTerms(int amount, int startAt) {
		List<ImportTerm> termList = new ArrayList<>(amount);
		for (int i = startAt; i < amount + startAt; i++) {
			ImportTerm term = new ImportTerm("prefname" + i, "TERM" + i, "desc of term" + i);
			term.originalId = term.sourceId;
			term.originalSource = "TEST_DATA";
			term.source = "TEST_DATA";
			termList.add(term);
		}

		return new ImportTermAndFacet(termList, FacetManagerTest.getImportFacet());
	}

}
