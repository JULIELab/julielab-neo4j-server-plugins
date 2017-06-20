package de.julielab.neo4j.plugins;

import static de.julielab.neo4j.plugins.constants.semedico.FacetConstants.FACET_FIELD_PREFIX;
import static de.julielab.neo4j.plugins.constants.semedico.FacetConstants.FACET_GROUP;
import static de.julielab.neo4j.plugins.constants.semedico.FacetConstants.PROP_CSS_ID;
import static de.julielab.neo4j.plugins.constants.semedico.FacetConstants.PROP_FILTER_FIELD_NAMES;
import static de.julielab.neo4j.plugins.constants.semedico.FacetConstants.PROP_POSITION;
import static de.julielab.neo4j.plugins.constants.semedico.FacetConstants.PROP_SEARCH_FIELD_NAMES;
import static de.julielab.neo4j.plugins.constants.semedico.FacetConstants.PROP_SOURCE_NAME;
import static de.julielab.neo4j.plugins.constants.semedico.FacetConstants.PROP_SOURCE_TYPE;
import static de.julielab.neo4j.plugins.constants.semedico.FacetConstants.SRC_TYPE_HIERARCHICAL;
import static de.julielab.neo4j.plugins.constants.semedico.NodeConstants.PROP_ID;
import static de.julielab.neo4j.plugins.constants.semedico.NodeConstants.PROP_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.shell.util.json.JSONException;
import org.neo4j.shell.util.json.JSONObject;

import com.google.common.collect.Lists;
import com.google.gson.Gson;

import de.julielab.neo4j.plugins.auxiliaries.NodeUtilities;
import de.julielab.neo4j.plugins.auxiliaries.RecursiveMappingRepresentation;
import de.julielab.neo4j.plugins.constants.semedico.FacetConstants;
import de.julielab.neo4j.plugins.constants.semedico.FacetGroupConstants;
import de.julielab.neo4j.plugins.constants.semedico.NodeConstants;
import de.julielab.neo4j.plugins.constants.semedico.NodeIDPrefixConstants;
import de.julielab.neo4j.plugins.constants.semedico.TermConstants;
import de.julielab.neo4j.plugins.datarepresentation.ImportFacet;
import de.julielab.neo4j.plugins.datarepresentation.ImportFacetGroup;
import de.julielab.neo4j.plugins.datarepresentation.ImportTerm;
import de.julielab.neo4j.plugins.datarepresentation.ImportTermAndFacet;
import de.julielab.neo4j.plugins.datarepresentation.JsonSerializer;
import de.julielab.neo4j.plugins.test.TestUtilities;

public class FacetManagerTest {
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
	public void testCreateFacetGroup() throws SecurityException, NoSuchMethodException,
			IllegalArgumentException, IllegalAccessException, InvocationTargetException,
			JSONException {
		Gson gson = new Gson();

		Map<String, Object> facetGroupMap1 = new HashMap<String, Object>();
		facetGroupMap1.put(FacetGroupConstants.PROP_NAME, "Test Facet Group");
		facetGroupMap1.put(FacetGroupConstants.PROP_POSITION, 1);
		facetGroupMap1.put(FacetGroupConstants.PROP_GENERAL_LABELS,
				Lists.newArrayList("showForBTerms"));
		// facetGroupMap1.put(FacetGroupConstants.PROP_SHOW_FOR_SEARCH, false);
		String facetGroupJsonString1 = gson.toJson(facetGroupMap1);
		JSONObject jsonFacetGroup1 = new JSONObject(facetGroupJsonString1);

		Map<String, Object> facetGroupMap2 = new HashMap<String, Object>();
		facetGroupMap2.put(FacetGroupConstants.PROP_NAME, "Test Facet Group 2");
		facetGroupMap2.put(FacetGroupConstants.PROP_POSITION, 2);
		facetGroupMap2.put(FacetGroupConstants.PROP_GENERAL_LABELS,
				Lists.newArrayList("showForSearch"));
		String facetGroupJsonString2 = gson.toJson(facetGroupMap2);
		JSONObject jsonFacetGroup2 = new JSONObject(facetGroupJsonString2);

		FacetManager fm = new FacetManager();
		Method createFacetGroupMethod = FacetManager.class.getDeclaredMethod("createFacetGroup",
				GraphDatabaseService.class, Node.class, JSONObject.class);
		createFacetGroupMethod.setAccessible(true);
		try (Transaction tx = graphDb.beginTx()) {
			Node facetGroupsNode = FacetManager.getFacetGroupsNode(graphDb);
			Node facetGroupNode = (Node) createFacetGroupMethod.invoke(fm, graphDb,
					facetGroupsNode, jsonFacetGroup1);
			assertEquals("fgid0", facetGroupNode.getProperty(FacetGroupConstants.PROP_ID));
			assertEquals("Test Facet Group",
					facetGroupNode.getProperty(FacetGroupConstants.PROP_NAME));
			assertEquals(1, facetGroupNode.getProperty(FacetGroupConstants.PROP_POSITION));
			// List<String> properties = Lists.newArrayList((String[])
			// facetGroupNode
			// .getProperty(FacetGroupConstants.PROP_GENERAL_LABELS));
			// assertTrue(properties.contains("showForBTerms"));
			// assertFalse(properties.contains("showForSearch"));
			assertTrue(facetGroupNode.hasLabel(DynamicLabel.label("showForBTerms")));
			assertFalse(facetGroupNode.hasLabel(DynamicLabel.label("showForSearch")));

			Node facetGroupNode2 = (Node) createFacetGroupMethod.invoke(fm, graphDb,
					facetGroupsNode, jsonFacetGroup2);
			assertEquals("fgid1", facetGroupNode2.getProperty(TermConstants.PROP_ID));
			assertEquals("Test Facet Group 2", facetGroupNode2.getProperty(TermConstants.PROP_NAME));
			assertEquals(2, facetGroupNode2.getProperty(FacetGroupConstants.PROP_POSITION));
			// List<String> properties2 = Lists.newArrayList((String[])
			// facetGroupNode2
			// .getProperty(FacetGroupConstants.PROP_GENERAL_LABELS));
			// assertFalse(properties2.contains("showForBTerms"));
			// assertTrue(properties2.contains("showForSearch"));
			assertFalse(facetGroupNode2.hasLabel(DynamicLabel.label("showForBTerms")));
			assertTrue(facetGroupNode2.hasLabel(DynamicLabel.label("showForSearch")));

			// Check whether the new facet group node is correctly connected. It
			// should be:
			// facetGroups --> facetGroup["Test Facets"]
			Relationship facetGroupRel = facetGroupNode.getSingleRelationship(
					FacetManager.EdgeTypes.HAS_FACET_GROUP, Direction.INCOMING);
			assertEquals(facetGroupsNode, facetGroupRel.getStartNode());
			assertEquals("facetGroups", facetGroupsNode.getProperty(PROP_NAME));
			tx.success();
		}

	}

	@Test
	public void testGetFacetGroupsNode() {
		try (Transaction tx = graphDb.beginTx()) {
			assertNull("In the beginning, there was no facet groups node",
					NodeUtilities.findSingleNodeByLabelAndProperty(graphDb,
							NodeConstants.Labels.ROOT, PROP_NAME, FacetConstants.NAME_FACET_GROUPS));
			Node facetGroupsNode = FacetManager.getFacetGroupsNode(graphDb);
			assertNotNull("One facet groups node should be there.", facetGroupsNode);
			// Get the node multiple times to make sure it isn't created twice.
			facetGroupsNode = FacetManager.getFacetGroupsNode(graphDb);
			facetGroupsNode = FacetManager.getFacetGroupsNode(graphDb);
			facetGroupsNode = FacetManager.getFacetGroupsNode(graphDb);
			facetGroupsNode = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb,
					NodeConstants.Labels.ROOT, PROP_NAME, FacetConstants.NAME_FACET_GROUPS);
			// graphDb.index().forNodes(NodeConstants.INDEX_ROOT_NODES)
			// .get(PROP_NAME, FacetConstants.NAME_FACET_GROUPS).getSingle();
			assertNotNull("There is one facet groups node", facetGroupsNode);
			// it =
			// graphDb.findNodesByLabelAndProperty(NodeConstants.Labels.UNIQUE,
			// PROP_NAME, FacetConstants.NAME_FACET_GROUPS).iterator();
			// assertTrue("There is one facet groups node", it.hasNext());
			// it.next();
			// assertFalse("There are no more than one facet groups nodes",
			// it.hasNext());
			tx.success();
		}
	}

	@Test
	public void testCreateMinimalFacet() throws JSONException {
		Map<String, Object> facetGroupMap = new HashMap<String, Object>();
		facetGroupMap.put(FacetGroupConstants.PROP_NAME, "facetGroup1");
		facetGroupMap.put(FacetGroupConstants.PROP_POSITION, 1);
		facetGroupMap.put(FacetGroupConstants.PROP_GENERAL_LABELS,
				Lists.newArrayList("showForSearch"));

		Map<String, Object> facetMap = new HashMap<String, Object>();
		facetMap.put(PROP_NAME, "testfacet1");
		facetMap.put(PROP_CSS_ID, "cssid1");
		facetMap.put(PROP_SOURCE_TYPE, SRC_TYPE_HIERARCHICAL);
		facetMap.put(PROP_SEARCH_FIELD_NAMES, new String[] { "sf1", "sf2" });
		facetMap.put(PROP_POSITION, 1);
		facetMap.put(FACET_GROUP, facetGroupMap);

		Gson gson = new Gson();
		String jsonFacetString = gson.toJson(facetMap);
		JSONObject jsonFacet = new JSONObject(jsonFacetString);

		Node facet = FacetManager.createFacet(graphDb, jsonFacet);
		assertNotNull(facet);
	}

	// @Test
	// public void testCreateFacetWithGeneralProperties() throws Exception {
	// Map<String, Object> facetGroupMap = new HashMap<String, Object>();
	// facetGroupMap.put(FacetGroupConstants.PROP_NAME, "facetGroup1");
	// facetGroupMap.put(FacetGroupConstants.PROP_POSITION, 1);
	// facetGroupMap.put(FacetGroupConstants.PROP_GENERAL_LABELS,
	// Lists.newArrayList("showForSearch"));
	//
	// Map<String, Object> facetMap = new HashMap<String, Object>();
	// facetMap.put(PROP_NAME, "testfacet1");
	// facetMap.put(PROP_CSS_ID, "cssid1");
	// facetMap.put(PROP_SOURCE_TYPE, SRC_TYPE_HIERARCHICAL);
	// facetMap.put(PROP_SEARCH_FIELD_NAMES, new String[] { "sf1", "sf2" });
	// facetMap.put(PROP_POSITION, 1);
	// facetMap.put(FACET_GROUP, facetGroupMap);
	// facetMap.put(FacetConstants.PROP_PROPERTIES,
	// "{\"key1\":\"value1\",\"key2\":\"value2\"}");
	//
	// Gson gson = new Gson();
	// String jsonFacetString = gson.toJson(facetMap);
	// JSONObject jsonFacet = new JSONObject(jsonFacetString);
	//
	// Node facet = FacetManager.createFacet(graphDb, jsonFacet);
	// try (Transaction tx = graphDb.beginTx()) {
	// assertNotNull(facet);
	// assertEquals("value1", facet.getProperty("key1"));
	// assertEquals("value2", facet.getProperty("key2"));
	// tx.success();
	// }
	// }

	@Test
	public void testCreateFacetWithSourceName() throws JSONException {
		ImportFacet facetMap = getImportFacet();
		facetMap.sourceName = "facetTermsAuthors";
		// facetMap.put(PROP_SOURCE_NAME, "facetTermsAuthors");

		Gson gson = new Gson();
		String jsonFacetString = gson.toJson(facetMap);
		JSONObject jsonFacet = new JSONObject(jsonFacetString);

		Node facet = FacetManager.createFacet(graphDb, jsonFacet);
		try (Transaction tx = graphDb.beginTx()) {
			assertNotNull(facet);
			assertEquals("Facet source name", "facetTermsAuthors",
					facet.getProperty(PROP_SOURCE_NAME));
			tx.success();
		}
	}

	@Test
	public void testCreateFacet() throws JSONException {
		ImportFacet facetMap = getTestFacetMap(1);
		facetMap.uniqueLabels = Lists.newArrayList("uniqueLabel1", "uniqueLabel2");
		Gson gson = new Gson();
		String jsonFacetString = gson.toJson(facetMap);
		JSONObject jsonFacet = new JSONObject(jsonFacetString);

		// Check whether the facet itself has been created correctly.
		Node facet = FacetManager.createFacet(graphDb, jsonFacet);
		try (Transaction tx = graphDb.beginTx()) {
			assertEquals("testfacet1", facet.getProperty(PROP_NAME));
			assertEquals("cssid1", facet.getProperty(PROP_CSS_ID));
			assertEquals(0, facet.getProperty(PROP_POSITION));
			assertEquals(FACET_FIELD_PREFIX + NodeIDPrefixConstants.FACET + "0",
					facet.getProperty(PROP_SOURCE_NAME));
			List<String> searchFields = Lists.newArrayList((String[]) facet
					.getProperty(PROP_SEARCH_FIELD_NAMES));
			assertTrue(searchFields.contains("sf11"));
			assertTrue(searchFields.contains("sf21"));
			List<String> filterFields = Lists.newArrayList((String[]) facet
					.getProperty(PROP_FILTER_FIELD_NAMES));
			assertTrue(filterFields.contains("ff11"));
			assertTrue(filterFields.contains("ff21"));
			assertTrue(facet.hasLabel(DynamicLabel.label("hidden")));
			assertTrue(facet.hasLabel(DynamicLabel.label("uniqueLabel1")));
			assertTrue(facet.hasLabel(DynamicLabel.label("uniqueLabel2")));

			// Check whether the connection to the facet group node is as
			// expected.
			Relationship hasFacetRel = facet.getSingleRelationship(
					FacetManager.EdgeTypes.HAS_FACET, Direction.INCOMING);
			Node facetGroupNode = hasFacetRel.getStartNode();
			assertEquals("facetGroup1", facetGroupNode.getProperty(PROP_NAME));
			assertEquals(1, facetGroupNode.getProperty(FacetGroupConstants.PROP_POSITION));
			assertTrue(facetGroupNode.hasLabel(DynamicLabel.label("showForSearch")));
			Relationship hasFacetGroupRel = facetGroupNode.getSingleRelationship(
					FacetManager.EdgeTypes.HAS_FACET_GROUP, Direction.INCOMING);
			Node facetGroupsNode = hasFacetGroupRel.getStartNode();
			assertEquals("facetGroups", facetGroupsNode.getProperty(PROP_NAME));
			tx.success();
		}

		// Let's see what happens when we create a second facet. There should be
		// two facets connected to a single facetGroups node.
		facetMap = getTestFacetMap(2);
		jsonFacetString = gson.toJson(facetMap);
		jsonFacet = new JSONObject(jsonFacetString);
		FacetManager.createFacet(graphDb, jsonFacet);
		try (Transaction tx = graphDb.beginTx()) {
			Node facetGroups = FacetManager.getFacetGroupsNode(graphDb);
			Node facetGroup1 = facetGroups.getSingleRelationship(
					FacetManager.EdgeTypes.HAS_FACET_GROUP, Direction.OUTGOING).getEndNode();
			Iterable<Relationship> facetRels = facetGroup1.getRelationships(
					FacetManager.EdgeTypes.HAS_FACET, Direction.OUTGOING);
			Iterator<Relationship> facetIt = facetRels.iterator();
			// check if the expected facets have been found; order is not
			// important (and - most of all - not maintained by Neo4j!)
			Set<String> expectedFacets = new HashSet<>();
			expectedFacets.add("testfacet1");
			expectedFacets.add("testfacet2");
			assertTrue("There should be a first facet.", facetIt.hasNext());
			Node facet1 = facetIt.next().getEndNode();
			assertTrue(expectedFacets.remove(facet1.getProperty(PROP_NAME)));
			assertTrue("There should be a second facet.", facetIt.hasNext());
			Node facet2 = facetIt.next().getEndNode();
			assertTrue(expectedFacets.remove(facet2.getProperty(PROP_NAME)));
			assertTrue(expectedFacets.isEmpty());
			tx.success();
		}

	}

	@Test
	public void testInsertFacets() throws JSONException, SecurityException, NoSuchFieldException,
			IllegalArgumentException, IllegalAccessException {
		List<ImportFacet> jsonFacets = new ArrayList<>();
		// List<Map<String, Object>> jsonFacets = new ArrayList<Map<String,
		// Object>>();
		ImportFacet facetMap = getTestFacetMap(1);
		jsonFacets.add(facetMap);

		facetMap = getTestFacetMap(2);
		jsonFacets.add(facetMap);

		Gson gson = new Gson();
		String jsonFacetsString = gson.toJson(jsonFacets);

		FacetManager fm = new FacetManager();
		ListRepresentation response = fm.insertFacets(graphDb, jsonFacetsString);
		Iterable<Representation> content = TestUtilities.getListFromListRepresentation(response);

		Iterator<Representation> it = content.iterator();
		RecursiveMappingRepresentation facetResponse = (RecursiveMappingRepresentation) it.next();
		assertEquals(NodeIDPrefixConstants.FACET + "0",
				facetResponse.getUnderlyingMap().get(PROP_ID));
		assertEquals("testfacet1", facetResponse.getUnderlyingMap().get(PROP_NAME));

		facetResponse = (RecursiveMappingRepresentation) it.next();
		assertEquals(NodeIDPrefixConstants.FACET + "1",
				facetResponse.getUnderlyingMap().get(PROP_ID));
		assertEquals("testfacet2", facetResponse.getUnderlyingMap().get(PROP_NAME));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testGetFacets() throws JSONException {
		// To be able to get facets, we first have to insert a few.
		List<ImportFacet> facetMaps = new ArrayList<>();
		facetMaps.add(getTestFacetMap(1));
		facetMaps.add(getTestFacetMap(2));
		facetMaps.add(getTestFacetMap(3));
		facetMaps.add(getTestFacetMap(4));
		facetMaps.add(getTestFacetMap(5));

		FacetManager fm = new FacetManager();
		Gson gson = new Gson();
		fm.insertFacets(graphDb, gson.toJson(facetMaps));

		// Additionally, we insert a few terms so we later can tell these terms
		// have NOT
		// been returned by the get facets method.
		// Each facet requires at least one term to be returned by
		// FacetManager#getFacets. I.e. the last facet should
		// not be returned since we don't add terms for it.

		ImportTermAndFacet termAndFacet0 = new ImportTermAndFacet(
				Lists.newArrayList(new ImportTerm("prefname", "TERM")), new ImportFacet(
						NodeIDPrefixConstants.FACET + "0"));
		ImportTermAndFacet termAndFacet1 = new ImportTermAndFacet(
				Lists.newArrayList(new ImportTerm("prefname1", "TERM1")), new ImportFacet(
						NodeIDPrefixConstants.FACET + "1"));
		ImportTermAndFacet termAndFacet2 = new ImportTermAndFacet(
				Lists.newArrayList(new ImportTerm("prefname2", "TERM2")), new ImportFacet(
						NodeIDPrefixConstants.FACET + "2"));
		ImportTermAndFacet termAndFacet3 = new ImportTermAndFacet(
				Lists.newArrayList(new ImportTerm("prefname3", "TERM3")), new ImportFacet(
						NodeIDPrefixConstants.FACET + "3"));

		TermManager ftm = new TermManager();
		ftm.insertFacetTerms(graphDb, JsonSerializer.toJson(termAndFacet0));
		ftm.insertFacetTerms(graphDb, JsonSerializer.toJson(termAndFacet1));
		ftm.insertFacetTerms(graphDb, JsonSerializer.toJson(termAndFacet2));
		ftm.insertFacetTerms(graphDb, JsonSerializer.toJson(termAndFacet3));

		// Get the facets and check that everything is alright.
		RecursiveMappingRepresentation facetRep = (RecursiveMappingRepresentation) fm.getFacets(
				graphDb, false);
		Map<String, Object> underlyingMap = facetRep.getUnderlyingMap();
		// There should be one element, viz. "facetGroups".
		assertEquals(1, underlyingMap.size());
		// Lets check the facet group itself.
		List<Map<String, Object>> facetGroups = (List<Map<String, Object>>) underlyingMap
				.get("facetGroups");
		// Now we should have a list of facet groups where each facet groups has
		// its list of facets. In this test, there should be only one facet
		// group.
		assertEquals(1, facetGroups.size());
		// Get this one facet group and check its values.
		Map<String, Object> facetGroup = facetGroups.get(0);
		assertEquals("facetGroup1", facetGroup.get(FacetGroupConstants.PROP_NAME));
		assertEquals(1, facetGroup.get(FacetGroupConstants.PROP_POSITION));

		// Now check whether all facets are present.
		try (Transaction tx = graphDb.beginTx()) {

			List<Node> facets = (List<Node>) facetGroup.get("facets");
			assertEquals(4, facets.size());
			Node facet = facets.get(0);
			assertEquals("testfacet4", facet.getProperty(PROP_NAME));
			facet = facets.get(1);
			assertEquals("testfacet3", facet.getProperty(PROP_NAME));
			facet = facets.get(2);
			assertEquals("testfacet2", facet.getProperty(PROP_NAME));
			facet = facets.get(3);
			assertEquals("testfacet1", facet.getProperty(PROP_NAME));
			tx.success();
		}
	}

	@Test
	public void testGetFacetSize() throws Exception {
		int amount = 10;
		String facet = "fid0";

		ImportTermAndFacet terms = TermManagerTest.getTestTerms(amount);
		TermManager termManager = new TermManager();
		termManager.insertFacetTerms(graphDb, JsonSerializer.toJson(terms));
		FacetManager facetManager = new FacetManager();

		assertEquals(amount, facetManager.getFacetSize(graphDb, facet));
	}

	public static ImportFacet getImportFacet() {
		return getTestFacetMap(1);
	}

	public static ImportFacet getTestFacetMap(int n) {

		ImportFacetGroup facetGroup = new ImportFacetGroup("facetGroup1", 1,
				Lists.newArrayList("showForSearch"));
		ImportFacet testFacet = new ImportFacet("testfacet" + n, "cssid" + n,
				SRC_TYPE_HIERARCHICAL, Lists.newArrayList("sf1" + n, "sf2" + n),
				Lists.newArrayList("ff1" + n, "ff2" + n), n - 1, Lists.newArrayList("hidden"),
				facetGroup);
		return testFacet;

		// Map<String, Object> facetGroupMap = new HashMap<String, Object>();
		// facetGroupMap.put(FacetGroupConstants.PROP_NAME, "facetGroup1");
		// facetGroupMap.put(FacetGroupConstants.PROP_POSITION, 1);
		// facetGroupMap.put(FacetGroupConstants.PROP_GENERAL_LABELS,
		// Lists.newArrayList("showForSearch"));
		// // facetGroupMap.put(FacetGroupConstants.PROP_SHOW_FOR_BTERMS,
		// false);
		// // facetGroupMap.put(FacetGroupConstants.PROP_SHOW_FOR_SEARCH, true);
		//
		// Map<String, Object> facetMap = new HashMap<String, Object>();
		// facetMap.put(PROP_NAME, "testfacet" + n);
		// facetMap.put(PROP_CSS_ID, "cssid" + n);
		// facetMap.put(PROP_SOURCE_TYPE, SRC_TYPE_HIERARCHICAL);
		// facetMap.put(PROP_SEARCH_FIELD_NAMES, new String[] { "sf1" + n, "sf2"
		// + n });
		// facetMap.put(PROP_FILTER_FIELD_NAMES, new String[] { "ff1" + n, "ff2"
		// + n });
		// facetMap.put(PROP_POSITION, n - 1);
		// facetMap.put(PROP_GENERAL_LABELS, new String[] { "hidden" });
		// facetMap.put(FACET_GROUP, facetGroupMap);
		// return facetMap;
	}

	@AfterClass
	public static void shutdown() {
		graphDb.shutdown();
	}
}
