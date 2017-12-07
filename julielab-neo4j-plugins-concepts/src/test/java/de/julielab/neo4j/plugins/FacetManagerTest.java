package de.julielab.neo4j.plugins;

import static de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants.FACET_GROUP;
import static de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants.PROP_SOURCE_TYPE;
import static de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants.SRC_TYPE_HIERARCHICAL;
import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_ID;
import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.neo4j.server.rest.repr.RecursiveMappingRepresentation;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.shell.util.json.JSONException;
import org.neo4j.shell.util.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.gson.Gson;

import de.julielab.neo4j.plugins.auxiliaries.NodeUtilities;
import de.julielab.neo4j.plugins.datarepresentation.ConceptCoordinates;
import de.julielab.neo4j.plugins.datarepresentation.CoordinateType;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcept;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;
import de.julielab.neo4j.plugins.datarepresentation.ImportFacet;
import de.julielab.neo4j.plugins.datarepresentation.ImportFacetGroup;
import de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetGroupConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.NodeIDPrefixConstants;
import de.julielab.neo4j.plugins.datarepresentation.util.ConceptsJsonSerializer;
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
		facetGroupMap1.put(FacetGroupConstants.PROP_LABELS,
				Lists.newArrayList("showForBTerms"));
		// facetGroupMap1.put(FacetGroupConstants.PROP_SHOW_FOR_SEARCH, false);
		String facetGroupJsonString1 = gson.toJson(facetGroupMap1);
		JSONObject jsonFacetGroup1 = new JSONObject(facetGroupJsonString1);

		Map<String, Object> facetGroupMap2 = new HashMap<String, Object>();
		facetGroupMap2.put(FacetGroupConstants.PROP_NAME, "Test Facet Group 2");
		facetGroupMap2.put(FacetGroupConstants.PROP_POSITION, 2);
		facetGroupMap2.put(FacetGroupConstants.PROP_LABELS,
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
			assertEquals("fgid1", facetGroupNode2.getProperty(ConceptConstants.PROP_ID));
			assertEquals("Test Facet Group 2", facetGroupNode2.getProperty(ConceptConstants.PROP_NAME));
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
		facetGroupMap.put(FacetGroupConstants.PROP_LABELS,
				Lists.newArrayList("showForSearch"));

		Map<String, Object> facetMap = new HashMap<String, Object>();
		facetMap.put(PROP_NAME, "testfacet1");
		facetMap.put("cssId", "cssid1");
		facetMap.put(PROP_SOURCE_TYPE, SRC_TYPE_HIERARCHICAL);
		facetMap.put("searchFieldNames", new String[] { "sf1", "sf2" });
		facetMap.put("position", 1);
		facetMap.put(FACET_GROUP, facetGroupMap);

		Gson gson = new Gson();
		String jsonFacetString = gson.toJson(facetMap);
		JSONObject jsonFacet = new JSONObject(jsonFacetString);

		Node facet = FacetManager.createFacet(graphDb, jsonFacet);
		assertNotNull(facet);
	}

	@Test
	public void testCreateFacetWithSourceName() throws JSONException {
		ImportFacet facetMap = getImportFacet();

		Gson gson = new Gson();
		String jsonFacetString = gson.toJson(facetMap);
		JSONObject jsonFacet = new JSONObject(jsonFacetString);
		jsonFacet.put("sourceName", "facetTermsAuthors");

		Node facet = FacetManager.createFacet(graphDb, jsonFacet);
		try (Transaction tx = graphDb.beginTx()) {
			assertNotNull(facet);
			assertEquals("Facet source name", "facetTermsAuthors",
					facet.getProperty("sourceName"));
			tx.success();
		}
	}

	@Test
	public void testCreateFacet() throws JSONException {
		ImportFacet facetMap = getTestFacetMap(1);
		facetMap.setLabels(Lists.newArrayList("uniqueLabel1", "uniqueLabel2"));
		Gson gson = new Gson();
		String jsonFacetString = gson.toJson(facetMap);
		JSONObject jsonFacet = new JSONObject(jsonFacetString);

		// Check whether the facet itself has been created correctly.
		Node facet = FacetManager.createFacet(graphDb, jsonFacet);
		try (Transaction tx = graphDb.beginTx()) {
			assertEquals("testfacet1", facet.getProperty(PROP_NAME));
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

		ImportConcepts termAndFacet0 = new ImportConcepts(
				Lists.newArrayList(new ImportConcept("prefname", new ConceptCoordinates("TERM", "TEST_DATA", CoordinateType.SRC))), new ImportFacet(
						NodeIDPrefixConstants.FACET + "0"));
		ImportConcepts termAndFacet1 = new ImportConcepts(
				Lists.newArrayList(new ImportConcept("prefname", new ConceptCoordinates("TERM1", "TEST_DATA", CoordinateType.SRC))), new ImportFacet(
						NodeIDPrefixConstants.FACET + "1"));
		ImportConcepts termAndFacet2 = new ImportConcepts(
				Lists.newArrayList(new ImportConcept("prefname", new ConceptCoordinates("TERM2", "TEST_DATA", CoordinateType.SRC))), new ImportFacet(
						NodeIDPrefixConstants.FACET + "2"));
		ImportConcepts termAndFacet3 = new ImportConcepts(
				Lists.newArrayList(new ImportConcept("prefname", new ConceptCoordinates("TERM3", "TEST_DATA", CoordinateType.SRC))), new ImportFacet(
						NodeIDPrefixConstants.FACET + "3"));

		ConceptManager ftm = new ConceptManager();
		ftm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(termAndFacet0));
		ftm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(termAndFacet1));
		ftm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(termAndFacet2));
		ftm.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(termAndFacet3));

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

		ImportConcepts terms = ConceptManagerTest.getTestTerms(amount);
		ConceptManager termManager = new ConceptManager();
		termManager.insertConcepts(graphDb, ConceptsJsonSerializer.toJson(terms));
		FacetManager facetManager = new FacetManager();

		assertEquals(amount, facetManager.getFacetSize(graphDb, facet));
	}

	public static ImportFacet getImportFacet() {
		return getTestFacetMap(1);
	}

	public static ImportFacet getTestFacetMap(int n) {

		ImportFacetGroup facetGroup = new ImportFacetGroup("facetGroup1", 1,
				Lists.newArrayList("showForSearch"));
		ImportFacet testFacet = new ImportFacet(facetGroup, "testfacet" + n, "testfacet" + n, "testfacet" + n, 
				SRC_TYPE_HIERARCHICAL, Arrays.asList("hidden"), false);
		return testFacet;
	}

	@AfterClass
	public static void shutdown() {
		graphDb.shutdown();
	}
}
