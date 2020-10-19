package de.julielab.neo4j.plugins.auxiliaries;

import com.google.common.collect.Lists;
import de.julielab.neo4j.plugins.constants.NodeConstants;
import de.julielab.neo4j.plugins.test.TestUtilities;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class NodeRepresentationTest {
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
	}
	
	@Test
	public void testNodeRepresentation() {
		try (Transaction tx = graphDb.beginTx()){
			Node node = tx.createNode(Label.label("label1"), Label.label("label2"));
			node.setProperty("property", "value");
			NodeRepresentation nodeRepresentation = new NodeRepresentation(node);
			Map<String, ?> map = nodeRepresentation.getUnderlyingMap();
			String[] labels = (String[]) map.get(NodeConstants.KEY_LABELS);
			List<String> labelList = Lists.newArrayList(labels);
			assertTrue(labelList.contains("label1"));
			assertTrue(labelList.contains("label2"));
			String value = (String) map.get("property");
			assertEquals("value", value);
		}
	}
	
	@AfterClass
	public static void shutdown() {
		graphDBMS.shutdown();
	}
}
