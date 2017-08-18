package de.julielab.neo4j.plugins.test;

import static org.junit.Assert.assertFalse;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.Representation;

public class TestUtilities {

	public static final File GRAPH_DB_DIR = new File("src/test/resources/graph.db");
	public static final File GRAPH_DB_DIR_2 = new File("src/test/resources/test");

	private static final Random random = new Random();
	private static final String[] symbols = new String[36];

	static {
		for (int idx = 0; idx < 10; ++idx)
			symbols[idx] = String.valueOf((char) ('0' + idx));
		for (int idx = 10; idx < 36; ++idx)
			symbols[idx] = String.valueOf((char) ('a' + idx - 10));
	}

	public static String randomLetter() {
		return symbols[random.nextInt(symbols.length)];
	}

	public static void deleteEverythingInDB(GraphDatabaseService graphDb) {
		try (Transaction tx = graphDb.beginTx()) {
			Iterable<Node> nodes = graphDb.getAllNodes();
			for (Node n : nodes) {
				for (Relationship r : n.getRelationships())
					r.delete();
				n.delete();
			}
			tx.success();
		}
		// ExecutionEngine executionEngine = new ExecutionEngine(graphDb);
		// executionEngine.execute("MATCH (n) OPTIONAL MATCH n-[r]-() DELETE n, r");
		// executionEngine.execute("MATCH n DELETE n");
		try (Transaction tx = graphDb.beginTx()) {
			Iterator<Node> nodeIt = graphDb.getAllNodes().iterator();
			// But there should be no nodes.
			assertFalse(nodeIt.hasNext());

			// Delete indexes.
			IndexManager indexManager = graphDb.index();
			for (String nodeIndex : indexManager.nodeIndexNames()) {
				indexManager.forNodes(nodeIndex).delete();
			}
			for (String relIndex : indexManager.relationshipIndexNames()) {
				indexManager.forNodes(relIndex).delete();
			}
			for (ConstraintDefinition cd : graphDb.schema().getConstraints()) {
				cd.drop();
			}
			for (IndexDefinition id : graphDb.schema().getIndexes())
				id.drop();
			tx.success();
		}
	}

	public static <T extends Representation> List<T> getListFromListRepresentation(ListRepresentation listrep)
			throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
		Field responseContent = ListRepresentation.class.getDeclaredField("content");
		responseContent.setAccessible(true);
		@SuppressWarnings("unchecked")
		List<T> content = (List<T>) responseContent.get(listrep);
		return content;
	}

	public static <T extends Representation> Iterable<T> getIterableFromListRepresentation(ListRepresentation listrep)
			throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
		Field responseContent = ListRepresentation.class.getDeclaredField("content");
		responseContent.setAccessible(true);
		@SuppressWarnings("unchecked")
		Iterable<T> content = (Iterable<T>) responseContent.get(listrep);
		return content;
	}

	public static GraphDatabaseService getGraphDB() {
		GraphDatabaseService database = new GraphDatabaseFactory().newEmbeddedDatabase(GRAPH_DB_DIR);
		return database;
	}

	public static void printNodeProperties(Node n) {
		for (String k : n.getPropertyKeys()) {
			System.out.println(k + ": " + n.getProperty(k));
		}
	}

}
