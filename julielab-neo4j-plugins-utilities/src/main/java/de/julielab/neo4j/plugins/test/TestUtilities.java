package de.julielab.neo4j.plugins.test;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.Representation;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertFalse;

public class TestUtilities {

	public static final File GRAPH_DB_DIR = new File("src/test/resources/graph.db");

	private static final Random random = new Random();
	private static final String[] symbols = new String[36];

	static {
		for (int idx = 0; idx < 10; ++idx)
			symbols[idx] = String.valueOf((char) ('0' + idx));
		for (int idx = 10; idx < 36; ++idx)
			symbols[idx] = String.valueOf((char) ('a' + idx - 10));
	}

	public static void deleteEverythingInDB(GraphDatabaseService graphDb) {
		try (Transaction tx = graphDb.beginTx()) {
			Iterable<Node> nodes = tx.getAllNodes();
			for (Node n : nodes) {
				for (Relationship r : n.getRelationships())
					r.delete();
				n.delete();
			}
			tx.commit();
		}
		try (Transaction tx = graphDb.beginTx()) {
			Iterator<Node> nodeIt = tx.getAllNodes().iterator();
			// But there should be no nodes.
			assertFalse(nodeIt.hasNext());


			for (ConstraintDefinition cd : tx.schema().getConstraints()) {
				cd.drop();
			}
			for (IndexDefinition id : tx.schema().getIndexes())
				id.drop();
			tx.commit();
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

	public static DatabaseManagementService getGraphDBMS() {
		DatabaseManagementService managementService = new DatabaseManagementServiceBuilder(GRAPH_DB_DIR).build();
		return managementService;
	}

}
