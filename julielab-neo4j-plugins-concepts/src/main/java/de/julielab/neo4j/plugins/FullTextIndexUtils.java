package de.julielab.neo4j.plugins;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;

import java.util.Map;

public class FullTextIndexUtils {
    public static void createTextIndex(Transaction tx, String indexName, Label[] labels, String[] properties) {
        tx.execute("CALL db.index.fulltext.createNodeIndex({indexName}, {labels}, {properties})", Map.of("indexName", indexName, "labels", labels, "properties", properties));
    }


    public static void deleteTextIndex(Transaction tx, String indexName) {
        tx.execute("CALL db.index.fulltext.drop({indexName})", Map.of("indexName", indexName));
    }

    public static ResourceIterator<Object> getNodes(Transaction tx, Label label, String property, String propertyValue) {
        // after https://neo4j.com/docs/java-reference/current/java-embedded/unique-nodes/
        return tx.execute("CALL db.index.fulltext.queryNodes({label}, {query}})", Map.of("label", label, "query", property + ":" + propertyValue)).columnAs("n");
    }
}