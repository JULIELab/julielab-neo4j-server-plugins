package de.julielab.neo4j.plugins;

import org.neo4j.graphdb.*;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.stream.Stream;

public class FullTextIndexUtils {
    public static void createTextIndex(Transaction tx, String indexName, Label[] labels, String[] properties) {
        createTextIndex(tx, indexName, labels, properties);
    }

    public static void createTextIndex(Transaction tx, String indexName, @Nullable Map<String, String> indexSettings, Label[] labels, String[] properties) {
        if (indexSettings == null)
            tx.execute("CALL db.index.fulltext.createNodeIndex($indexName, $labels, $properties)", Map.of("indexName", indexName, "labels", Stream.of(labels).map(Label::name).toArray(), "properties", properties));
        else
            tx.execute("CALL db.index.fulltext.createNodeIndex($indexName, $labels, $properties, $indexSettings)", Map.of("indexName", indexName, "labels", Stream.of(labels).map(Label::name).toArray(), "properties", properties, "indexSettings", indexSettings));
    }


    public static void deleteTextIndex(Transaction tx, String indexName) {
        tx.execute("CALL db.index.fulltext.drop($indexName)", Map.of("indexName", indexName));
    }

    public static ResourceIterator<Object> getNodes(Transaction tx, String indexName, String property, String propertyValue) {
        // after https://neo4j.com/docs/java-reference/current/java-embedded/unique-nodes/
        return tx.execute("CALL db.index.fulltext.queryNodes(indexName, $query)", Map.of("indexName", indexName, "query", property + ":" + propertyValue)).columnAs("n");
    }

    public static Node getNode(Transaction tx, String indexName, String property, String propertyValue) {
        // after https://neo4j.com/docs/java-reference/current/java-embedded/unique-nodes/
        Node n = null;
        Result r = tx.execute("CALL db.index.fulltext.queryNodes($indexName, $query)", Map.of("indexName", indexName, "query", property + ":" + propertyValue));
        try (ResourceIterator<Node> it = tx.execute("CALL db.index.fulltext.queryNodes($indexName, $query)", Map.of("indexName", indexName, "query", property + ":\"" + propertyValue + "\"")).columnAs("node")) {
            if (it.hasNext())
                n = it.next();
            if (it.hasNext())
                throw new IllegalStateException("There are multiple nodes that have the property value " + property + ":" + propertyValue);
        }
        return n;
    }
}