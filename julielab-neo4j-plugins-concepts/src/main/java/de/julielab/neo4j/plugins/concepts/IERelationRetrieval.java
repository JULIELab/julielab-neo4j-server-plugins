package de.julielab.neo4j.plugins.concepts;

import de.julielab.neo4j.plugins.datarepresentation.RelationRetrievalRequest;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

import java.util.List;

public class IERelationRetrieval {
    public static List<String> retrieve(RelationRetrievalRequest retrievalRequest, GraphDatabaseService dbms, Log log) {
        System.out.println(retrievalRequest);
        try (Transaction tx = dbms.beginTx()) {
        }
        return null;
    }
}
