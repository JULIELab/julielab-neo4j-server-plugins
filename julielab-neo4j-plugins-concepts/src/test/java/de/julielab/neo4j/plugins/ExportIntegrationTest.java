package de.julielab.neo4j.plugins;

import de.julielab.neo4j.plugins.concepts.ConceptInsertion;
import de.julielab.neo4j.plugins.concepts.InsertionReport;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;
import de.julielab.neo4j.plugins.util.ConceptInsertionException;
import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.rule.Neo4jRule;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
public class ExportIntegrationTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withUnmanagedExtension("/app", Export.class).withFixture(graphDatabaseService -> {
                new Indexes(null).createIndexes(graphDatabaseService);
                try (Transaction tx = graphDatabaseService.beginTx()) {
                    ImportConcepts testConcepts = ConceptManagerTest.getTestConcepts(10);
                    ConceptInsertion.insertConcepts(tx, testConcepts, new HashMap<>(), new InsertionReport());
                    tx.commit();
                } catch (ConceptInsertionException e) {
                    throw new IllegalArgumentException(e);
                }
                return null;
            });

    @Test
    public void exportIdMapping() throws IOException {
        URI uri = neo4j.httpURI().resolve(String.format("app/%s/%s", "export", Export.CONCEPT_ID_MAPPING));
        HttpURLConnection conn = (HttpURLConnection) uri.toURL().openConnection();
        try(InputStream is = conn.getInputStream()){
            String mapping = IOUtils.toString(is);
            assertThat(mapping).isEqualTo(
                    "CONCEPT6\ttid6\n" +
                    "CONCEPT5\ttid5\n" +
                    "CONCEPT9\ttid9\n" +
                    "CONCEPT8\ttid8\n" +
                    "CONCEPT0\ttid0\n" +
                    "CONCEPT7\ttid7\n" +
                    "CONCEPT2\ttid2\n" +
                    "CONCEPT1\ttid1\n" +
                    "CONCEPT4\ttid4\n" +
                    "CONCEPT3\ttid3\n");
        }
    }
}
