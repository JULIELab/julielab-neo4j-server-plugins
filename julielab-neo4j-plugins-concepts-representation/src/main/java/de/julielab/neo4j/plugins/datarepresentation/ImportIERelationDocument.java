package de.julielab.neo4j.plugins.datarepresentation;

import java.util.HashMap;

public class ImportIERelationDocument extends HashMap<String, ImportIERelationMap> {
    public static ImportIERelationDocument of(String docId, ImportIERelationMap relationlist) {
        ImportIERelationDocument d = new ImportIERelationDocument();
        d.put(docId, relationlist);
        return d;
    }

    public static ImportIERelationDocument of(String docId1, ImportIERelationMap relationlist1, String docId2, ImportIERelationMap relationlist2) {
        ImportIERelationDocument d = new ImportIERelationDocument();
        d.put(docId1, relationlist1);
        d.put(docId2, relationlist2);
        return d;
    }

    public static ImportIERelationDocument of(String docId1, ImportIERelationMap relationlist1, String docId2, ImportIERelationMap relationlist2, String docId3, ImportIERelationMap relationlist3) {
        ImportIERelationDocument d = new ImportIERelationDocument();
        d.put(docId1, relationlist1);
        d.put(docId2, relationlist2);
        d.put(docId3, relationlist3);
        return d;
    }
}
