package de.julielab.neo4j.plugins.datarepresentation;

import java.util.HashMap;

public class ImportIETypedRelations extends HashMap<String, ImportIERelation> {
    public static ImportIETypedRelations of(String rt, ImportIERelation r) {
        ImportIETypedRelations rs = new ImportIETypedRelations();
        rs.put(rt, r);
        return rs;
    }

    public static ImportIETypedRelations of(String rt1, ImportIERelation r1, String rt2, ImportIERelation r2) {
        ImportIETypedRelations rs = new ImportIETypedRelations();
        rs.put(rt1, r1);
        rs.put(rt2, r2);
        return rs;
    }

    public static ImportIETypedRelations of(String rt1, ImportIERelation r1, String rt2, ImportIERelation r2, String rt3, ImportIERelation r3) {
        ImportIETypedRelations rs = new ImportIETypedRelations();
        rs.put(rt1, r1);
        rs.put(rt2, r2);
        rs.put(rt3, r3);
        return rs;
    }

}
