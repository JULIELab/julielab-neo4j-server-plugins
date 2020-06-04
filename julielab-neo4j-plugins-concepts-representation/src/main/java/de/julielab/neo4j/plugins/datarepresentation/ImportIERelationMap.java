package de.julielab.neo4j.plugins.datarepresentation;

import java.util.HashMap;
import java.util.List;

public class ImportIERelationMap extends HashMap<String, List<ImportIERelation>> {
    public static ImportIERelationMap of(String relationshipType, ImportIERelation... relation) {
        ImportIERelationMap l = new ImportIERelationMap();
        l.put(relationshipType, List.of(relation));
        return l;
    }

    public static ImportIERelationMap of(String relationshipType, List<ImportIERelation> relations) {
        ImportIERelationMap l = new ImportIERelationMap();
        l.put(relationshipType, relations);
        return l;
    }

    public static ImportIERelationMap of(String relationshipType1, List<ImportIERelation> relations1, String relationshipType2, List<ImportIERelation> relations2) {
        ImportIERelationMap l = new ImportIERelationMap();
        l.put(relationshipType1, relations1);
        l.put(relationshipType2, relations2);
        return l;
    }

    public static ImportIERelationMap of(String relationshipType1, List<ImportIERelation> relations1, String relationshipType2, List<ImportIERelation> relations2, String relationshipType3, List<ImportIERelation> relations3) {
        ImportIERelationMap l = new ImportIERelationMap();
        l.put(relationshipType1, relations1);
        l.put(relationshipType2, relations2);
        l.put(relationshipType3, relations3);
        return l;
    }
}
