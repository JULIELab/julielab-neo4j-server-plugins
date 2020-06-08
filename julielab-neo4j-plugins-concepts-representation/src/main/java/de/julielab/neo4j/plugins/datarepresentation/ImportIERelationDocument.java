package de.julielab.neo4j.plugins.datarepresentation;

import java.util.List;

public class ImportIERelationDocument {
    private String name;
    private boolean isDb;
    private List<ImportIETypedRelations> relations;

    public static ImportIERelationDocument of(String name, boolean isDb, ImportIETypedRelations... relations) {
        ImportIERelationDocument d = new ImportIERelationDocument();
        d.setName(name);
        d.setDb(isDb);
        d.setRelations(List.of(relations));
        return d;
    }

    public boolean isDb() {
        return isDb;
    }

    public void setDb(boolean db) {
        isDb = db;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<ImportIETypedRelations> getRelations() {
        return relations;
    }

    public void setRelations(List<ImportIETypedRelations> relations) {
        this.relations = relations;
    }
}
