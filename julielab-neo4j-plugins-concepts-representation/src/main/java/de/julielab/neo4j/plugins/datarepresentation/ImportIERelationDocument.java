package de.julielab.neo4j.plugins.datarepresentation;

public class ImportIERelationDocument {
    private String name;
    private boolean isDb;
    private ImportIETypedRelations relations;

    public static ImportIERelationDocument of(String name, boolean isDb, ImportIETypedRelations relations) {
        ImportIERelationDocument d = new ImportIERelationDocument();
        d.setName(name);
        d.setDb(isDb);
        d.setRelations(relations);
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
        if (name == null)
            throw new IllegalArgumentException("The DB name / document ID cannot be null.");
        this.name = name;
    }

    public ImportIETypedRelations getRelations() {
        return relations;
    }

    public void setRelations(ImportIETypedRelations relations) {
        this.relations = relations;
    }
}
