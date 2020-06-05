package de.julielab.neo4j.plugins.datarepresentation;

import java.util.List;

public class ImportIERelationDocument {
    private String docId;
    private List<ImportIETypedRelations> relations;

    public static ImportIERelationDocument of(String docId, ImportIETypedRelations... relations) {
        ImportIERelationDocument d = new ImportIERelationDocument();
        d.setDocId(docId);
        d.setRelations(List.of(relations));
        return d;
    }

    public String getDocId() {
        return docId;
    }

    public void setDocId(String docId) {
        this.docId = docId;
    }

    public List<ImportIETypedRelations> getRelations() {
        return relations;
    }

    public void setRelations(List<ImportIETypedRelations> relations) {
        this.relations = relations;
    }
}
