package de.julielab.neo4j.plugins.datarepresentation.constants;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import de.julielab.neo4j.plugins.datarepresentation.ImportIERelationDocument;

import java.util.ArrayList;
import java.util.List;

@JsonPropertyOrder({ImportIERelations.NAME_ID_SOURCE, ImportIERelations.NAME_ID_PROPERTY, ImportIERelations.NAME_DOCUMENTS})
public class ImportIERelations {
    public static final String NAME_ID_PROPERTY = "id_property";
    public static final String NAME_ID_SOURCE = "id_source";
    public static final String NAME_DOCUMENTS = "documents";
    @JsonProperty(NAME_ID_PROPERTY)
    private String idProperty;
    @JsonProperty(NAME_ID_SOURCE)
    private String idSource;
    private List<ImportIERelationDocument> relations = new ArrayList<>();
    public ImportIERelations(String idProperty, String idSource) {
        this.idProperty = idProperty;
        this.idSource = idSource;
    }

    public ImportIERelations(String idProperty) {
        this.idProperty = idProperty;
    }


    public void addRelationDocument(ImportIERelationDocument document) {
        relations.add(document);
    }

    public String getIdProperty() {
        return idProperty;
    }

    public String getIdSource() {
        return idSource;
    }

    public List<ImportIERelationDocument> getRelations() {
        return relations;
    }
}
