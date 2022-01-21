package de.julielab.neo4j.plugins.datarepresentation;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class RelationIdList {
    public static final String NAME_ID_PROPERTY = "id_property";
    /**
     * @deprecated Not used
     */
    @Deprecated
    public static final String NAME_ID_SOURCE = "id_source";
    @JsonProperty(NAME_ID_PROPERTY)
    private String idProperty;
    @JsonProperty(NAME_ID_SOURCE)
    private String idSource;
    private List<String> ids;

    @Override
    public String toString() {
        return "RelationIdList{" +
                "idProperty='" + idProperty + '\'' +
                ", idSource='" + idSource + '\'' +
                ", ids=" + ids +
                '}';
    }

    public String getIdProperty() {
        return idProperty;
    }

    @Deprecated
    public String getIdSource() {
        return idSource;
    }

    public List<String> getIds() {
        return ids;
    }

    public void setIds(List<String> ids) {
        this.ids = ids;
    }

    public RelationIdList() {
    }

    @Deprecated
    public void setIdSource(String idSource) {
        this.idSource = idSource;
    }

    public void setIdProperty(String idProperty) {
        this.idProperty = idProperty;
    }
}
