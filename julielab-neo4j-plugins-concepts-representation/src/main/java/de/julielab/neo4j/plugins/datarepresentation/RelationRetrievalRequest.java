package de.julielab.neo4j.plugins.datarepresentation;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class RelationRetrievalRequest {
    public static final String A_LIST = "a_list";
    public static final String B_LIST = "b_list";
    @JsonProperty(A_LIST)
    private RelationIdList alist;
    @JsonProperty(B_LIST)
    private RelationIdList blist;

    public List<String> getRelationTypes() {
        return relationTypes;
    }

    public void setRelationTypes(List<String> relationTypes) {
        this.relationTypes = relationTypes;
    }

    private List<String> relationTypes;

    public RelationRetrievalRequest() {
    }

    public RelationIdList getAlist() {
        return alist;
    }

    public RelationIdList getBlist() {
        return blist;
    }

    public void setAlist(RelationIdList alist) {
        this.alist = alist;
    }

    public void setBlist(RelationIdList blist) {
        this.blist = blist;
    }

    @Override
    public String toString() {
        return "RelationRetrievalRequest{" +
                "alist=" + alist +
                ", blist=" + blist +
                '}';
    }
}
