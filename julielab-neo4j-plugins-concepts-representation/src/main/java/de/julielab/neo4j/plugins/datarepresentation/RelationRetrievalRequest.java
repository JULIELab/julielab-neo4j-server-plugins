package de.julielab.neo4j.plugins.datarepresentation;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class RelationRetrievalRequest {
    public static final String A_LIST = "a_list";
    public static final String B_LIST = "b_list";
    public static final String INTER_INPUT_RELATION_RETRIEVAL_ENABLED = "enable_inter_input_relation_retrieval";
    @JsonProperty(A_LIST)
    private RelationIdList alist;
    @JsonProperty(B_LIST)
    private RelationIdList blist;
    @JsonProperty(INTER_INPUT_RELATION_RETRIEVAL_ENABLED)
    private boolean interInputRelationRetrievalEnabled;
    private List<String> relationTypes;

    public RelationRetrievalRequest() {
    }

    /**
     * <p>
     * Only for a-search without the specification of a b-list. Indicates whether relations between input nodes
     * should be returned.
     * </p>
     * <p>Defaults to false.</p>
     *
     * @return If relations within the a-list input is to be returned.
     */
    public boolean isInterInputRelationRetrievalEnabled() {
        return interInputRelationRetrievalEnabled;
    }

    /**
     * <p>
     * Only for a-search without the specification of a b-list. Indicates whether relations between input nodes
     * should be returned.
     * </p>
     * <p>
     * Defaults to false.
     * </p>
     *
     * @param interInputRelationRetrievalEnabled If relations within the a-list input is to be returned.
     */
    public void setInterInputRelationRetrievalEnabled(boolean interInputRelationRetrievalEnabled) {
        this.interInputRelationRetrievalEnabled = interInputRelationRetrievalEnabled;
    }

    public List<String> getRelationTypes() {
        return relationTypes;
    }

    public void setRelationTypes(List<String> relationTypes) {
        this.relationTypes = relationTypes;
    }

    public RelationIdList getAlist() {
        return alist;
    }

    public void setAlist(RelationIdList alist) {
        this.alist = alist;
    }

    public RelationIdList getBlist() {
        return blist;
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
