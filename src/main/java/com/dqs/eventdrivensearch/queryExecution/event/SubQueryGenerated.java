package com.dqs.eventdrivensearch.queryExecution.event;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SubQueryGenerated {
    @JsonProperty
    String queryId;
    @JsonProperty
    String subQueryId;
    @JsonProperty
    String[] indexPaths;

    public SubQueryGenerated(){}

    public SubQueryGenerated(String queryId,String subQueryId,String[] indexPaths){
        this.queryId = queryId;
        this.subQueryId = subQueryId;
        this.indexPaths = indexPaths;
    }

    public String queryId(){
        return queryId;
    }

    public String subQueryId(){
        return subQueryId;
    }

    public String[] indexPaths(){
        return indexPaths;
    }

}
