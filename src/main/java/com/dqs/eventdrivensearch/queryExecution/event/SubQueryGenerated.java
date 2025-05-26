package com.dqs.eventdrivensearch.queryExecution.event;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SubQueryGenerated {
    @JsonProperty
    String queryId;
    @JsonProperty
    String subQueryId;
    @JsonProperty
    String[] indexPaths;
    @JsonProperty
    int totalSubQueries;

    public SubQueryGenerated(){}

    public SubQueryGenerated(String queryId,String subQueryId,String[] indexPaths,int totalSubQueries){
        this.queryId = queryId;
        this.subQueryId = subQueryId;
        this.indexPaths = indexPaths;
        this.totalSubQueries = totalSubQueries;
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

    public int totalSubQueries(){
        return totalSubQueries;
    }
}
