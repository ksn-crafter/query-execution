package com.dqs.eventdrivensearch.queryExecution.event;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SubQueryExecuted {
    @JsonProperty
    private String queryId;

    @JsonProperty
    private String subQueryId;

    @JsonProperty
    private String resultsPath;

    @JsonProperty
    private int totalSubQueries;

    public SubQueryExecuted(){}

    public SubQueryExecuted(String subQueryId,String queryId,String resultsPath,int totalSubQueries) {
        this.queryId = queryId;
        this.subQueryId = subQueryId;
        this.resultsPath = resultsPath;
        this.totalSubQueries = totalSubQueries;
    }

    public String queryId(){
        return queryId;
    }

    public String subQueryId(){
        return subQueryId;
    }

    public String resultsPath(){
        return resultsPath;
    }
}
