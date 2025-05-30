package com.dqs.eventdrivensearch.queryExecution.event;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SubQueryGenerated {
    @JsonProperty
    private String queryId;
    @JsonProperty
    private String subQueryId;
    @JsonProperty
    private String[] filePaths;
    @JsonProperty
    private int totalSubQueries;
    @JsonProperty
    private String tenant;

    public SubQueryGenerated(){}

    public SubQueryGenerated(String queryId, String subQueryId, String[] filePaths, int totalSubQueries){
        this.queryId = queryId;
        this.subQueryId = subQueryId;
        this.filePaths = filePaths;
        this.totalSubQueries = totalSubQueries;
    }

    public String queryId(){
        return queryId;
    }

    public String subQueryId(){
        return subQueryId;
    }

    public String[] indexPaths(){
        return filePaths;
    }

    public int totalSubQueries(){
        return totalSubQueries;
    }

    public String tenant(){
        return tenant;
    }
}
