package com.dqs.eventdrivensearch.queryExecution.event;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;

public class SubQueryExecuted {
    @JsonProperty
    private String queryId;

    @JsonProperty
    private String subQueryId;

    @JsonProperty
    private int totalSubQueries;

    @JsonProperty
    private LocalDateTime completionTime;

    @JsonProperty
    private String tenant;

    public SubQueryExecuted(){}

    public SubQueryExecuted(String subQueryId,String queryId,int totalSubQueries,LocalDateTime completionTime,String tenant) {
        this.queryId = queryId;
        this.subQueryId = subQueryId;
        this.totalSubQueries = totalSubQueries;
        this.completionTime = completionTime;
        this.tenant = tenant;
    }

    public String queryId(){
        return queryId;
    }

    public String subQueryId(){
        return subQueryId;
    }

    public int totalSubQueries(){
        return totalSubQueries;
    }

    public LocalDateTime completionTime(){
        return completionTime;
    }

    public String tenant(){
        return tenant;
    }
}
