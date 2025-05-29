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

    public SubQueryExecuted(){}

    public SubQueryExecuted(String subQueryId,String queryId,int totalSubQueries,LocalDateTime completionTime) {
        this.queryId = queryId;
        this.subQueryId = subQueryId;
        this.totalSubQueries = totalSubQueries;
        this.completionTime = completionTime;
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
}
