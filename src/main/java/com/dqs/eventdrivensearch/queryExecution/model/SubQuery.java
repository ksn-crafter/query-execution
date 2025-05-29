package com.dqs.eventdrivensearch.queryExecution.model;


import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;

@Document(collection = "sub_queries")
public class SubQuery {
    @Id
    private String id;
    private String queryId;
    private String subQueryId;
    private String[] filePaths;
    private SubQueryStatus status;
    private LocalDateTime creationTime;
    private LocalDateTime completionTime;
    private int totalSubQueries;

    public SubQuery() {
    }

    public SubQuery(String queryId, String subQueryId, String[] filePaths, int totalSubQueries) {
        this.queryId = queryId;
        this.subQueryId = subQueryId;
        this.filePaths = filePaths;
        this.status = SubQueryStatus.CREATED;
        this.creationTime = LocalDateTime.now();
        this.totalSubQueries = totalSubQueries;
    }

    public String queryId() {
        return queryId;
    }

    public String subQueryId() {
        return subQueryId;
    }

    public String[] indexPaths() {
        return filePaths;
    }

    public SubQueryStatus status() {
        return status;
    }

    public LocalDateTime creationTime() {
        return creationTime;
    }

    public LocalDateTime completionTime() {
        return completionTime;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public int totalSubQueries(){
        return totalSubQueries;
    }
}
