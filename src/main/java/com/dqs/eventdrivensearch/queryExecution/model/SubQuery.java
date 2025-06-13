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
    private int totalSubqueries;
    private String tenant;

    public SubQuery() {
    }

    public SubQuery(String queryId, String subQueryId, String[] filePaths, int totalSubqueries, String tenant) {
        this.queryId = queryId;
        this.subQueryId = subQueryId;
        this.filePaths = filePaths;
        this.status = SubQueryStatus.CREATED;
        this.creationTime = LocalDateTime.now();
        this.totalSubqueries = totalSubqueries;
        this.tenant = tenant;
    }

    public SubQuery(String id, String queryId, String subQueryId, int totalSubqueries, SubQueryStatus status) {
        this.id = id;
        this.queryId = queryId;
        this.subQueryId = subQueryId;
        this.totalSubqueries = totalSubqueries;
        this.status = status;
    }

    public SubQuery(String id, String queryId, String subQueryId, int totalSubqueries, SubQueryStatus status, LocalDateTime completionTime) {
        this.id = id;
        this.queryId = queryId;
        this.subQueryId = subQueryId;
        this.totalSubqueries = totalSubqueries;
        this.status = status;
        this.completionTime = completionTime;
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
        return totalSubqueries;
    }

    public void complete() {
        this.status = SubQueryStatus.COMPLETED;
        this.completionTime = LocalDateTime.now();
    }
}
