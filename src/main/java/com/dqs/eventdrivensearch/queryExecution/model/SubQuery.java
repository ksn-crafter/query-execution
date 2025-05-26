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
    private String[] indexPaths;
    private SubQueryStatus status;
    private LocalDateTime creationTime;
    private LocalDateTime completionTime;

    public SubQuery() {
    }

    public SubQuery(String queryId, String subQueryId, String[] indexPaths) {
        this.queryId = queryId;
        this.subQueryId = subQueryId;
        this.indexPaths = indexPaths;
        this.status = SubQueryStatus.CREATED;
        this.creationTime = LocalDateTime.now();
    }

    public String queryId() {
        return queryId;
    }

    public String subQueryId() {
        return subQueryId;
    }

    public String[] indexPaths() {
        return indexPaths;
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

    public void setIndexPaths(String[] indexPaths) {
        this.indexPaths = indexPaths;
    }

    public void setSubQueryId(String subQueryId) {
        this.subQueryId = subQueryId;
    }

    public void setCreationTime(LocalDateTime creationTime) {
        this.creationTime = creationTime;
    }

    public void setCompletionTime(LocalDateTime completionTime) {
        this.completionTime = completionTime;
    }
}
