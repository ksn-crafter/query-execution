package com.dqs.eventdrivensearch.queryExecution.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import java.time.LocalDateTime;

@Document(collection = "queryDescription")
public class QueryDescription {
    @Id
    private String queryId;
    private String tenantId;
    private String term;
    private int yearStart;
    private int yearEnd;
    private QueryStatus status;
    private LocalDateTime creationTime;
    private LocalDateTime completionTime;

    public QueryDescription() {}

    public QueryDescription(String queryId, String tenantId, String term, int yearStart, int yearEnd, QueryStatus status, LocalDateTime creationTime) {
        this.queryId = queryId;
        this.tenantId = tenantId;
        this.term = term;
        this.yearStart = yearStart;
        this.yearEnd = yearEnd;
        this.status = status;
        this.creationTime = creationTime;
    }

    public String queryId() {
        return queryId;
    }

    public String tenantId() {
        return tenantId;
    }

    public String term() {
        return term;
    }

    public int yearStart() {
        return yearStart;
    }

    public int yearEnd() {
        return yearEnd;
    }

    public QueryStatus status() {
        return status;
    }

    public LocalDateTime creationTime() {
        return creationTime;
    }

    public LocalDateTime completionTime() {
        return completionTime;
    }

    public void setStatusToInProgress(){
        status = QueryStatus.InProgress;
    }
}
