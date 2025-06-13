package com.dqs.eventdrivensearch.queryExecution.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import java.time.LocalDateTime;

@Document(collection = "query_descriptions")
public class QueryDescription {
    @Id
    private String queryId;
    private String tenant;
    private String term;
    private int yearStart;
    private int yearEnd;
    private QueryStatus status;
    private LocalDateTime creationTime;
    private LocalDateTime completionTime;

    public QueryDescription() {}

    public QueryDescription(String queryId, String tenant, String term, int yearStart, int yearEnd, QueryStatus status, LocalDateTime creationTime) {
        this.queryId = queryId;
        this.tenant = tenant;
        this.term = term;
        this.yearStart = yearStart;
        this.yearEnd = yearEnd;
        this.status = status;
        this.creationTime = creationTime;
    }

    public String queryId() {
        return queryId;
    }

    public String tenant() {
        return tenant;
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

    public void complete() {
        this.completionTime = LocalDateTime.now();
        this.status = QueryStatus.Completed;
    }
}
