package com.dqs.eventdrivensearch.queryExecution.search.model;

import java.util.List;

public class SearchResult {
    private List<String> documentIds;
    private int total;
    private long totalHits;
    private int totalMatchedDocuments;

    public SearchResult(List<String> documentIds, int total, long totalHits, int totalMatchedDocuments) {
        this.documentIds = documentIds;
        this.total = total;
        this.totalHits = totalHits;
        this.totalMatchedDocuments = totalMatchedDocuments;
    }

    public List<String> documentIds() {
        return documentIds;
    }

    public int total() {
        return total;
    }

    public long totalHits() {
        return totalHits;
    }

    public int totalMatchedDocuments() {
        return totalMatchedDocuments;
    }

    public void mergeFrom(SearchResult other) {
        if (other != null) {
            this.documentIds.addAll(other.documentIds);
            this.total += other.total;
            this.totalHits += other.totalHits;
            this.totalMatchedDocuments += other.totalMatchedDocuments;
        }
    }
}
