package com.dqs.eventdrivensearch.queryExecution.search.model;

import java.util.List;

public class SearchResult {
    final private List<String> documentIds;
    final private int total;
    final private long totalHits;
    final private int totalMatchedDocuments;

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


}
