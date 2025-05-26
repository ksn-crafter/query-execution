package com.dqs.eventdrivensearch.queryExecution.search.model;

import java.util.List;

public record SearchResult(List<String> documentIds, int total, long totalHits, int totalMatchedDocuments) {
}