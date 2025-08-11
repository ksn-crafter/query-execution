package com.dqs.eventdrivensearch.queryExecution.model;

import java.util.List;

public record SearchResult(List<String> documentIds, int total, long totalHits, int totalMatchedDocuments) {


}
