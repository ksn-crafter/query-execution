package com.dqs.eventdrivensearch.queryExecution.search.model;

import org.apache.lucene.search.Query;

import java.nio.file.Path;

public record SearchTask(Path indexPath, Query query, String queryId, String subQueryId) {
}
