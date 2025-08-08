package com.dqs.eventdrivensearch.queryExecution.search.model;

import org.apache.lucene.search.Query;


public record SearchTask(Query query, String queryId, String subQueryId, String s3IndexFilePath) {
}

