package com.dqs.eventdrivensearch.queryExecution.model;

import org.apache.lucene.search.Query;

import java.nio.file.Path;


public record SearchTask(Query query, String queryId, String subQueryId, String s3IndexFilePath, Path indexPath) {
}

