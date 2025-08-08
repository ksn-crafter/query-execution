package com.dqs.eventdrivensearch.queryExecution.search.model;

public record ResultWriterTask(String queryId, String subQueryId, SearchResult result) {}
