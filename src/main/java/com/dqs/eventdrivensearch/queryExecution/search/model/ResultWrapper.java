package com.dqs.eventdrivensearch.queryExecution.search.model;

public record ResultWrapper(String queryId, String subQueryId, SearchResult result) {}
