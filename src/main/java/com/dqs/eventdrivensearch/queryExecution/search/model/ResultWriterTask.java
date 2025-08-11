package com.dqs.eventdrivensearch.queryExecution.search.model;

import com.dqs.eventdrivensearch.queryExecution.model.SearchResult;

public record ResultWriterTask(String queryId, String subQueryId, SearchResult result) {}
