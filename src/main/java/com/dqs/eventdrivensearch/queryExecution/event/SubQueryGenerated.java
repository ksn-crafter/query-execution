package com.dqs.eventdrivensearch.queryExecution.event;

public record SubQueryGenerated(String queryId, String subQueryId, String[] indexPaths) {
}
