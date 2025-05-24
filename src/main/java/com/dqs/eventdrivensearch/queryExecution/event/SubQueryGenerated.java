package com.dqs.eventdrivensearch.queryExecution.event;

import com.dqs.eventdrivensearch.queryExecution.model.QueryId;
import com.dqs.eventdrivensearch.queryExecution.model.SubQueryId;

public record SubQueryGenerated(String queryId, SubQueryId subQueryId, String[] indexPaths) {
}
