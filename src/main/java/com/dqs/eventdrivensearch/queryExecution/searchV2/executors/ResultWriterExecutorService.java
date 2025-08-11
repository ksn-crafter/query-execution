package com.dqs.eventdrivensearch.queryExecution.searchV2.executors;

import com.dqs.eventdrivensearch.queryExecution.model.SearchResult;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
public class ResultWriterExecutorService {

    public CompletableFuture<Void> submit(String queryId, SearchResult result, String s3IndexFilePath) {
        return CompletableFuture.completedFuture(null);
    }
}
