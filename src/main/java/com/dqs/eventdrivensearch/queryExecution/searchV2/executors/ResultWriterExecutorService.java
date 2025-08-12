package com.dqs.eventdrivensearch.queryExecution.searchV2.executors;

import com.dqs.eventdrivensearch.queryExecution.model.SearchResult;
import com.dqs.eventdrivensearch.queryExecution.searchV2.ResultWriter;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import java.util.concurrent.CompletableFuture;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
public class ResultWriterExecutorService {

    private final ResultWriter resultWriter;

    public ResultWriterExecutorService(ResultWriter resultWriter) {
        this.resultWriter = resultWriter;
    }

    public CompletableFuture<Void> submit(String queryId, SearchResult result, String s3IndexFilePath) {
        return resultWriter.writeResult(queryId, result, s3IndexFilePath);
    }
}
