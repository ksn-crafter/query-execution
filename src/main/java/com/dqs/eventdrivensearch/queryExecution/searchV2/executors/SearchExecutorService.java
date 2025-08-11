package com.dqs.eventdrivensearch.queryExecution.searchV2.executors;

import com.dqs.eventdrivensearch.queryExecution.searchV2.IndexSearcher;
import com.dqs.eventdrivensearch.queryExecution.search.model.SearchResult;
import com.dqs.eventdrivensearch.queryExecution.search.model.SearchTaskWithIndexPath;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
public class SearchExecutorService {

    @Autowired
    private IndexSearcher indexSearcher;

    private final ExecutorService executorService;

    public SearchExecutorService(@Value("${index_searcher_pool_size:2}") int poolSize) {
        this.executorService = Executors.newFixedThreadPool(poolSize);
    }

    public Future<CompletableFuture<SearchResult>> submit(SearchTaskWithIndexPath task) {
        return executorService.submit(() -> indexSearcher.search(task));
    }

//    public void submit(SearchTask task) {
//        //TODO: for cached file paths
//    }

    @PreDestroy
    public void shutdown() {
        executorService.shutdown();
    }
}
