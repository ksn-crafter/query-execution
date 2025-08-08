package com.dqs.eventdrivensearch.queryExecution.search.executors;

import com.dqs.eventdrivensearch.queryExecution.search.index.IndexSearcher;
import com.dqs.eventdrivensearch.queryExecution.search.model.SearchTask;
import com.dqs.eventdrivensearch.queryExecution.search.model.SearchTaskWithIndexPath;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
public class SearchExecutorService {

    @Autowired
    private IndexSearcher indexSearcher;

    private final ExecutorService executorService;

    public SearchExecutorService(@Value("${index_searcher_pool_size:2}") int poolSize) {
        this.executorService = Executors.newFixedThreadPool(poolSize);
    }

    public void submit(SearchTaskWithIndexPath task) {
        executorService.submit(() -> indexSearcher.search(task));
    }

    public void submit(SearchTask task) {
        //TODO:for cached file paths
    }

    @PreDestroy
    public void shutdown() {
        executorService.shutdown();
    }
}
