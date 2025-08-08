package com.dqs.eventdrivensearch.queryExecution.search.queue;

import com.dqs.eventdrivensearch.queryExecution.search.model.ResultWrapper;
import com.dqs.eventdrivensearch.queryExecution.search.model.SearchResult;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
public class SearchResultQueue {

    private final Queue<ResultWrapper> results = new ConcurrentLinkedQueue<>();


    public void submitResult(ResultWrapper result) {
        results.add(result);
    }

    public List<SearchResult> drainAll() {
        List<SearchResult> drained = new ArrayList<>();
        ResultWrapper wrapper;
        while ((wrapper = results.poll()) != null) {
            drained.add(wrapper.result());
        }
        return drained;
    }
}

