package com.dqs.eventdrivensearch.queryExecution.search.queue;

import com.dqs.eventdrivensearch.queryExecution.search.model.SearchTask;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
public class SearchTaskQueue {

    private final BlockingQueue<SearchTask> queue = new LinkedBlockingQueue<>();

    public void submitTask(SearchTask task) {
        queue.offer(task);
    }

    public SearchTask takeTask() throws InterruptedException {
        return queue.take(); // blocks until task is available
    }
}