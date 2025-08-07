package com.dqs.eventdrivensearch.queryExecution.searchV2;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

@Component
public class IndexQueue {
    private final BlockingQueue<Path> indexPaths;

    public IndexQueue(@Value("${number_of_index_files}")int numberOfIndexFiles) {
        //TODO: decide between an array or synchronous queue
        this.indexPaths = new ArrayBlockingQueue<>(numberOfIndexFiles);
    }

    public void put(Path indexPath) throws InterruptedException {
        indexPaths.put(indexPath);
    }
}
