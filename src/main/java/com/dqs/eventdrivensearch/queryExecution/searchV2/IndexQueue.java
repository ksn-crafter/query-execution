package com.dqs.eventdrivensearch.queryExecution.searchV2;


import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

@Component
public class IndexQueue {
    final BlockingQueue<Path> indexPaths;

    public IndexQueue() {
        //TODO: decide between an array or synchronous queue
        //TODO: need to do something about this hardcoding
        this.indexPaths = new ArrayBlockingQueue<>(64);
    }

    public void put(Path indexPath) throws InterruptedException {
        indexPaths.put(indexPath);
    }
}
