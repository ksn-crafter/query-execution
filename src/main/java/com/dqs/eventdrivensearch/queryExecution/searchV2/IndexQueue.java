package com.dqs.eventdrivensearch.queryExecution.searchV2;


import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

@Component
public class IndexQueue {
    final BlockingQueue<Path> indexPaths;

    public IndexQueue(@Value("${number_of_downloaded_indexes_in_queue}") int numberOfDownloadedIndexesInQueue) {
        this.indexPaths = new ArrayBlockingQueue<>(numberOfDownloadedIndexesInQueue);
    }

    public void put(Path indexPath) throws InterruptedException {
        indexPaths.put(indexPath);
    }
}
