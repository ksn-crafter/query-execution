package com.dqs.eventdrivensearch.queryExecution.search.index;

import com.dqs.eventdrivensearch.queryExecution.search.model.ResultWrapper;
import com.dqs.eventdrivensearch.queryExecution.search.model.SearchResult;
import com.dqs.eventdrivensearch.queryExecution.search.model.SearchTask;
import com.dqs.eventdrivensearch.queryExecution.search.queue.SearchResultQueue;
import com.dqs.eventdrivensearch.queryExecution.search.queue.SearchTaskQueue;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
public class IndexSearcher {
    private static final Logger logger = Logger.getLogger(IndexSearcher.class.getName());
    private final ExecutorService executorService = Executors.newWorkStealingPool();

    @Autowired
    SearchTaskQueue searchTaskQueue;
    @Autowired
    SearchResultQueue searchResultQueue;

    @PostConstruct
    public void init() {
        startBackgroundTask();
    }

    private void startBackgroundTask() {
        Thread dispatcherThread = new Thread(() -> {
            while (true) {
                try {
                    SearchTask task = searchTaskQueue.takeTask();
                    Thread searchThread = getSearchThread(task);
                    executorService.submit(searchThread);

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.log(Level.WARNING, "exception starting background search thread", e);
                }
            }
        });

        dispatcherThread.setDaemon(true);
        executorService.submit(dispatcherThread);
    }


    private SearchResult search(Path indexDirectory, Query query) throws IOException {
        SearchResult searchResult = null;
        try (Directory directory = new MMapDirectory(indexDirectory)) {
            DirectoryReader reader = DirectoryReader.open(directory);
            org.apache.lucene.search.IndexSearcher searcher = new org.apache.lucene.search.IndexSearcher(reader);
            searchResult = readResults(searcher, query);
        } finally {
            deleteTempDirectory(indexDirectory.toFile());
        }

        return searchResult;
    }

    private SearchResult readResults(org.apache.lucene.search.IndexSearcher searcher, Query query) throws IOException {

        final int optimalPageSize = 30000; // Number of results per page
        ScoreDoc lastDoc = null; // Starting point for pagination (null for first page)
        List<String> documentIds = new ArrayList<>();
        Long totalHits = null;

        while (true) {
            // Perform the search, either starting fresh or after the last document of the previous page
            TopDocs results = (lastDoc == null) ? searcher.search(query, optimalPageSize)
                    : searcher.searchAfter(lastDoc, query, optimalPageSize);

            /*
                TO BE DISCUSSED - Mismatch in return types:
                "results.totalHits.value" returns long & "searcher.getIndexReader().numDocs()" returns int
             */

            totalHits = results.totalHits.value;

            // Process the current page of results
            for (ScoreDoc scoreDoc : results.scoreDocs) {
                Document hitDoc = searcher.getIndexReader().storedFields().document(scoreDoc.doc, Set.of("id"));
                documentIds.add(hitDoc.getField("id").stringValue() + "," + scoreDoc.score);
            }

            // Update lastDoc to the last document of the current page
            lastDoc = results.scoreDocs.length > 0 ? results.scoreDocs[results.scoreDocs.length - 1] : null;

            // If fewer results than optimalPageSize were returned, we're done
            if (results.scoreDocs.length < optimalPageSize) {
                break;
            }
        }

        return new SearchResult(documentIds, searcher.getIndexReader().numDocs(), totalHits, documentIds.size());
    }

    private void deleteTempDirectory(File directory) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (!file.isDirectory()) {
                    file.delete();
                }
            }
        }
    }

    private Thread getSearchThread(SearchTask task) {
        Thread taskThread = new Thread(() -> {
            try {
                SearchResult result = search(task.indexPath(), task.query());
                searchResultQueue.submitResult(new ResultWrapper(task.queryId(), task.subQueryId(), result));
            } catch (Exception e) {
                logger.log(Level.WARNING, String.format("Index search failed for queryId=%s, subQueryId=%s, indexPath=%s", task.queryId(), task.subQueryId(), task.indexPath()), e);
            }
        });

        taskThread.setName("search-task-" + task.subQueryId());
        return taskThread;
    }

    @PreDestroy
    public void shutdown() {
        executorService.shutdown();
    }

}

