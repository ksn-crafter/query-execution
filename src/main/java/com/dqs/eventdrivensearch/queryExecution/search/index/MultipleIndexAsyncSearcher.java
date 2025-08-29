package com.dqs.eventdrivensearch.queryExecution.search.index;


import com.dqs.eventdrivensearch.queryExecution.search.io.S3SearchResultWriter;
import com.dqs.eventdrivensearch.queryExecution.search.metrics.MetricsPublisher;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;


@Component
public class MultipleIndexAsyncSearcher {

    private final List<SingleIndexAsyncSearcher> singleIndexAsyncSearchers;

    private final MetricsPublisher metricsPublisher;

    private final String outputFolderPath;

    private final List<S3SearchResultWriter> s3SearchResultWriters;

    public MultipleIndexAsyncSearcher(ApplicationContext context,
                                 MetricsPublisher metricsPublisher,
                                 @Value("${single_index_searcher_count}")
                                 int singleIndexSearcherCount,
                                 @Value("${output_folder_path}")
                                 String outputFolderPath
    ) {

        singleIndexAsyncSearchers = new ArrayList<>();
        s3SearchResultWriters = new ArrayList<>();

        for (int idx = 0; idx < singleIndexSearcherCount; idx++) {
            singleIndexAsyncSearchers.add(context.getBean(SingleIndexAsyncSearcher.class));
            s3SearchResultWriters.add(context.getBean(S3SearchResultWriter.class));
        }
        this.metricsPublisher = metricsPublisher;
        this.outputFolderPath = outputFolderPath;
    }

    private static final Logger logger = Logger.getLogger(MultipleIndexSearcher.class.getName());

    public void search(String searchQueryString, String queryId, String[] filePaths, String subQueryId) throws ParseException {
        Instant start = Instant.now();
        List<Future<CompletableFuture<Void>>> searchTasks = new ArrayList<>();
        int totalAvailableCores = Runtime.getRuntime().availableProcessors() - 1;
        final ExecutorService executorService = Executors.newWorkStealingPool(totalAvailableCores);

        try {
            final String queryResultLocation = outputFolderPath.endsWith("/")
                    ? outputFolderPath + queryId
                    : outputFolderPath + "/" + queryId;
            for (int idx = 0; idx < filePaths.length; idx++) {
                SingleIndexAsyncSearcher singleIndexAsyncSearcher = singleIndexAsyncSearchers.get(idx);
                String filePath = filePaths[idx];
                S3SearchResultWriter s3SearchResultWriter = s3SearchResultWriters.get(idx);
                var query = singleIndexAsyncSearcher.getQuery(searchQueryString, new StandardAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET));
                var task = executorService.submit(() -> {
                    try {
                        return searchOnSingleIndex(queryResultLocation, filePath, singleIndexAsyncSearcher, query, queryId, s3SearchResultWriter);
                    } catch (IOException | ParseException e) {
                        System.out.println(String.format("Search for file %s has failed. The query id is %s and sub query id is %s", filePath, queryId, subQueryId));
                        logger.log(Level.WARNING, e.getMessage() + "\n" + e.getStackTrace() + "\n" + "filePath: " + filePath);
                        throw new RuntimeException(e);
                    }
                });
                searchTasks.add(task);
            }
            waitForTasks(searchTasks);

        } catch (Exception e) {
            System.out.println(e.getMessage() + "\n" + e.getStackTrace().toString());
            throw e;
        } finally {
            executorService.shutdown();
            metricsPublisher.putMetricData(MetricsPublisher.MetricNames.INTERNAL_SEARCH_TIME, Duration.between(start, Instant.now()).toMillis(), queryId);
            metricsPublisher.publishToCloudWatch();
        }
    }

    private CompletableFuture<Void> searchOnSingleIndex(String queryResultLocation, String filePath, SingleIndexAsyncSearcher singleIndexAsyncSearcher, Query query, String queryId, S3SearchResultWriter s3SearchResultWriter) throws IOException, ParseException {
        Instant start = Instant.now();

        return singleIndexAsyncSearcher.search(filePath, query, queryId).thenAccept(
                (searchResult) -> {
                    metricsPublisher.putMetricData(MetricsPublisher.MetricNames.DOWNLOAD_INDEX_SHARD_LOAD_INTO_LUCENE_DIRECTORY_AND_SEARCH, Duration.between(start, Instant.now()).toMillis(), queryId);
                    Instant writeStart = Instant.now();
                    s3SearchResultWriter.write(searchResult, queryResultLocation, filePath);
                    metricsPublisher.putMetricData(MetricsPublisher.MetricNames.WRITE_RESULT_TO_S3_FOR_SINGLE_INDEX_SHARD, Duration.between(writeStart, Instant.now()).toMillis(), queryId);
                }
        );

    }

    private void waitForTasks(List<Future<CompletableFuture<Void>>> tasks) {
        var innerFutures = tasks.stream()
                .map(future -> {
                    try {
                        return future.get();
                    } catch (InterruptedException | ExecutionException e) {
                        return CompletableFuture.failedFuture(e);
                    }
                })
                .toList();


        // TODO: requesting gc
        CompletableFuture<Void> allDone = CompletableFuture.allOf(innerFutures.toArray(new CompletableFuture[0]));
        allDone.join();
    }
}
