package com.dqs.eventdrivensearch.queryExecution.search.index;

import com.dqs.eventdrivensearch.queryExecution.search.io.S3SearchResultWriter;
import com.dqs.eventdrivensearch.queryExecution.search.metrics.MetricsPublisher;
import com.dqs.eventdrivensearch.queryExecution.search.model.SearchResult;
import jakarta.annotation.PreDestroy;
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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;


@Component
public class MultipleIndexSearcher {

    private final List<SingleIndexSearcher> singleIndexSearchers;

    private final MetricsPublisher metricsPublisher;

    private final String outputFolderPath;

    private final List<S3SearchResultWriter> s3SearchResultWriters;

    private final ExecutorService executor;

    public MultipleIndexSearcher(ApplicationContext context,
                                 MetricsPublisher metricsPublisher,
                                 @Value("${single_index_searcher_count}")
                                 int singleIndexSearcherCount,
                                 @Value("${output_folder_path}")
                                 String outputFolderPath
    ) {

        singleIndexSearchers = new ArrayList<>();
        s3SearchResultWriters = new ArrayList<>();


        for (int idx = 0; idx < singleIndexSearcherCount; idx++) {
            singleIndexSearchers.add(context.getBean(SingleIndexSearcher.class));
            s3SearchResultWriters.add(context.getBean(S3SearchResultWriter.class));
        }
        this.metricsPublisher = metricsPublisher;
        this.outputFolderPath = outputFolderPath;
        this.executor = Executors.newWorkStealingPool(Runtime.getRuntime().availableProcessors() - 2);
    }

    private static final Logger logger = Logger.getLogger(MultipleIndexSearcher.class.getName());

    public void search(String searchQueryString, String queryId, String[] filePaths, String subQueryId) throws ParseException {
        Instant start = Instant.now();
        List<Future> searchTasks = new ArrayList<>();
        int totalAvailableCores = Runtime.getRuntime().availableProcessors() - 1;
        final ExecutorService executorService = Executors.newWorkStealingPool(totalAvailableCores);

        try {
            final String queryResultLocation = outputFolderPath.endsWith("/")
                    ? outputFolderPath + queryId
                    : outputFolderPath + "/" + queryId;
            for (int idx = 0; idx < filePaths.length; idx++) {
                SingleIndexSearcher singleIndexSearcher = singleIndexSearchers.get(idx);
                String filePath = filePaths[idx];
                S3SearchResultWriter s3SearchResultWriter = s3SearchResultWriters.get(idx);
                var query = singleIndexSearcher.getQuery(searchQueryString, new StandardAnalyzer());
                var task = executorService.submit(() -> {
                    try {
                        searchOnSingleIndex(queryResultLocation, filePath, singleIndexSearcher, query, queryId, s3SearchResultWriter);
                    } catch (IOException | ParseException e) {
                        System.out.println(String.format("Search for file %s has failed. The query id is %s and sub query id is %s", filePath, queryId, subQueryId));
                        logger.log(Level.WARNING, e.getMessage() + "\n" + e.getStackTrace() + "\n" + "filePath: " + filePath);
                        throw new RuntimeException(e);
                    }
                });
                searchTasks.add(task);
            }

            for (Future task : searchTasks) {
                while (!task.isDone()) ;
            }

        } catch (Exception e) {
            System.out.println(e.getMessage() + "\n" + e.getStackTrace().toString());
            throw e;
        } finally {
            executorService.shutdown();
            metricsPublisher.putMetricData(MetricsPublisher.MetricNames.INTERNAL_SEARCH_TIME, Duration.between(start, Instant.now()).toMillis(), queryId);
            metricsPublisher.publishToCloudWatch();
        }
    }

    public void searchV2(String searchQueryString, String queryId, String[] indexPaths, String subQueryId) throws ParseException {
        Instant start = Instant.now();

        try {
            final String queryResultLocation = outputFolderPath.endsWith("/")
                    ? outputFolderPath + queryId
                    : outputFolderPath + "/" + queryId;

            List<Future<?>> searchTasks = new ArrayList<>();

            //TODO: run these indexes in a threadpool
            for (int idx = 0; idx < indexPaths.length; idx++) {
                SingleIndexSearcher singleIndexSearcher = singleIndexSearchers.get(idx);
                String indexPath = indexPaths[idx];
                S3SearchResultWriter s3SearchResultWriter = s3SearchResultWriters.get(idx);
                var query = singleIndexSearcher.getQuery(searchQueryString, new StandardAnalyzer());
                searchTasks.add(
                        executor.submit(() -> {
                            try {
                                searchOnSplits(queryResultLocation, indexPath, singleIndexSearcher, query, queryId, s3SearchResultWriter);
                            } catch (IOException | ParseException e) {
                                System.out.printf("SearchV2 for split path %s has failed. The query id is %s and sub query id is %s%n", indexPath, queryId, subQueryId);
                                logger.log(Level.SEVERE, e.getMessage() + "\n" + Arrays.toString(e.getStackTrace()) + "\n" + "indexPath: " + indexPath);
                                throw new RuntimeException(e);
                            }
                        }));
            }
            for (Future<?> task : searchTasks) {
                while (!task.isDone()) ;
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            metricsPublisher.putMetricData(MetricsPublisher.MetricNames.INTERNAL_SEARCH_TIME, Duration.between(start, Instant.now()).toMillis(), queryId);
            metricsPublisher.publishToCloudWatch();
        }
    }

    public void searchV3(String searchQueryString, String queryId, String[] indexPaths, String subQueryId) throws ParseException {
        Instant start = Instant.now();

        try {
            final String queryResultLocation = outputFolderPath.endsWith("/")
                    ? outputFolderPath + queryId
                    : outputFolderPath + "/" + queryId;

            List<Future<?>> searchTasks = new ArrayList<>();

            //TODO: run these indexes in a threadpool
            for (int idx = 0; idx < indexPaths.length; idx++) {
                SingleIndexSearcher singleIndexSearcher = singleIndexSearchers.get(idx);
                String indexPath = indexPaths[idx];
                S3SearchResultWriter s3SearchResultWriter = s3SearchResultWriters.get(idx);
                var query = singleIndexSearcher.getQuery(searchQueryString, new StandardAnalyzer());
                searchTasks.add(
                        executor.submit(() -> {
                            try {
                                searchOnZippedSplits(queryResultLocation, indexPath, singleIndexSearcher, query, queryId, s3SearchResultWriter);
                            } catch (IOException | ParseException e) {
                                System.out.printf("SearchV2 for split path %s has failed. The query id is %s and sub query id is %s%n", indexPath, queryId, subQueryId);
                                logger.log(Level.SEVERE, e.getMessage() + "\n" + Arrays.toString(e.getStackTrace()) + "\n" + "indexPath: " + indexPath);
                                throw new RuntimeException(e);
                            }
                        }));
            }
            for (Future<?> task : searchTasks) {
                while (!task.isDone()) ;
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            metricsPublisher.putMetricData(MetricsPublisher.MetricNames.INTERNAL_SEARCH_TIME, Duration.between(start, Instant.now()).toMillis(), queryId);
            metricsPublisher.publishToCloudWatch();
        }
    }


    private void searchOnSingleIndex(String queryResultLocation, String filePath, SingleIndexSearcher singleIndexSearcher, Query query, String queryId, S3SearchResultWriter s3SearchResultWriter) throws IOException, ParseException {
        Instant start = Instant.now();

        SearchResult searchResult = singleIndexSearcher.search(filePath, query, queryId);

        metricsPublisher.putMetricData(MetricsPublisher.MetricNames.DOWNLOAD_INDEX_SHARD_LOAD_INTO_LUCENE_DIRECTORY_AND_SEARCH, Duration.between(start, Instant.now()).toMillis(), queryId);

        start = Instant.now();
        s3SearchResultWriter.write(searchResult, queryResultLocation, filePath);
        metricsPublisher.putMetricData(MetricsPublisher.MetricNames.WRITE_RESULT_TO_S3_FOR_SINGLE_INDEX_SHARD, Duration.between(start, Instant.now()).toMillis(), queryId);
    }

    private void searchOnSplits(String queryResultLocation, String indexPath, SingleIndexSearcher singleIndexSearcher, Query query, String queryId, S3SearchResultWriter s3SearchResultWriter) throws IOException, ParseException {
        Instant start = Instant.now();
        singleIndexSearcher.searchV2(indexPath, query, queryId, queryResultLocation);
        metricsPublisher.putMetricData(MetricsPublisher.MetricNames.DOWNLOAD_SPLITS_LOAD_INTO_LUCENE_DIRECTORY_AND_SEARCH, Duration.between(start, Instant.now()).toMillis(), queryId);
    }

    private void searchOnZippedSplits(String queryResultLocation, String indexPath, SingleIndexSearcher singleIndexSearcher, Query query, String queryId, S3SearchResultWriter s3SearchResultWriter) throws IOException, ParseException {
        Instant start = Instant.now();
        singleIndexSearcher.searchV3(indexPath, query, queryId, queryResultLocation);
        metricsPublisher.putMetricData(MetricsPublisher.MetricNames.DOWNLOAD_SPLITS_LOAD_INTO_LUCENE_DIRECTORY_AND_SEARCH, Duration.between(start, Instant.now()).toMillis(), queryId);
    }

    @PreDestroy
    public void clean() {
        this.executor.shutdown();
    }
}