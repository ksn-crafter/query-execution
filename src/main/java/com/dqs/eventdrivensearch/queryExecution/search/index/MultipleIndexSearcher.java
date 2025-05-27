package com.dqs.eventdrivensearch.queryExecution.search.index;

import com.dqs.eventdrivensearch.queryExecution.search.io.EnvironmentVars;
import com.dqs.eventdrivensearch.queryExecution.search.io.FileStream;
import com.dqs.eventdrivensearch.queryExecution.search.metrics.MetricsPublisher;
import com.dqs.eventdrivensearch.queryExecution.search.model.SearchResult;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MultipleIndexSearcher {

    private static final Logger logger = Logger.getLogger(MultipleIndexSearcher.class.getName());

    public void search(String searchQueryString, String queryId, String[] filePaths) throws ParseException {
        Instant start = Instant.now();

        SingleIndexSearcher singleIndexSearcher = new SingleIndexSearcher();
        final String queryResultLocation = EnvironmentVars.getOutPutFolderPath().endsWith("/")
                                            ? EnvironmentVars.getOutPutFolderPath() + queryId
                                            : EnvironmentVars.getOutPutFolderPath() + "/" + queryId;
        List<Future> tasks = new ArrayList<>();

        var query = singleIndexSearcher.getQuery(searchQueryString, new StandardAnalyzer());
        ExecutorService executorService = Executors.newWorkStealingPool(Runtime.getRuntime().availableProcessors()-1);
        try{
            for (String filePath : filePaths) {
                var task = executorService.submit(() -> {
                    try {
                        processIndexFile(queryResultLocation, filePath, singleIndexSearcher, query,queryId);
                    } catch (IOException | ParseException e) {
                        logger.log(Level.WARNING, e.getMessage() + "\n" + e.getStackTrace() + "\n" + "filePath: " + filePath);
                        throw new RuntimeException(e);
                    }
                });
                tasks.add(task);
            }

            for (Future task : tasks) {
                while (!task.isDone()) ;
            }

        }catch(Exception e){
            System.out.println(e.getMessage() + "\n" + e.getStackTrace());
        }finally {
            executorService.shutdown();
            MetricsPublisher.putMetricData(MetricsPublisher.MetricNames.INTERNAL_SEARCH_TIME, Duration.between(start, Instant.now()).toMillis(),queryId);
        }
    }

    private static void processIndexFile(String queryResultLocation, String filePath, SingleIndexSearcher singleIndexSearcher, Query query, String queryId) throws IOException, ParseException {
        Instant start = Instant.now();

        SearchResult searchResult = singleIndexSearcher.search(filePath, query,queryId);

        MetricsPublisher.putMetricData(MetricsPublisher.MetricNames.DOWNLOAD_INDEX_SHARD_LOAD_INTO_LUCENE_DIRECTORY_AND_SEARCH, Duration.between(start, Instant.now()).toMillis(),queryId);

        start = Instant.now();
        FileStream.writeToTxtFile(searchResult, queryResultLocation, filePath);
        MetricsPublisher.putMetricData(MetricsPublisher.MetricNames.WRITE_RESULT_TO_S3_FOR_SINGLE_INDEX_SHARD, Duration.between(start, Instant.now()).toMillis(),queryId);
    }
}