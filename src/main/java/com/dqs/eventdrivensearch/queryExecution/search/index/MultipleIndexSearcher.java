package com.dqs.eventdrivensearch.queryExecution.search.index;

import com.dqs.eventdrivensearch.queryExecution.search.io.EnvironmentVars;
import com.dqs.eventdrivensearch.queryExecution.search.metrics.MetricsPublisher;
import com.dqs.eventdrivensearch.queryExecution.search.io.S3SeachResultWriter;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;


@Component
public class MultipleIndexSearcher {

    private final List<SingleIndexSearcher> singleIndexSearchers;

    @Value("${single_index_searcher_count}")
    private int singleIndexSearcherCount;

    private final ExecutorService executorService;

    public MultipleIndexSearcher(ApplicationContext context){
        int totalAvailableCores = Runtime.getRuntime().availableProcessors() - 1;
        executorService = Executors.newWorkStealingPool(totalAvailableCores);
        singleIndexSearchers = new ArrayList<>();
        for(int idx=0;idx<singleIndexSearcherCount;idx++){
            singleIndexSearchers.add(idx,context.getBean(SingleIndexSearcher.class));
        }
    }

    private static final Logger logger = Logger.getLogger(MultipleIndexSearcher.class.getName());

    public void search(String searchQueryString, String queryId, String[] filePaths) throws ParseException {
        Instant start = Instant.now();

        final String queryResultLocation = EnvironmentVars.getOutPutFolderPath().endsWith("/")
                ? EnvironmentVars.getOutPutFolderPath() + queryId
                : EnvironmentVars.getOutPutFolderPath() + "/" + queryId;
        List<Future> searchTasks = new ArrayList<>();

        try {
            for (int idx = 0; idx < filePaths.length; idx++) {
                SingleIndexSearcher singleIndexSearcher  = singleIndexSearchers.get(idx);
                String filePath = filePaths[idx];
                var query = singleIndexSearcher.getQuery(searchQueryString, new StandardAnalyzer());
                var task = executorService.submit(() -> {
                    try {
                        searchOnSingleIndex(queryResultLocation, filePath, singleIndexSearcher, query, queryId);
                    } catch (IOException | ParseException e) {
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
            System.out.println(e.getMessage() + "\n" + e.getStackTrace());
        } finally {
            executorService.shutdown();
            MetricsPublisher.putMetricData(MetricsPublisher.MetricNames.INTERNAL_SEARCH_TIME, Duration.between(start, Instant.now()).toMillis(), queryId);
        }
    }

    private static void searchOnSingleIndex(String queryResultLocation, String filePath, SingleIndexSearcher singleIndexSearcher, Query query, String queryId) throws IOException, ParseException {
        Instant start = Instant.now();

        S3SeachResultWriter s3SeachResultWriter = singleIndexSearcher.search(filePath, query, queryId);

        MetricsPublisher.putMetricData(MetricsPublisher.MetricNames.DOWNLOAD_INDEX_SHARD_LOAD_INTO_LUCENE_DIRECTORY_AND_SEARCH, Duration.between(start, Instant.now()).toMillis(), queryId);

        start = Instant.now();
        s3SeachResultWriter.write(queryResultLocation, filePath);
        MetricsPublisher.putMetricData(MetricsPublisher.MetricNames.WRITE_RESULT_TO_S3_FOR_SINGLE_INDEX_SHARD, Duration.between(start, Instant.now()).toMillis(), queryId);
    }
}