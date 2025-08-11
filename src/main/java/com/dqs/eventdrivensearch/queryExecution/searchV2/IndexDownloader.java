package com.dqs.eventdrivensearch.queryExecution.searchV2;

import com.dqs.eventdrivensearch.queryExecution.model.SearchTask;
import com.dqs.eventdrivensearch.queryExecution.search.metrics.MetricsPublisher;
import com.dqs.eventdrivensearch.queryExecution.searchV2.executors.SearchExecutorService;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;

import static com.dqs.eventdrivensearch.queryExecution.search.utils.Utilities.readAndUnzipInDirectory;

@Component
public class IndexDownloader {
    private final MetricsPublisher metricsPublisher;
    private final Semaphore numberOfVitrualThreadsSemaphore;
    private final SearchExecutorService searchThreadPool;

    private static final java.util.logging.Logger logger = java.util.logging.Logger.getLogger(IndexDownloader.class.getName());

    public IndexDownloader(SearchExecutorService searchThreadPool,MetricsPublisher metricsPublisher, @Value("${number_of_virtual_threads_for_download:2}") int numberOfVirtualThreadsForDownload) {
        this.metricsPublisher = metricsPublisher;
        numberOfVitrualThreadsSemaphore = new Semaphore(numberOfVirtualThreadsForDownload);
        this.searchThreadPool = searchThreadPool;
    }

    public void downloadIndices(List<S3IndexLocation> s3Locations, String queryId, String subQueryId, String searchTerm) {
        if(s3Locations == null || s3Locations.isEmpty()) {
            throw new IllegalArgumentException("s3Locations cannot be null or empty");
        }
        for (S3IndexLocation s3Location : s3Locations) {
            downloadIndex(s3Location, queryId,subQueryId,searchTerm);
        }
    }

    public void downloadIndex(S3IndexLocation s3IndexLocation, String queryId,String subQueryId,String searchTerm) {
        numberOfVitrualThreadsSemaphore.acquireUninterruptibly();
        Thread.startVirtualThread(() -> {
            try {
                InputStream indexInputStream = s3IndexLocation.downloadAsStream(queryId);
                Path indexDirectory = unzipToDirectory(indexInputStream, queryId);
                System.out.println(String.format("Downloaded index %s from %s", indexDirectory.toAbsolutePath(), s3IndexLocation));
                searchThreadPool.submit(new SearchTask(getQuery(searchTerm,new StandardAnalyzer()),
                                                            queryId,
                                                            subQueryId,
                                                            s3IndexLocation.toString(),
                                                            indexDirectory));
                System.out.println("Line after submit search task to search thread pool:");
            } catch (IOException | ParseException  e) {
                //log the exception for now, eventually this should add up as a metric to final results
                //as a skipped/failed document search count
                System.out.println("Exception aya hain");
                logger.log(Level.SEVERE, e.getMessage() + "\n" + Arrays.toString(e.getStackTrace()) + "\n" + "index url: " + s3IndexLocation);
            } finally {
                numberOfVitrualThreadsSemaphore.release();
            }
        });
    }

    private Path unzipToDirectory(InputStream indexInputStream, String queryId) throws IOException {
        Instant start = Instant.now();
        Path indexDirectory = Files.createTempDirectory("indexDir-");

        try {
            readAndUnzipInDirectory(indexInputStream, indexDirectory);
        } catch (IOException e) {
            //log the exception for now, eventually this should add up as a metric to final results
            //as a skipped/failed document search count
            logger.log(Level.SEVERE, e.getMessage() + "\n" + Arrays.toString(e.getStackTrace()) + "\n" + "queryId: " + queryId);
        } finally {
            metricsPublisher.putMetricData(MetricsPublisher.MetricNames.UNZIP_SINGLE_INDEX_SHARD, Duration.between(start, Instant.now()).toMillis(), queryId);
            indexInputStream.close();
        }
        return indexDirectory;
    }

    private static Query getQuery(String queryString, StandardAnalyzer analyzer) throws ParseException {
        final String[] DOCUMENT_FIELDS = {"body", "subject", "date", "from", "to", "cc", "bcc"};

        MultiFieldQueryParser parser = new MultiFieldQueryParser(DOCUMENT_FIELDS, analyzer);
        return parser.parse(queryString);
    }
}
