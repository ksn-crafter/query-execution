package com.dqs.eventdrivensearch.queryExecution.searchV2;

import com.dqs.eventdrivensearch.queryExecution.model.SearchResult;
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
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;

@Component
public class IndexDownloader {
    private final MetricsPublisher metricsPublisher;
    private final Semaphore numberOfVitrualThreadsSemaphore;
    private final SearchExecutorService searchThreadPool;
    private final ZippedIndex  zippedIndex;
    private final EmailIndex emailIndex;
    private final Executor virtualThreadExecutor = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());
    private static final java.util.logging.Logger logger = java.util.logging.Logger.getLogger(IndexDownloader.class.getName());

    public IndexDownloader(SearchExecutorService searchThreadPool,ZippedIndex zippedIndex,EmailIndex emailIndex, MetricsPublisher metricsPublisher, @Value("${number_of_virtual_threads_for_download}") int numberOfVirtualThreadsForDownload) {
        this.metricsPublisher = metricsPublisher;
        numberOfVitrualThreadsSemaphore = new Semaphore(numberOfVirtualThreadsForDownload);
        this.searchThreadPool = searchThreadPool;
        this.zippedIndex = zippedIndex;
        this.emailIndex = emailIndex;
    }

    public CompletableFuture<Void> downloadIndices(List<S3IndexLocation> s3IndexLocations, String queryId, String subQueryId, String searchTerm) {
        if(s3IndexLocations == null || s3IndexLocations.isEmpty()) {
            throw new IllegalArgumentException("s3Locations cannot be null or empty");
        }
        return CompletableFuture.allOf(
                s3IndexLocations.stream()
                        .map(loc -> downloadIndex(loc, queryId, subQueryId, searchTerm))
                        .toArray(CompletableFuture[]::new)
        );
    }

    public CompletableFuture<Void> downloadIndex(S3IndexLocation s3IndexLocation, String queryId, String subQueryId, String searchTerm) {
        return CompletableFuture.supplyAsync(() -> {
            numberOfVitrualThreadsSemaphore.acquireUninterruptibly();
            try {
                InputStream indexInputStream = s3IndexLocation.downloadAsStream(queryId);
                Path indexDirectory = unzipToDirectory(indexInputStream, queryId);

                return searchThreadPool.submit(new SearchTask(
                    emailIndex.getQuery(searchTerm),
                    queryId,
                    subQueryId,
                    s3IndexLocation.toString(),
                    indexDirectory
                ));
            } catch (IOException | ParseException e) {
                logger.log(Level.SEVERE, e.getMessage(), e);
                return CompletableFuture.<SearchResult>failedFuture(e);
            } finally {
                numberOfVitrualThreadsSemaphore.release();
            }
        }, virtualThreadExecutor)
        .thenCompose(f -> f.thenApply(r -> null));
    }

    private Path unzipToDirectory(InputStream indexInputStream, String queryId) throws IOException {
        Instant start = Instant.now();
        try (indexInputStream) {
            return zippedIndex.unzip(indexInputStream);
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.getMessage() + "\n" + Arrays.toString(e.getStackTrace()) + "\n" + "queryId: " + queryId);
        } finally {
            metricsPublisher.putMetricData(MetricsPublisher.MetricNames.UNZIP_SINGLE_INDEX_SHARD, Duration.between(start, Instant.now()).toMillis(), queryId);
        }
        return null;
    }

    private static Query getQuery(String queryString, StandardAnalyzer analyzer) throws ParseException {
        final String[] DOCUMENT_FIELDS = {"body", "subject", "date", "from", "to", "cc", "bcc"};

        MultiFieldQueryParser parser = new MultiFieldQueryParser(DOCUMENT_FIELDS, analyzer);
        return parser.parse(queryString);
    }
}
