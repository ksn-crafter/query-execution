package com.dqs.eventdrivensearch.queryExecution.searchV2;

import com.dqs.eventdrivensearch.queryExecution.model.SearchResult;
import com.dqs.eventdrivensearch.queryExecution.search.metrics.MetricsPublisher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;

@Component
public class ResultWriter {
    private static final java.util.logging.Logger logger = java.util.logging.Logger.getLogger(ResultWriter.class.getName());
    private final Semaphore numberOfVitrualThreadsSemaphore;
    private final S3Client s3Client;
    private final String outPutFolderPath;
    private final MetricsPublisher metricsPublisher;
    private final Executor virtualThreadExecutor = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());

    public ResultWriter(
            @Value("${number_of_virtual_threads_for_write:2}") 
            int numberOfVirtualThreadsForWrite, 
            S3Client s3Client,
            @Value("${output_folder_path}")
            String outPutFolderPath,
            MetricsPublisher metricsPublisher) {
        numberOfVitrualThreadsSemaphore = new Semaphore(numberOfVirtualThreadsForWrite);
        this.s3Client = s3Client;
        this.outPutFolderPath = outPutFolderPath;
        this.metricsPublisher = metricsPublisher;
    }

    public CompletableFuture<Void> writeResult(String queryId, SearchResult searchResult, String indexFilePath) {
        return CompletableFuture.runAsync(() -> {
            numberOfVitrualThreadsSemaphore.acquireUninterruptibly();
            try {
                write(queryId, searchResult, indexFilePath);
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Error writing result", e);
            } finally {
                numberOfVitrualThreadsSemaphore.release();
            }
        }, virtualThreadExecutor);
    }

    public void write(String queryId, SearchResult searchResult, String indexFilePath) {
        Instant start = Instant.now();
        String filePath = getFilePath(queryId, searchResult, outPutFolderPath);

        try {
            URL url = new URL(filePath);
            String bucketName = url.getHost().split("\\.")[0];
            String key = url.getPath().substring(1);

            String content = indexFilePath + "\n" + String.join("\n", searchResult.documentIds());
            s3Client.putObject(PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build(), RequestBody.fromString(content));

        } catch (MalformedURLException | NoSuchKeyException e) {
            logger.log(Level.WARNING, "message", e);
            throw new RuntimeException(e);
        } finally {
            metricsPublisher.putMetricData(MetricsPublisher.MetricNames.WRITE_RESULT_TO_S3_FOR_SINGLE_INDEX_SHARD, Duration.between(start, Instant.now()).toMillis(), queryId);
        }
    }

    private String getFilePath(String queryId, SearchResult searchResult, String folderPath) {
        return folderPath + "/" + queryId + "/" + UUID.randomUUID() + "_" + searchResult.total() + "_" + searchResult.totalHits() + "_" + searchResult.totalMatchedDocuments() + ".txt";
    }
}
