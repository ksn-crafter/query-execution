package com.dqs.eventdrivensearch.queryExecution.searchV2;

import com.dqs.eventdrivensearch.queryExecution.search.metrics.MetricsPublisher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@Component
public class IndexDownloader {
    private final IndexQueue indexLocalDirectoryPaths;
    private final S3Adapter s3Adapter;
    private final MetricsPublisher metricsPublisher;
    private final Semaphore numberOfVitrualThreadsSemaphore;

    private static final java.util.logging.Logger logger = java.util.logging.Logger.getLogger(S3Adapter.class.getName());

    public IndexDownloader(IndexQueue indexLocalDirectoryPaths, S3Adapter s3Adapter, MetricsPublisher metricsPublisher, @Value("${number_of_virtual_threads_for_download}") int numberOfVirtualThreadsForDownload) {
        this.indexLocalDirectoryPaths = indexLocalDirectoryPaths;
        this.s3Adapter = s3Adapter;
        this.metricsPublisher = metricsPublisher;
        numberOfVitrualThreadsSemaphore = new Semaphore(numberOfVirtualThreadsForDownload);
    }

    public void downloadIndices(String[] s3IndexUrls, String queryId) {
        if (s3IndexUrls == null || s3IndexUrls.length == 0) {
            throw new IllegalArgumentException("s3IndexUrls cannot be null or empty");
        }
        for (String s3IndexUrl : s3IndexUrls) {
            numberOfVitrualThreadsSemaphore.acquireUninterruptibly();
            Thread.startVirtualThread(() -> {
                try {
                    downloadIndex(s3IndexUrl, queryId);
                } catch (InterruptedException | IOException e) {
                    //log the exception for now, eventually this should add up as a metric to final results
                    //as a skipped/failed document search count
                    logger.log(Level.SEVERE, e.getMessage() + "\n" + Arrays.toString(e.getStackTrace()) + "\n" + "index url: " + s3IndexUrl);
                } finally {
                    numberOfVitrualThreadsSemaphore.release();
                }
            });
        }
    }

    private void downloadIndex(String s3IndexUrl, String queryId) throws InterruptedException, IOException {
        InputStream indexInputStream = s3Adapter.getInputStream(s3IndexUrl, queryId);
        indexLocalDirectoryPaths.put(unzipToDirectory(indexInputStream, queryId));
    }

    private Path unzipToDirectory(InputStream indexInputStream, String queryId) throws IOException {
        Instant start = Instant.now();
        Path indexDirectory = Files.createTempDirectory("indexDir-");

        final int OPTIMAL_STREAM_BUFFER_SIZE = 1048576;
        try (ZipInputStream zipIn = new ZipInputStream(new BufferedInputStream(indexInputStream, OPTIMAL_STREAM_BUFFER_SIZE))) {
            byte[] zipStreamBuffer = new byte[OPTIMAL_STREAM_BUFFER_SIZE];
            ZipEntry entry;
            while ((entry = zipIn.getNextEntry()) != null) {
                Path filePath = indexDirectory.resolve(entry.getName());
                if (!entry.isDirectory()) {
                    // Extract file
                    try (BufferedOutputStream indexOutputStream = new BufferedOutputStream(new FileOutputStream(filePath.toFile()), OPTIMAL_STREAM_BUFFER_SIZE)) {
                        int length;
                        while ((length = zipIn.read(zipStreamBuffer)) > 0) {
                            indexOutputStream.write(zipStreamBuffer, 0, length);
                        }
                    }
                } else {
                    // Create directory
                    Files.createDirectories(filePath);
                }
                zipIn.closeEntry();
            }
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
}
