package com.dqs.eventdrivensearch.queryExecution.searchV2;

import com.dqs.eventdrivensearch.queryExecution.search.metrics.MetricsPublisher;
import com.dqs.eventdrivensearch.queryExecution.search.utils.Utilities;
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

import static com.dqs.eventdrivensearch.queryExecution.search.utils.Utilities.readAndUnzipInDirectory;

@Component
public class IndexDownloader {
    private final MetricsPublisher metricsPublisher;
    private final Semaphore numberOfVitrualThreadsSemaphore;

    private static final java.util.logging.Logger logger = java.util.logging.Logger.getLogger(IndexDownloader.class.getName());

    public IndexDownloader(MetricsPublisher metricsPublisher, @Value("${number_of_virtual_threads_for_download:2}") int numberOfVirtualThreadsForDownload) {
        this.metricsPublisher = metricsPublisher;
        numberOfVitrualThreadsSemaphore = new Semaphore(numberOfVirtualThreadsForDownload);
    }

    public void downloadIndices(S3IndexLocation[] s3Locations, String queryId) {
        for (S3IndexLocation s3Location : s3Locations) {
            downloadIndex(s3Location, queryId);
        }
    }

    public void downloadIndex(S3IndexLocation s3IndexLocation, String queryId) {
        numberOfVitrualThreadsSemaphore.acquireUninterruptibly();
        Thread.startVirtualThread(() -> {
            try {
                InputStream indexInputStream = s3IndexLocation.downloadAsStream(queryId);
                unzipToDirectory(indexInputStream, queryId);
            } catch (IOException e) {
                //log the exception for now, eventually this should add up as a metric to final results
                //as a skipped/failed document search count
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
}
