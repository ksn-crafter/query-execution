package com.dqs.eventdrivensearch.queryExecution.search.io;

import com.dqs.eventdrivensearch.queryExecution.search.metrics.MetricsPublisher;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@Component
public class S3AsyncIndexDownloader {
    private static final Logger logger = Logger.getLogger(S3AsyncIndexDownloader.class.getName());
    private S3AsyncClient s3AsyncClient;

    private MetricsPublisher metricsPublisher;

    public S3AsyncIndexDownloader(S3AsyncClient s3AsyncClient,MetricsPublisher metricsPublisher){
        this.s3AsyncClient = s3AsyncClient;
        this.metricsPublisher = metricsPublisher;
    }

    public CompletableFuture<Void> downloadAndUnzipIndex(String filePath, String queryId, Path outputDir) {
        InputStream inputStream = null;
        Instant start = Instant.now();

        try {
            URL url = new URL(filePath);
            String bucketName = url.getHost().split("\\.")[0];
            String key = url.getPath().substring(1);

            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            String localZipFilePath = outputDir.toAbsolutePath() + ".zip";

            return s3AsyncClient.getObject(getObjectRequest, AsyncResponseTransformer.toFile(Paths.get(localZipFilePath)))
                    .thenAccept((result) -> {
                        metricsPublisher.putMetricData(MetricsPublisher.MetricNames.DOWNLOAD_SINGLE_INDEX_SHARD, Duration.between(start, Instant.now()).toMillis(), queryId);
                        unzipTheDownloadedFile(outputDir, localZipFilePath, queryId);
                    })
                    .exceptionally(ex->{
                        throw new RuntimeException(ex.getMessage());
                    });

        } catch (MalformedURLException | NoSuchKeyException e) {
            logger.log(Level.WARNING, e.getMessage() + "\n" + e.getStackTrace() + "\n" + "filePath: " + filePath);
            throw new RuntimeException(e);
        }

    }

    private void unzipTheDownloadedFile(Path outputDir, String localZipFilePath, String queryId) {
        Instant start = Instant.now();
        final int OPTIMAL_STREAM_BUFFER_SIZE = 1048576;
        try (ZipInputStream zipIn = new ZipInputStream(new BufferedInputStream(new FileInputStream(localZipFilePath), OPTIMAL_STREAM_BUFFER_SIZE))) {
            byte[] zipStreamBuffer = new byte[OPTIMAL_STREAM_BUFFER_SIZE];
            ZipEntry entry;
            while ((entry = zipIn.getNextEntry()) != null) {
                Path filePath = outputDir.resolve(entry.getName());
                if (!entry.isDirectory()) {
                    try (BufferedOutputStream bos = new BufferedOutputStream(Files.newOutputStream(filePath), OPTIMAL_STREAM_BUFFER_SIZE)) {
                        int len;
                        while ((len = zipIn.read(zipStreamBuffer)) > 0) {
                            bos.write(zipStreamBuffer, 0, len);
                        }
                    } catch (IOException e) {
                        System.out.println(e.getMessage() + "\n" + e.getStackTrace());
                    }

                } else {
                    Files.createDirectories(filePath);
                }
                zipIn.closeEntry();
            }
        } catch (IOException e) {
            System.out.println(e.getMessage() + "\n" + e.getStackTrace());
        }

        metricsPublisher.putMetricData(MetricsPublisher.MetricNames.UNZIP_SINGLE_INDEX_SHARD, Duration.between(start, Instant.now()).toMillis(), queryId);
    }


}
