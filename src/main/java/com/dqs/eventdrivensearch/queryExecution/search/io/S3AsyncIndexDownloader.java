package com.dqs.eventdrivensearch.queryExecution.search.io;

import com.dqs.eventdrivensearch.queryExecution.search.metrics.MetricsPublisher;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
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

    public CompletableFuture<Directory> downloadAndUnzipToByteBuffer(String filePath, String queryId, Path outputDir) {

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

            return /*s3AsyncClient.getObject(getObjectRequest, AsyncResponseTransformer.toFile(Paths.get(localZipFilePath)))*/
                    s3AsyncClient.getObject(getObjectRequest, AsyncResponseTransformer.toBlockingInputStream())
                    .thenApply((result) -> {
                        metricsPublisher.putMetricData(MetricsPublisher.MetricNames.DOWNLOAD_SINGLE_INDEX_SHARD, Duration.between(start, Instant.now()).toMillis(), queryId);
                        return unzipToByteBuffer(/*localZipFilePath,*/ result,  queryId, outputDir);
                    })
                    .exceptionally(ex->{
                        throw new RuntimeException(ex.getMessage());
                    });

        } catch (MalformedURLException | NoSuchKeyException e) {
            logger.log(Level.WARNING, e.getMessage() + "\n" + e.getStackTrace() + "\n" + "filePath: " + filePath);
            throw new RuntimeException(e);
        }

    }

    private Directory unzipToByteBuffer(InputStream inputStream,/*String localZipFilePath,*/ String queryId, Path outputDir) {
        Instant start = Instant.now();
        Directory byteBufferDirectory = new ByteBuffersDirectory();
        final int OPTIMAL_STREAM_BUFFER_SIZE = 1048576;
        try (ZipInputStream zipIn = new ZipInputStream(new BufferedInputStream(inputStream))) {
            byte[] zipStreamBuffer = new byte[OPTIMAL_STREAM_BUFFER_SIZE];
            ZipEntry entry;
            while ((entry = zipIn.getNextEntry()) != null) {
                if (!entry.isDirectory()) {
                    try (IndexOutput output = byteBufferDirectory.createOutput(entry.getName(), IOContext.DEFAULT)) {
                        int len;
                        while ((len = zipIn.read(zipStreamBuffer)) > 0) {
                            output.writeBytes(zipStreamBuffer, 0, len);
                        }
                    }
                }
                zipIn.closeEntry();
            }

//            deleteTempDirectory(outputDir.toFile());
//            deleteDownloadedFile(outputDir);
        } catch (Exception e) {
            System.out.println(e);
        }
        metricsPublisher.putMetricData(MetricsPublisher.MetricNames.UNZIP_SINGLE_INDEX_SHARD, Duration.between(start, Instant.now()).toMillis(), queryId);

        return byteBufferDirectory;
    }


    private void deleteTempDirectory(File directory) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (!file.isDirectory()) {
                    file.delete();
                }
            }
        }
    }

    private void deleteDownloadedFile(Path targetTempDirectory) {
        File file = new File(targetTempDirectory.toAbsolutePath() + ".zip");
        if (file.exists()) {
            file.delete();
        }
    }


}

