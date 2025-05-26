package com.dqs.eventdrivensearch.queryExecution.search.io;


import com.dqs.eventdrivensearch.queryExecution.search.metrics.MetricsPublisher;
import com.dqs.eventdrivensearch.queryExecution.search.model.SearchResult;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FileStream {
    private static final Logger logger = Logger.getLogger(FileStream.class.getName());

    public static void writeToTxtFile(SearchResult searchResult, String outPutFolderPath, String documentFilePath) {

        List<String> documentIds = searchResult.documentIds();

        String filePath = getFilePath(searchResult, outPutFolderPath);

        try (S3Client s3Client = getS3Client()) {

            URL url = new URL(filePath);
            String bucketName = url.getHost().split("\\.")[0];
            String key = url.getPath().substring(1);

            String content = documentFilePath + "\n" + String.join("\n", documentIds);
            s3Client.putObject(PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build(), RequestBody.fromString(content));

        } catch (MalformedURLException | NoSuchKeyException e) {
            logger.log(Level.WARNING, e.getMessage() + "\n" + e.getStackTrace() + "\n" + "outPutFolderPath: " + outPutFolderPath);
            throw new RuntimeException(e);
        }
    }

    public static String getFilePath(SearchResult searchResult, String folderPath) {
        return folderPath + "/" + UUID.randomUUID() + "_" + searchResult.total() + "_" + searchResult.totalHits() + "_" + searchResult.totalMatchedDocuments() + ".txt";
    }

    public static S3Client getS3Client() {
        return S3Client.builder()
                .region(Region.US_EAST_1)
                .build();
    }

    public static InputStream getInputStream(String filePath,String queryId) {
        InputStream inputStream = null;
        Instant start = Instant.now();

        try {
            S3Client s3Client = getS3Client();
            URL url = new URL(filePath);
            String bucketName = url.getHost().split("\\.")[0];
            String key = url.getPath().substring(1);

            ResponseInputStream<?> responseInputStream = s3Client.getObject(GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build());
            inputStream = responseInputStream;

        } catch (MalformedURLException | NoSuchKeyException e) {
            logger.log(Level.WARNING, e.getMessage() + "\n" + e.getStackTrace() + "\n" + "filePath: " + filePath);
            throw new RuntimeException(e);
        }

        MetricsPublisher.putMetricData(MetricsPublisher.MetricNames.DOWNLOAD_SINGLE_INDEX_SHARD, Duration.between(start, Instant.now()).toMillis(),queryId);
        return inputStream;
    }
}
