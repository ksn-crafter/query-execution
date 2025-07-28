package com.dqs.eventdrivensearch.queryExecution.search.io;


import com.dqs.eventdrivensearch.queryExecution.search.metrics.MetricsPublisher;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class S3IndexDownloader {
    private static final Logger logger = Logger.getLogger(S3IndexDownloader.class.getName());

    private S3Client s3Client;

    private MetricsPublisher metricsPublisher;

    public S3IndexDownloader(S3Client s3Client,MetricsPublisher metricsPublisher){
        this.s3Client = s3Client;
        this.metricsPublisher = metricsPublisher;
    }

    public InputStream getInputStream(String filePath,String queryId) {
        InputStream inputStream = null;
        Instant start = Instant.now();

        try {
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

        metricsPublisher.putMetricData(MetricsPublisher.MetricNames.DOWNLOAD_SINGLE_INDEX_SHARD, Duration.between(start, Instant.now()).toMillis(),queryId);
        return inputStream;
    }

    public List<S3Object> getListing(String bucketName, String prefix) {
        ListObjectsV2Request listReq = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(prefix)
                .build();

        return s3Client.listObjectsV2(listReq).contents();

    }

    public Path downloadFile(String key, String bucket, Path tempDir) throws IOException {

        String fileName = Paths.get(key).getFileName().toString();
        Path downloadedFile = tempDir.resolve(fileName);


        GetObjectRequest getReq = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        s3Client.getObject(getReq, ResponseTransformer.toFile(downloadedFile));

        return downloadedFile;

    }

}
