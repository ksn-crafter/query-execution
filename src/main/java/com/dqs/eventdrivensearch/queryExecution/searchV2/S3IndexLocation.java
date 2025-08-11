package com.dqs.eventdrivensearch.queryExecution.searchV2;

import com.dqs.eventdrivensearch.queryExecution.search.metrics.MetricsPublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.logging.Level;

public class S3IndexLocation {
    private final String bucket;
    private final String key;

    private S3Client s3Client;
    private final MetricsPublisher metricsPublisher;

    private static final java.util.logging.Logger logger = java.util.logging.Logger.getLogger(S3IndexLocation.class.getName());

    S3IndexLocation(String s3IndexUrl, S3Client s3Client, MetricsPublisher metricsPublisher) throws URISyntaxException, MalformedURLException {
        URL url = new URI(s3IndexUrl).toURL();
        this.bucket = url.getHost().split("\\.")[0];
        this.key = url.getPath().substring(1);
        if (this.bucket.isEmpty() || this.key.isEmpty()) {
            throw new MalformedURLException("Invalid S3 index url: " + s3IndexUrl);
        }
        this.s3Client = s3Client;
        this.metricsPublisher = metricsPublisher;
    }

    public InputStream downloadAsStream(String queryId) {
        Instant start = Instant.now();
        try {
            return s3Client.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build());
        } catch (NoSuchKeyException e) {
            logger.log(Level.WARNING, e.getMessage() + "\n" + Arrays.toString(e.getStackTrace()) + "\n" + "bucket: " + bucket + ", key: " + key);
        } finally {
            metricsPublisher.putMetricData(MetricsPublisher.MetricNames.DOWNLOAD_SINGLE_INDEX_SHARD, Duration.between(start, Instant.now()).toMillis(), queryId);
        }

        return null;
    }
}
