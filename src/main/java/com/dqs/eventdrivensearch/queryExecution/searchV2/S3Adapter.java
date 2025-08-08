package com.dqs.eventdrivensearch.queryExecution.searchV2;

import com.dqs.eventdrivensearch.queryExecution.search.metrics.MetricsPublisher;
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

@Component
public class S3Adapter {
    private final S3Client s3Client;

    private final MetricsPublisher metricsPublisher;

    public S3Adapter(S3Client s3Client,MetricsPublisher metricsPublisher){
        this.s3Client = s3Client;
        this.metricsPublisher = metricsPublisher;
    }

    private static final java.util.logging.Logger logger = java.util.logging.Logger.getLogger(S3Adapter.class.getName());

    public InputStream getInputStream(String s3IndexUrl, String queryId) {
        InputStream s3IndexInputStream = null;
        Instant start = Instant.now();

        try {
            URL url = new URI(s3IndexUrl).toURL();
            String bucketName = url.getHost().split("\\.")[0];
            String key = url.getPath().substring(1);

            s3IndexInputStream = s3Client.getObject(GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build());

        } catch (MalformedURLException  | URISyntaxException | NoSuchKeyException e) {
            logger.log(Level.WARNING, e.getMessage() + "\n" + Arrays.toString(e.getStackTrace()) + "\n" + "index url: " + s3IndexUrl);
        }

        metricsPublisher.putMetricData(MetricsPublisher.MetricNames.DOWNLOAD_SINGLE_INDEX_SHARD, Duration.between(start, Instant.now()).toMillis(),queryId);
        return s3IndexInputStream;
    }
}
