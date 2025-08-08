package com.dqs.eventdrivensearch.queryExecution.searchV2;

import com.dqs.eventdrivensearch.queryExecution.search.metrics.MetricsPublisher;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.logging.Level;

@Component
public class S3Adapter {
    private S3Client s3Client;

    private MetricsPublisher metricsPublisher;

    public S3Adapter(S3Client s3Client,MetricsPublisher metricsPublisher){
        this.s3Client = s3Client;
        this.metricsPublisher = metricsPublisher;
    }

    private static final java.util.logging.Logger logger = java.util.logging.Logger.getLogger(S3Adapter.class.getName());

    public InputStream getInputStream(String s3IndexUrl, String queryId) {
        InputStream s3IndexInputStream = null;
        Instant start = Instant.now();

        try {
            URL url = new URL(s3IndexUrl);
            String bucketName = url.getHost().split("\\.")[0];
            String key = url.getPath().substring(1);

            ResponseInputStream<?> responseInputStream = s3Client.getObject(GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build());
            s3IndexInputStream = responseInputStream;

        } catch (MalformedURLException | NoSuchKeyException e) {
            logger.log(Level.WARNING, e.getMessage() + "\n" + e.getStackTrace() + "\n" + "index url: " + s3IndexUrl);
            throw new RuntimeException(e);
        }

        metricsPublisher.putMetricData(MetricsPublisher.MetricNames.DOWNLOAD_SINGLE_INDEX_SHARD, Duration.between(start, Instant.now()).toMillis(),queryId);
        return s3IndexInputStream;
    }
}
