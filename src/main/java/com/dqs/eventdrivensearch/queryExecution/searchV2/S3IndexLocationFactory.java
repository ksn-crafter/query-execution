package com.dqs.eventdrivensearch.queryExecution.searchV2;

import com.dqs.eventdrivensearch.queryExecution.search.metrics.MetricsPublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.s3.S3Client;

@Component
public class S3IndexLocationFactory {
    @Autowired
    private S3Client s3Client;

    @Autowired
    private MetricsPublisher metricsPublisher;

    public S3IndexLocation create(String s3IndexUrl){
        try {
            return new S3IndexLocation(s3IndexUrl,s3Client,metricsPublisher);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
