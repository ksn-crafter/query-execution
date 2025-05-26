package com.dqs.eventdrivensearch.queryExecution.search.metrics;


import com.dqs.eventdrivensearch.queryExecution.search.io.EnvironmentVars;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MetricsPublisher {
    public enum MetricNames {
        INTERNAL_SEARCH_TIME, DOWNLOAD_INDEX_SHARD_LOAD_INTO_LUCENE_DIRECTORY_AND_SEARCH, WRITE_RESULT_TO_S3_FOR_SINGLE_INDEX_SHARD, DOWNLOAD_SINGLE_INDEX_SHARD, UNZIP_SINGLE_INDEX_SHARD, SEARCH_SINGLE_INDEX_SHARD
    }

    private static final Logger logger = Logger.getLogger(MetricsPublisher.class.getName());

    private static CloudWatchClient cloudWatch = CloudWatchClient.builder().region(Region.US_EAST_1).build();

    private static ConcurrentLinkedQueue<MetricDatum> metrics = new ConcurrentLinkedQueue<>();

    public static void putMetricData(MetricNames metricName, long value,String queryId) {
        try {
            List<Dimension> dimensions = new ArrayList<>();

            dimensions.add(Dimension.builder().name("RUN_ID").value(queryId).build());

            MetricDatum datum = MetricDatum.builder().metricName(String.valueOf(metricName)).unit(StandardUnit.MILLISECONDS).value((double) value).timestamp(Instant.now()).dimensions(dimensions).storageResolution(1).build();

            metrics.add(datum);

        } catch (Exception e) {
            logger.log(Level.WARNING, e.getMessage() + "\n" + e.getStackTrace());
            throw new RuntimeException(e);
        }
    }

    public static void publishToCloudWatch() {
        List<MetricDatum> tempMetrics = new ArrayList<>();
        for (MetricDatum metric : metrics) {
            tempMetrics.add(metric);

            if (tempMetrics.size() % 100 == 0) {
                PutMetricDataRequest request = PutMetricDataRequest.builder().namespace(EnvironmentVars.getCloudwachNamespace()).metricData(tempMetrics).build();
                cloudWatch.putMetricData(request);
                tempMetrics.clear();
            }
        }

        if (!tempMetrics.isEmpty()) {
            PutMetricDataRequest request = PutMetricDataRequest.builder().namespace(EnvironmentVars.getCloudwachNamespace()).metricData(tempMetrics).build();
            cloudWatch.putMetricData(request);
            tempMetrics.clear();
        }


        metrics.clear();
    }

    public static void close() {
        cloudWatch.close();
    }
}
