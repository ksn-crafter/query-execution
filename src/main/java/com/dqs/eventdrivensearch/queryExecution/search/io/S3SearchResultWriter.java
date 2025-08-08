package com.dqs.eventdrivensearch.queryExecution.search.io;

import com.dqs.eventdrivensearch.queryExecution.search.metrics.MetricsPublisher;
import com.dqs.eventdrivensearch.queryExecution.search.model.SearchResult;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class S3SearchResultWriter {

    final private S3Client s3Client;
    final MetricsPublisher metricsPublisher;

    public S3SearchResultWriter(S3Client s3Client, MetricsPublisher metricsPublisher) {
        this.s3Client = s3Client;
        this.metricsPublisher = metricsPublisher;
    }

    public void write(SearchResult searchResult, String outPutFolderPath, String indexFilePath) {

        String filePath = getFilePath(searchResult, outPutFolderPath);

        try {
            URL url = new URL(filePath);
            String bucketName = url.getHost().split("\\.")[0];
            String key = url.getPath().substring(1);

            String content = getStringV2(searchResult.documentIds(), indexFilePath);

            s3Client.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .build(),
                    RequestBody.fromString(content)
            );
        } catch (MalformedURLException | NoSuchKeyException e) {
            System.out.println(Level.WARNING + e.getMessage() + "\n" + Arrays.toString(e.getStackTrace()) + "\n" + "outPutFolderPath: " + outPutFolderPath);
            throw new RuntimeException(e);
        }
    }

    public void write(SearchResult searchResult, String outPutFolderPath, String indexFilePath, String queryId) {

        String filePath = getFilePath(searchResult, outPutFolderPath);

        try {
            URL url = new URL(filePath);
            String bucketName = url.getHost().split("\\.")[0];
            String key = url.getPath().substring(1);

//            metricsPublisher.putMetricData(MetricsPublisher.MetricNames.NUMBER_OF_MATCHING_DOCS_IN_INDEX, searchResult.documentIds().size(), queryId);


//            Instant start = Instant.now();
            String content = getStringV2(searchResult.documentIds(), indexFilePath);
//            metricsPublisher.putMetricData(MetricsPublisher.MetricNames.CONTENT_GENERATION_FROM_SEARCH_RESULT, Duration.between(start, Instant.now()).toMillis(), queryId);

//            Instant putObjectStart = Instant.now();
            s3Client.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .build(),
                    RequestBody.fromString(content)
            );

//            metricsPublisher.putMetricData(MetricsPublisher.MetricNames.RESULT_PUT_OBJECT, Duration.between(putObjectStart, Instant.now()).toMillis(), queryId);

        } catch (MalformedURLException | NoSuchKeyException e) {
            System.out.println(Level.WARNING + e.getMessage() + "\n" + Arrays.toString(e.getStackTrace()) + "\n" + "outPutFolderPath: " + outPutFolderPath);
            throw new RuntimeException(e);
        }
    }

    static String getString(List<String> documentIds, String indexFilePath) {
        return indexFilePath + "\n" + String.join("\n", documentIds);
    }

    static String getStringV2(List<String> documentIds, String indexFilePath) {
        int totalLength = indexFilePath.length() + 1; // for '\n'
        for (String id : documentIds) totalLength += id.length() + 1; // +1 for '\n'

        StringBuilder builder = new StringBuilder(totalLength);
        builder.append(indexFilePath).append('\n');

        for (String documentId : documentIds) {
            builder.append(documentId);
            builder.append('\n');
        }
        return builder.toString();
    }

    static byte[] getStringV3(List<String> documentIds, String indexFilePath) throws IOException {
        int totalLength = indexFilePath.length() + 1; // for '\n'
        for (String id : documentIds) totalLength += id.length() + 1; // +1 for '\n'

        ByteArrayOutputStream out = new ByteArrayOutputStream(totalLength);
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))) {
            writer.write(indexFilePath);
            writer.write('\n');
            for (String id : documentIds) {
                writer.write(id);
                writer.write('\n');
            }
            writer.flush();
        }
        return out.toByteArray();
    }

    private String getFilePath(SearchResult searchResult, String folderPath) {
        return folderPath + "/" + UUID.randomUUID() + "_" + searchResult.total() + "_" + searchResult.totalHits() + "_" + searchResult.totalMatchedDocuments() + ".txt";
    }
}