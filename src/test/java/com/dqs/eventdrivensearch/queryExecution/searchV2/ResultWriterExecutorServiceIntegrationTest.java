package com.dqs.eventdrivensearch.queryExecution.searchV2;

import com.dqs.eventdrivensearch.queryExecution.model.SearchResult;
import com.dqs.eventdrivensearch.queryExecution.searchV2.executors.ResultWriterExecutorService;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Scope;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Testcontainers
@ActiveProfiles("test")
class ResultWriterExecutorServiceIntegrationTest {
    @Autowired
    private ResultWriterExecutorService resultWriterExecutorService;

    @Autowired
    private S3Client s3Client;

    @Container
    private static final LocalStackContainer localStack =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:latest"))
                    .withServices(LocalStackContainer.Service.S3)
                    .withServices(LocalStackContainer.Service.CLOUDWATCH)
                    .withServices(LocalStackContainer.Service.CLOUDWATCHLOGS);


    private static final String BUCKET = "test-bucket";
    private static final String KEY = "results/result.json";
    private static final String S3_URL = "https://" + BUCKET + "/" + KEY;

    @TestConfiguration
    static class AwsClientTestConfig {
        @Bean
        @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
        @Primary
        public S3Client s3Client() {
            return S3Client.builder()
                    .endpointOverride(URI.create(localStack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
                    .region(Region.of(localStack.getRegion()))
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(localStack.getAccessKey(), localStack.getSecretKey())))
                    .build();
        }

        @Bean
        @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
        @Primary
        public CloudWatchClient cloudWatchClient() {
            return CloudWatchClient.builder()
                    .endpointOverride(URI.create(localStack.getEndpointOverride(LocalStackContainer.Service.CLOUDWATCH).toString()))
                    .region(Region.of(localStack.getRegion()))
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(localStack.getAccessKey(), localStack.getSecretKey())))
                    .build();
        }
    }

    @BeforeEach
    void setUp() {
        s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
    }

    @Test
    void shouldWriteResultToS3() {
        SearchResult searchResult = new SearchResult(List.of("doc1,1.0", "doc2,0.9"), 2, 2L, 2);
        resultWriterExecutorService.submit("query-1", searchResult, S3_URL).join();

        ListObjectsV2Response listResponse = s3Client.listObjectsV2(
                ListObjectsV2Request.builder()
                        .bucket(BUCKET)
                        .prefix("result/query-1/")
                        .build()
        );

        assertThat(listResponse.contents()).isNotEmpty();
    }

    @Test
    void shouldWriteResultToS3WithCorrectDocuments() throws IOException {
        SearchResult searchResult = new SearchResult(List.of("doc1,1.0", "doc2,0.9"), 2, 2L, 2);
        resultWriterExecutorService.submit("query-1", searchResult, S3_URL).join();

        ListObjectsV2Response listResponse = s3Client.listObjectsV2(
                ListObjectsV2Request.builder()
                        .bucket(BUCKET)
                        .prefix("result/query-1/")
                        .build()
        );

        String actualKey = listResponse.contents().getFirst().key();

        String content = new String(
                s3Client.getObject(GetObjectRequest.builder().bucket(BUCKET).key(actualKey).build())
                        .readAllBytes(),
                StandardCharsets.UTF_8
        );

        assertThat(content).contains("doc1", "doc2");

        long docLineCount = content.lines().filter(line -> line.contains("doc")).count();
        assertThat(docLineCount).isEqualTo(searchResult.totalHits());
    }
}
