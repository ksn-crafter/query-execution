package com.dqs.eventdrivensearch.queryExecution.searchV2.executors;

import com.dqs.eventdrivensearch.queryExecution.searchV2.EmailIndex;
import com.dqs.eventdrivensearch.queryExecution.model.SearchResult;
import com.dqs.eventdrivensearch.queryExecution.model.SearchTask;
import com.dqs.eventdrivensearch.queryExecution.searchV2.ZippedIndex;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Testcontainers
@ActiveProfiles("test")
public class SearchExecutorServiceTest {
    private static final String BUCKET = "test-bucket";
    @Autowired
    private SearchExecutorService searchExecutorService;
    static Path indexFilePath;

    @Autowired
    private EmailIndex emailIndex;

    @Autowired
    private S3Client s3Client;

    @Container
    private static final LocalStackContainer localStack =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:latest"))
                    .withServices(LocalStackContainer.Service.S3);

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
    void setup() throws IOException {
        ZippedIndex zippedIndex = new ZippedIndex();
        try (InputStream inputStream = SearchExecutorServiceTest.class.getClassLoader().getResourceAsStream("sample.zip")) {
            indexFilePath = zippedIndex.unzip(inputStream);
        }
        s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
    }


    @Test
    void shouldExecuteSearchTaskAndReturnCorrectResults() throws ExecutionException, InterruptedException, ParseException {
        Query query = emailIndex.getQuery("Historical");
        SearchTask task = new SearchTask(query, "query-1", "subquery-1", "path/to/s3",indexFilePath);
        CompletableFuture<SearchResult> result = searchExecutorService.submit(task);
        Assertions.assertEquals(1048, result.get().totalHits());
    }

    @Test
    void shouldExecuteSearchTaskAndWriteResultsToS3() throws ExecutionException, InterruptedException, ParseException {
        Query query = emailIndex.getQuery("Historical");
        SearchTask task = new SearchTask(query, "query-2", "subquery-1", "path/to/s3",indexFilePath);
        searchExecutorService.submit(task).get();

        ListObjectsV2Response listResponse = s3Client.listObjectsV2(
                ListObjectsV2Request.builder()
                        .bucket(BUCKET)
                        .prefix("result/query-2/")
                        .build()
        );

        assertThat(listResponse.contents()).isNotEmpty();

    }
}
