package com.dqs.eventdrivensearch.queryExecution.searchV2;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest
@Testcontainers
@ActiveProfiles("test")
public class IndexDownloaderIntegrationTests {
    @Autowired
    private IndexDownloader indexDownloader;

    @Container
    private static final LocalStackContainer localStack =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:latest"))
                    .withServices(LocalStackContainer.Service.S3)
                    .withServices(LocalStackContainer.Service.CLOUDWATCH)
                    .withServices(LocalStackContainer.Service.CLOUDWATCHLOGS);

    @Autowired
    private S3Client s3Client;

    @Autowired
    private S3IndexLocationFactory s3IndexLocationFactory;


    @Value("${number_of_virtual_threads_for_download}")
    private int numberOfVirtualThreadsForDownload;

    @Value("${number_of_downloaded_indexes_in_queue}")
    private int numberOfDownloadedIndexesInQueue;

    private final String s3IndexUrl = "https://test-bucket/128MB-chunks/part-00000-0001a714-1dc6-443f-98f5-1a27c467863b-c000.json.gz";

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
    public void setup() throws URISyntaxException {
        Path sampleZip = Paths.get(getClass().getClassLoader().getResource("sample.zip").toURI());
        URI s3Uri = URI.create(s3IndexUrl);
        String bucketName = s3Uri.getHost().split("\\.")[0];
        String key = s3Uri.getPath().substring(1);

        s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
        s3Client.putObject(PutObjectRequest.builder().bucket(bucketName).key(key).build(),
                RequestBody.fromFile(sampleZip));
    }

    @Test
    public void shouldDownloadIndicesPerformSearchAndWriteResultToS3() {
        List<S3IndexLocation> s3IndexLocations = List.of(s3IndexLocationFactory.create(s3IndexUrl), s3IndexLocationFactory.create(s3IndexUrl));
        CompletableFuture<Void> allStepsFuture = indexDownloader.downloadIndices(s3IndexLocations, "query-1", "", "test");
        allStepsFuture.join();

        URI s3Uri = URI.create(s3IndexUrl);
        String bucket = s3Uri.getHost().split("\\.")[0];
        ListObjectsV2Response listResponse = s3Client.listObjectsV2(
                ListObjectsV2Request.builder()
                        .bucket(bucket)
                        .prefix("result/query-1/")
                        .build()
        );

        assertThat(listResponse.contents()).isNotEmpty();
    }

    @Test
    public void shouldNotAcceptEmptyIndexPathsForDownloads() {
        assertThrows(IllegalArgumentException.class, () -> {
            List<S3IndexLocation> s3IndexLocations = new ArrayList<>();
            indexDownloader.downloadIndices(s3IndexLocations, "", "", "test");
        });
    }
}

