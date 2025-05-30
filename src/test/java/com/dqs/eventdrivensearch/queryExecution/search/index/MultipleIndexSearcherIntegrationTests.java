package com.dqs.eventdrivensearch.queryExecution.search.index;

import org.apache.lucene.queryparser.classic.ParseException;
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
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@Testcontainers
@ActiveProfiles("test")
public class MultipleIndexSearcherIntegrationTests {
    @Autowired
    private MultipleIndexSearcher searcher;

    @Container
    private static final LocalStackContainer localStack =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:latest"))
                    .withServices(LocalStackContainer.Service.S3);

    @Autowired
    private S3Client s3Client;

    private final String s3ZipFilePath = "https://test-bucket/128MB-chunks/part-00000-0001a714-1dc6-443f-98f5-1a27c467863b-c000.json.gz";

    @TestConfiguration
    static class S3TestConfig {
        @Bean
        @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
        @Primary
        public S3Client s3Client(){
            return S3Client.builder()
                    .endpointOverride(URI.create(localStack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
                    .region(Region.of(localStack.getRegion()))
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(localStack.getAccessKey(),localStack.getSecretKey())))
                    .build();
        }
    }

    @BeforeEach
    public void setup() throws URISyntaxException {
        Path sampleZip = Paths.get(getClass().getClassLoader().getResource("sample.zip").toURI());
        URI s3Uri = URI.create(s3ZipFilePath);
        String bucketName = s3Uri.getHost().split("\\.")[0];
        String key = s3Uri.getPath().substring(1);

        s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
        s3Client.putObject(PutObjectRequest.builder().bucket(bucketName).key(key).build(),
                RequestBody.fromFile(sampleZip));
    }

    @Test
    public void shouldSearchOnIndexAndWriteResultToS3() throws ParseException {
        //note: ensure that the s3OutputPath is same as the OUTPUT_FOLDER_PATH environment variable
        //which is set via SetEnvironmentVariable annotation on the tests
        final String s3OutputPath = "https://test-bucket.s3.us-east-1.amazonaws.com/result";

        String[] indexPaths = {s3ZipFilePath};
        String queryId = "query-1";
        String subQueryId = "sub-query-1";
        searcher.search("Historical",queryId,indexPaths,subQueryId);

        URI s3Uri = URI.create(s3OutputPath + "/" + queryId);
        String bucketName = s3Uri.getHost().split("\\.")[0];
        String prefix = s3Uri.getPath().substring(1);

        ListObjectsV2Response listResp = s3Client.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(prefix)
                .maxKeys(1) // Only need to check presence
                .build());

        boolean fileExists = !listResp.contents().isEmpty();
        assertTrue(fileExists, "Expected at least one search result file in S3!");
    }
}
