//package com.dqs.eventdrivensearch.queryExecution.consumer;
//
//import com.dqs.eventdrivensearch.queryExecution.event.SubQueryGenerated;
//import com.dqs.eventdrivensearch.queryExecution.model.QueryDescription;
//import com.dqs.eventdrivensearch.queryExecution.model.QueryStatus;
//import com.dqs.eventdrivensearch.queryExecution.model.SubQuery;
//import com.dqs.eventdrivensearch.queryExecution.repository.QueryDescriptionRepository;
//import com.dqs.eventdrivensearch.queryExecution.repository.SubQueryRepository;
//import de.flapdoodle.embed.mongo.MongodExecutable;
//import de.flapdoodle.embed.mongo.MongodStarter;
//import de.flapdoodle.embed.mongo.config.MongodConfig;
//import de.flapdoodle.embed.mongo.config.Net;
//import de.flapdoodle.embed.mongo.distribution.Version;
//import de.flapdoodle.embed.process.runtime.Network;
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.common.serialization.StringSerializer;
//import org.junit.jupiter.api.AfterAll;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.TestInstance;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.beans.factory.config.ConfigurableBeanFactory;
//import org.springframework.boot.test.context.SpringBootTest;
//import org.springframework.boot.test.context.TestConfiguration;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Primary;
//import org.springframework.context.annotation.Scope;
//import org.springframework.kafka.core.DefaultKafkaProducerFactory;
//import org.springframework.kafka.core.KafkaTemplate;
//import org.springframework.kafka.core.ProducerFactory;
//import org.springframework.kafka.support.serializer.JsonSerializer;
//import org.springframework.kafka.test.EmbeddedKafkaBroker;
//import org.springframework.kafka.test.context.EmbeddedKafka;
//import org.springframework.kafka.test.utils.KafkaTestUtils;
//import org.springframework.test.context.ActiveProfiles;
//import org.springframework.test.context.DynamicPropertyRegistry;
//import org.springframework.test.context.DynamicPropertySource;
//import org.testcontainers.containers.localstack.LocalStackContainer;
//import org.testcontainers.junit.jupiter.Container;
//import org.testcontainers.utility.DockerImageName;
//import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
//import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
//import software.amazon.awssdk.core.sync.RequestBody;
//import software.amazon.awssdk.regions.Region;
//import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
//import software.amazon.awssdk.services.s3.S3Client;
//import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
//import software.amazon.awssdk.services.s3.model.PutObjectRequest;
//
//import java.net.URI;
//import java.net.URISyntaxException;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.time.LocalDateTime;
//import java.util.Map;
//import java.util.Optional;
//import java.util.concurrent.TimeUnit;
//
//import static org.assertj.core.api.Assertions.assertThat;
//
//@SpringBootTest
//@TestInstance(TestInstance.Lifecycle.PER_CLASS)
//@ActiveProfiles("test")
//@EmbeddedKafka(partitions = 1, topics = {"incoming_sub_queries_jpmc"})
//class SubQueryGeneratedConsumerIntegrationTests {
//
//    private final String s3ZipFilePath = "https://test-bucket-2/128MB-chunks/part-00000-0001a714-1dc6-443f-98f5-1a27c467863b-c000.json.gz";
//    private KafkaTemplate<String, SubQueryGenerated> kafkaTemplate;
//
//    @Container
//    private static final LocalStackContainer localStack =
//            new LocalStackContainer(DockerImageName.parse("localstack/localstack:latest"))
//                    .withServices(LocalStackContainer.Service.S3)
//                    .withServices(LocalStackContainer.Service.CLOUDWATCH)
//                    .withServices(LocalStackContainer.Service.CLOUDWATCHLOGS);
//
//
//    @Autowired
//    private SubQueryRepository subQueryRepository;
//
//    @Autowired
//    private QueryDescriptionRepository queryDescriptionRepository;
//
//    @Autowired
//    private EmbeddedKafkaBroker embeddedKafkaBroker;
//
//    @Autowired
//    private S3Client s3Client;
//
//    private static final MongodExecutable mongodExecutable;
//
//    private static final int mongoPort;
//
//    @TestConfiguration
//    static class AwsClientTestConfig {
//        @Bean
//        @Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
//        @Primary
//        public S3Client s3Client(){
//            return S3Client.builder()
//                    .endpointOverride(URI.create(localStack.getEndpointOverride(LocalStackContainer.Service.S3).toString()))
//                    .region(Region.of(localStack.getRegion()))
//                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(localStack.getAccessKey(),localStack.getSecretKey())))
//                    .build();
//        }
//
//        @Bean
//        @Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
//        @Primary
//        public CloudWatchClient cloudWatchClient() {
//            return CloudWatchClient.builder()
//                    .endpointOverride(URI.create(localStack.getEndpointOverride(LocalStackContainer.Service.CLOUDWATCH).toString()))
//                    .region(Region.of(localStack.getRegion()))
//                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(localStack.getAccessKey(),localStack.getSecretKey())))
//                    .build();
//        }
//    }
//
//    static {
//        try {
//            mongoPort = Network.getFreeServerPort();
//            MongodConfig config = MongodConfig.builder()
//                    .version(Version.Main.V6_0)
//                    .net(new Net(mongoPort, Network.localhostIsIPv6()))
//                    .build();
//            mongodExecutable = MongodStarter.getDefaultInstance().prepare(config);
//            mongodExecutable.start();
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    @DynamicPropertySource
//    static void setMongoProperties(DynamicPropertyRegistry registry) {
//        registry.add("spring.data.mongodb.uri", () -> "mongodb://localhost:" + mongoPort + "/dqs");
//    }
//
//    @AfterAll
//    public void stopEmbeddedMongo() {
//        mongodExecutable.stop();
//    }
//
//    @BeforeEach
//    void setup() throws URISyntaxException {
//        Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
//        senderProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//        senderProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
//
//        ProducerFactory<String, SubQueryGenerated> producerFactory = new DefaultKafkaProducerFactory<>(senderProps);
//        kafkaTemplate = new KafkaTemplate<>(producerFactory);
//
//        Path sampleZip = Paths.get(getClass().getClassLoader().getResource("sample.zip").toURI());
//        URI s3Uri = URI.create(s3ZipFilePath);
//        String bucketName = s3Uri.getHost().split("\\.")[0];
//        String key = s3Uri.getPath().substring(1);
//
//        s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
//        s3Client.putObject(PutObjectRequest.builder().bucket(bucketName).key(key).build(),
//                RequestBody.fromFile(sampleZip));
//    }
//
//
//
//    @Test
//    void updatesQueryDescriptionAndSaveSubQuery() {
//        queryDescriptionRepository.save(new QueryDescription("query-510", "Deutsche", "Historical", 2001, 2007, QueryStatus.Acknowledged, LocalDateTime.now()));
//        String[] indexPaths = {s3ZipFilePath, s3ZipFilePath};
//
//        kafkaTemplate.send("incoming_sub_queries_jpmc", new SubQueryGenerated("query-510", "subquery-1", indexPaths, 2));
//
//        // we are waiting for the mongo transaction to finish here
//        try {
//            TimeUnit.SECONDS.sleep(5);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//
//        Optional<SubQuery> subQuery = subQueryRepository.findBySubQueryId("subquery-1");
//        assertThat(subQuery.get().subQueryId()).isEqualTo("subquery-1");
//        assertThat(subQuery.get().totalSubQueries()).isEqualTo(2);
//
//        QueryDescription queryDescription = queryDescriptionRepository.findByQueryId("query-510").get();
//        assertThat(queryDescription.status()).isEqualTo(QueryStatus.InProgress);
//    }
//TODO: fix this, this test case was failing silently as we were marking subquery completed even if search fails now we have changed that logic.
//
//    @Test
//    void updatesQueryDescriptionStatusToCompleted() {
//        queryDescriptionRepository.save(new QueryDescription("query-520", "Deutsche", "Historical", 2001, 2007, QueryStatus.Acknowledged, LocalDateTime.now()));
//
//        String[] indexPaths = {s3ZipFilePath, s3ZipFilePath};
//        kafkaTemplate.send("incoming_sub_queries_jpmc", new SubQueryGenerated("query-520", "subquery-1520", indexPaths, 2));
//        kafkaTemplate.send("incoming_sub_queries_jpmc", new SubQueryGenerated("query-520", "subquery-2520", indexPaths, 2));
//
//        // we are waiting for the mongo transaction to finish here
//        try {
//            TimeUnit.SECONDS.sleep(5);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//
//        Optional<SubQuery> subQuery = subQueryRepository.findBySubQueryId("subquery-1520");
//        assertThat(subQuery.get().subQueryId()).isEqualTo("subquery-1520");
//        assertThat(subQuery.get().totalSubQueries()).isEqualTo(2);
//
//        QueryDescription queryDescription = queryDescriptionRepository.findByQueryId("query-520").get();
//        assertThat(queryDescription.status()).isEqualTo(QueryStatus.Completed);
//    }
//}
