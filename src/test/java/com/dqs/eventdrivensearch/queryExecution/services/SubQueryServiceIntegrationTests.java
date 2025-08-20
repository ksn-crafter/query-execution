package com.dqs.eventdrivensearch.queryExecution.services;

import com.dqs.eventdrivensearch.queryExecution.event.SubQueryGenerated;
import com.dqs.eventdrivensearch.queryExecution.model.SubQuery;
import com.dqs.eventdrivensearch.queryExecution.model.SubQueryStatus;
import com.dqs.eventdrivensearch.queryExecution.repository.SubQueryRepository;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfig;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.process.runtime.Network;
import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Scope;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import de.flapdoodle.embed.mongo.distribution.Version;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;


@ActiveProfiles("test")
@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EmbeddedKafka(partitions = 1, topics = {"incoming_sub_queries_jpmc"})
public class SubQueryServiceIntegrationTests {
    private KafkaTemplate<String, SubQueryGenerated> kafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private SubQueryService subQueryService;

    @Autowired
    private SubQueryRepository subQueryRepository;


    @TestConfiguration
    static class S3TestConfig {
        @Bean
        @Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
        @Primary
        public S3Client s3Client() {
            return S3Client.builder()
                    .region(Region.US_EAST_1)
                    .build();
        }

        @Bean
        @Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
        @Primary
        public S3AsyncClient s3AsyncClient() {
            return S3AsyncClient.crtBuilder()
                    .region(Region.US_EAST_1)
                    .minimumPartSizeInBytes(4 * 1024 * 1024L)
                    .build();
        }


        @Bean
        @Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
        @Primary
        public CloudWatchClient cloudWatchClient() {
            return CloudWatchClient.builder()
                    .region(Region.US_EAST_1)
                    .build();
        }
    }

    private static final MongodExecutable mongodExecutable;

    private static final int mongoPort;

    static{
        try {
            mongoPort = Network.getFreeServerPort();
            MongodConfig mongodConfig = MongodConfig.builder()
                    .version(Version.Main.V6_0)
                    .net(new Net(mongoPort,Network.localhostIsIPv6()))
                    .build();
            mongodExecutable = MongodStarter.getDefaultInstance().prepare(mongodConfig);
            mongodExecutable.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @DynamicPropertySource
    static void setMongoProperties(DynamicPropertyRegistry registry){
        registry.add("spring.data.mongodb.uri", () -> "mongodb://localhost:" + mongoPort + "/dqs");
    }

    @AfterAll
    public void stopEmbeddedMongo(){
        mongodExecutable.stop();
    }

    @Test
    public void shouldAddOnlyOneEntryForAGivenSubQueryId(){
        String[] indexPaths = {"index-1"};
        subQueryRepository.save(new SubQuery("query-1","sub-query-1",indexPaths,1,"default"));
        subQueryService.save(new SubQuery("query-1","sub-query-1",indexPaths,1,"default"));
        Optional<List<SubQuery>> subQueries = subQueryRepository.findAllBySubQueryId("sub-query-1");
        assertThat(subQueries.get().size()).isEqualTo(1);
    }

    @Test
    void completeASubQuery() {
        subQueryRepository.deleteAllById(List.of("1", "2"));
        subQueryRepository.save(new SubQuery("1", "query-1", "subquery-1", 2, SubQueryStatus.COMPLETED, LocalDateTime.now()));
        subQueryRepository.save(new SubQuery("2", "query-1", "subquery-2", 2, SubQueryStatus.IN_PROGRESS));

        subQueryService.completeSubQuery("query-1", "subquery-2");

        boolean allDone = subQueryService.areAllSubQueriesDone("query-1");

        assertThat(allDone).isTrue();
    }

    @Test
    void completeASubQueryWithCompletionTime() {
        subQueryRepository.deleteAllById(List.of("1", "2"));
        subQueryRepository.save(new SubQuery("1", "query-1", "subquery-1", 2, SubQueryStatus.COMPLETED, LocalDateTime.now()));
        subQueryRepository.save(new SubQuery("2", "query-1", "subquery-2", 2, SubQueryStatus.IN_PROGRESS));

        subQueryService.completeSubQuery("query-1", "subquery-2");

        SubQuery subQuery = subQueryRepository.findByQueryIdAndSubQueryId("query-1", "subquery-2").get();

        AssertionsForClassTypes.assertThat(subQuery.completionTime()).isNotNull();
    }

    @Test
    void attemptToCompleteSubqueryWithAnInvalidQueryId() {
        assertThrows(RuntimeException.class, () -> {
            subQueryService.completeSubQuery("anything", "subquery-2");
        });
    }

    @Test
    void allSubQueriesAreNotDone() {
        subQueryRepository.deleteAllById(List.of("1", "2"));
        subQueryRepository.save(new SubQuery("1", "query-1", "subquery-1", 2, SubQueryStatus.COMPLETED, LocalDateTime.now()));
        subQueryRepository.save(new SubQuery("2", "query-1", "subquery-2", 2, SubQueryStatus.IN_PROGRESS));

        boolean allDone = subQueryService.areAllSubQueriesDone("query-1");

        AssertionsForClassTypes.assertThat(allDone).isFalse();
    }

    @Test
    void allSubQueriesAreNotDoneDespiteCompletingOneSubQuery() {
        subQueryRepository.deleteAllById(List.of("101", "201", "301"));
        subQueryRepository.save(new SubQuery("101", "query-10", "subquery-10", 3, SubQueryStatus.COMPLETED, LocalDateTime.now()));
        subQueryRepository.save(new SubQuery("201", "query-10", "subquery-20", 3, SubQueryStatus.IN_PROGRESS));
        subQueryRepository.save(new SubQuery("301", "query-10", "subquery-30", 3, SubQueryStatus.IN_PROGRESS));

        subQueryService.completeSubQuery("query-10", "subquery-30");

        boolean allDone = subQueryService.areAllSubQueriesDone("query-10");

        AssertionsForClassTypes.assertThat(allDone).isFalse();
    }

    @Test
    void allSubQueriesAreDone() {
        subQueryRepository.deleteAllById(List.of("102", "202", "302"));
        subQueryRepository.save(new SubQuery("102", "query-15", "subquery-10", 3, SubQueryStatus.COMPLETED, LocalDateTime.now()));
        subQueryRepository.save(new SubQuery("202", "query-15", "subquery-20", 3, SubQueryStatus.IN_PROGRESS));
        subQueryRepository.save(new SubQuery("302", "query-15", "subquery-30", 3, SubQueryStatus.IN_PROGRESS));

        subQueryService.completeSubQuery("query-15", "subquery-20");
        subQueryService.completeSubQuery("query-15", "subquery-30");

        boolean allDone = subQueryService.areAllSubQueriesDone("query-15");

        AssertionsForClassTypes.assertThat(allDone).isTrue();
    }
}
