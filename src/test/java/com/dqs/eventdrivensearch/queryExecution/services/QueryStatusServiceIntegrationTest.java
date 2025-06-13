package com.dqs.eventdrivensearch.queryExecution.services;

import com.dqs.eventdrivensearch.queryExecution.model.QueryDescription;
import com.dqs.eventdrivensearch.queryExecution.model.QueryStatus;
import com.dqs.eventdrivensearch.queryExecution.model.SubQuery;
import com.dqs.eventdrivensearch.queryExecution.model.SubQueryStatus;
import com.dqs.eventdrivensearch.queryExecution.repository.QueryDescriptionRepository;
import com.dqs.eventdrivensearch.queryExecution.repository.SubQueryRepository;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfig;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
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
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EmbeddedKafka(partitions = 1, topics = {"incoming_sub_queries_jpmc"})
public class QueryStatusServiceIntegrationTest {

    @Autowired
    private QueryStatusService queryStatusService;

    @Autowired
    private SubQueryRepository subQueryRepository;

    @Autowired
    private QueryDescriptionRepository queryRepository;

    @TestConfiguration
    static class S3TestConfig {
        @Bean
        @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
        @Primary
        public S3Client s3Client() {
            return S3Client.builder()
                    .region(Region.US_EAST_1)
                    .build();
        }

        @Bean
        @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
        @Primary
        public CloudWatchClient cloudWatchClient() {
            return CloudWatchClient.builder()
                    .region(Region.US_EAST_1)
                    .build();
        }
    }

    private static final MongodExecutable mongodExecutable;

    private static final int mongoPort;

    static {
        try {
            mongoPort = Network.getFreeServerPort();
            MongodConfig config = MongodConfig.builder()
                    .version(Version.Main.V6_0)
                    .net(new Net(mongoPort, Network.localhostIsIPv6()))
                    .build();
            mongodExecutable = MongodStarter.getDefaultInstance().prepare(config);
            mongodExecutable.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @DynamicPropertySource
    static void setMongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", () -> "mongodb://localhost:" + mongoPort + "/dqs");
    }

    @AfterAll
    public void stopEmbeddedMongo() {
        mongodExecutable.stop();
    }

    @Test
    void completeAQueryAndAllItsSubqueries() {
        subQueryRepository.deleteAllById(List.of("1", "2"));
        queryRepository.deleteByQueryId("query-40");

        queryRepository.save(new QueryDescription("query-40", "JPMC", "Historical", 2001, 2007, QueryStatus.InProgress, LocalDateTime.now()));
        subQueryRepository.save(new SubQuery("1", "query-40", "subquery-1", 2, SubQueryStatus.COMPLETED, LocalDateTime.now()));
        subQueryRepository.save(new SubQuery("2", "query-40", "subquery-2", 2, SubQueryStatus.IN_PROGRESS));

        queryStatusService.mayBeCompleteTheQuery("query-40", "subquery-2");

        QueryDescription queryDescription = queryRepository.findByQueryId("query-40").get();

        assertThat(queryDescription.status()).isEqualTo(QueryStatus.Completed);
    }

    @Test
    void doNotCompleteTheQueryGivenItsSubqueriesAreNotCompleted() {
        subQueryRepository.deleteAllById(List.of("100", "200", "300"));
        queryRepository.deleteByQueryId("query-100");

        queryRepository.save(new QueryDescription("query-100", "JPMC", "Historical", 2001, 2007, QueryStatus.InProgress, LocalDateTime.now()));
        subQueryRepository.save(new SubQuery("100", "query-100", "subquery-1", 3, SubQueryStatus.COMPLETED, LocalDateTime.now()));
        subQueryRepository.save(new SubQuery("200", "query-100", "subquery-2", 3, SubQueryStatus.IN_PROGRESS));
        subQueryRepository.save(new SubQuery("300", "query-100", "subquery-3", 3, SubQueryStatus.IN_PROGRESS));

        queryStatusService.mayBeCompleteTheQuery("query-100", "subquery-3");

        QueryDescription queryDescription = queryRepository.findByQueryId("query-100").get();

        assertThat(queryDescription.status()).isEqualTo(QueryStatus.InProgress);
    }

    @Test
    void completeTheQueryGivenItsSubqueriesAreCompleted() {
        subQueryRepository.deleteAllById(List.of("1001", "2001", "3001"));
        queryRepository.deleteByQueryId("query-1001");

        queryRepository.save(new QueryDescription("query-1001", "JPMC", "Historical", 2001, 2007, QueryStatus.InProgress, LocalDateTime.now()));
        subQueryRepository.save(new SubQuery("1001", "query-1001", "subquery-1", 3, SubQueryStatus.COMPLETED, LocalDateTime.now()));
        subQueryRepository.save(new SubQuery("2001", "query-1001", "subquery-2", 3, SubQueryStatus.IN_PROGRESS));
        subQueryRepository.save(new SubQuery("3001", "query-1001", "subquery-3", 3, SubQueryStatus.IN_PROGRESS));

        queryStatusService.mayBeCompleteTheQuery("query-1001", "subquery-2");
        queryStatusService.mayBeCompleteTheQuery("query-1001", "subquery-3");

        QueryDescription queryDescription = queryRepository.findByQueryId("query-1001").get();

        assertThat(queryDescription.status()).isEqualTo(QueryStatus.Completed);
    }
}
