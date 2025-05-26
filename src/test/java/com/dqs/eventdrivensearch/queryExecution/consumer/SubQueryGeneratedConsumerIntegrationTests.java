package com.dqs.eventdrivensearch.queryExecution.consumer;

import com.dqs.eventdrivensearch.queryExecution.event.SubQueryGenerated;
import com.dqs.eventdrivensearch.queryExecution.model.QueryDescription;
import com.dqs.eventdrivensearch.queryExecution.model.QueryStatus;
import com.dqs.eventdrivensearch.queryExecution.model.SubQuery;
import com.dqs.eventdrivensearch.queryExecution.repository.QueryDescriptionRepository;
import com.dqs.eventdrivensearch.queryExecution.repository.SubQueryRepository;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfig;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Optional;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;


@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = {"incoming_sub_queries_jpmc"})
class SubQueryGeneratedConsumerIntegrationTests {

    private KafkaTemplate<String, SubQueryGenerated> kafkaTemplate;

    @Autowired
    private SubQueryRepository subQueryRepository;

    @Autowired
    private QueryDescriptionRepository queryDescriptionRepository;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

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

    @BeforeEach
    void setup() {
        Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        senderProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        senderProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        ProducerFactory<String, SubQueryGenerated> producerFactory = new DefaultKafkaProducerFactory<>(senderProps);
        kafkaTemplate = new KafkaTemplate<>(producerFactory);
    }

    @Test
    void updatesQueryDescriptionAndSavesSubQuery() {
        queryDescriptionRepository.save(new QueryDescription("query-510", "Deutsche", "Historical", 2001, 2007, QueryStatus.Acknowledged, LocalDateTime.now()));
        String[] indexPaths = {"path-1","path-2"};
        kafkaTemplate.send("incoming_sub_queries_jpmc", new SubQueryGenerated("query-510", "subquery-1",indexPaths,2));

        await().atMost(ofSeconds(10)).untilAsserted(() -> {
            Optional<SubQuery> subQuery = subQueryRepository.findBySubQueryId("subquery-1");
            assertThat(subQuery.get().subQueryId()).isEqualTo("subquery-1");

            QueryDescription queryDescription = queryDescriptionRepository.findByQueryId("query-510").get();
            assertThat(queryDescription.status()).isEqualTo(QueryStatus.InProgress);
        });
    }

}
