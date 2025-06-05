package com.dqs.eventdrivensearch.queryExecution.producer;

import com.dqs.eventdrivensearch.queryExecution.event.SubQueryExecuted;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Scope;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EmbeddedKafka(partitions = 1, topics = "sub_query_executed_jpmc")
@ActiveProfiles("test")
class SubQueryExecutedProducerIntegrationTests {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private SubQueryExecutedProducer publisher;

    private Consumer<String, SubQueryExecuted> consumer;

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

    @BeforeAll
    void setupConsumer() {
        Map<String, Object> props = new HashMap<>(KafkaTestUtils.consumerProps("testGroup", "true", embeddedKafka));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");

        DefaultKafkaConsumerFactory<String, SubQueryExecuted> consumerFactory =
                new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new JsonDeserializer<>(SubQueryExecuted.class, false));

        consumer = consumerFactory.createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "sub_query_executed_jpmc");
    }

    @AfterAll
    void tearDown() {
        consumer.close();
    }

    @Test
    void publishSubQueryExecuted() {
        UUID id = UUID.randomUUID();
        SubQueryExecuted event = new SubQueryExecuted(id.toString(), "query-1",3, LocalDateTime.now(),"jpmc");

        publisher.produce(event,"jpmc");

        ConsumerRecord<String, SubQueryExecuted> record = KafkaTestUtils.getSingleRecord(consumer, "sub_query_executed_jpmc");

        assertEquals(id.toString(), record.key());

        SubQueryExecuted subQueryExecuted = record.value();
        assertEquals(event.subQueryId(), subQueryExecuted.subQueryId());
        assertEquals(event.queryId(), subQueryExecuted.queryId());
        assertEquals(event.totalSubQueries(), subQueryExecuted.totalSubQueries());
        assertEquals(event.completionTime(), subQueryExecuted.completionTime());
        assertEquals(event.tenant(), subQueryExecuted.tenant());
    }
}
