package com.dqs.eventdrivensearch.queryExecution.consumer;


import com.dqs.eventdrivensearch.queryExecution.model.SubQuery;
import com.dqs.eventdrivensearch.queryExecution.model.SubQueryStatus;
import com.dqs.eventdrivensearch.queryExecution.repository.SubQueryRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = { "user-topic" })
public class SubQueryGeneratedConsumerIntegrationTests {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private SubQueryRepository subQueryRepository;

    @BeforeEach
    public void setup() {

    }

    @Test
    public void shouldConsumeKafkaMessageAndWriteToMongo() {
//        String jsonMessage = "{\"queryId\":\"123\",\"subQueryId\":\"456\",\"indexPahts\":[\"http://dqs-poc-index-path\"]}";
//        kafkaTemplate.send("incoming_sub_queries_default", jsonMessage);
//
//
//        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
//            List<SubQuery> subQueries = subQueryRepository.findAll();
//            assertThat(subQueries).hasSize(1);
//            assertThat(subQueries.get(0).queryId()).isEqualTo("");
//            assertThat(subQueries.get(0).status()).isEqualTo(SubQueryStatus.CREATED);
//        });
    }
}
