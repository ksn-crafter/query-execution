package com.dqs.eventdrivensearch.queryExecution.producer;

import com.dqs.eventdrivensearch.queryExecution.event.SubQueryExecuted;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class SubQueryExecutedProducer {
    private final KafkaTemplate<String, SubQueryExecuted> kafkaTemplate;

    private static final String topicPrefix = "sub_query_executed_";

    public SubQueryExecutedProducer(KafkaTemplate<String,SubQueryExecuted> kafkaTemplate){
        this.kafkaTemplate = kafkaTemplate;
    }

    public void produce(SubQueryExecuted subQueryExecuted,String tenant){
        kafkaTemplate.send(topicNameFor(tenant),
                subQueryExecuted.subQueryId(),
                subQueryExecuted);
    }

    private String topicNameFor(String tenant) {
        return topicPrefix + tenant.toLowerCase();
    }

}
