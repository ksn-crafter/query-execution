package com.dqs.eventdrivensearch.queryExecution.consumer;

import com.dqs.eventdrivensearch.queryExecution.event.SubQueryGenerated;
import com.dqs.eventdrivensearch.queryExecution.model.SubQuery;
import com.dqs.eventdrivensearch.queryExecution.repository.SubQueryRepository;
import com.dqs.eventdrivensearch.queryExecution.services.QueryDescriptionService;
import com.dqs.eventdrivensearch.queryExecution.services.SubQueryService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class SubQueryGeneratedConsumer {
    private final String topic;
    private final String consumerGroupId;
    private final QueryDescriptionService queryDescriptionService;

    public SubQueryGeneratedConsumer(@Value("${sub-query-generated-topic}")String topic,
                                     @Value("${sub-query-generated-event-consumer-group}")String consumerGroupId,
                                     QueryDescriptionService queryDescriptionService){
        this.topic = topic;
        this.consumerGroupId = consumerGroupId;
        this.queryDescriptionService = queryDescriptionService;
    }

    @KafkaListener(topics="#{__listener.topic}",groupId="#{__listener.consumerGroupId}")
    public void consume(SubQueryGenerated subQueryGenerated){
      queryDescriptionService.update(new SubQuery(subQueryGenerated.queryId(),subQueryGenerated.subQueryId(), subQueryGenerated.indexPaths()));
    }
}
