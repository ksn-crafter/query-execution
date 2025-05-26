package com.dqs.eventdrivensearch.queryExecution.consumer;

import com.dqs.eventdrivensearch.queryExecution.event.SubQueryGenerated;
import com.dqs.eventdrivensearch.queryExecution.model.QueryDescription;
import com.dqs.eventdrivensearch.queryExecution.model.SubQuery;
import com.dqs.eventdrivensearch.queryExecution.search.index.IndexSequenceProcessor;
import com.dqs.eventdrivensearch.queryExecution.services.QueryDescriptionService;
import org.apache.lucene.queryparser.classic.ParseException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class SubQueryGeneratedConsumer {
    private final String topic;
    private final String consumerGroupId;
    private final QueryDescriptionService queryDescriptionService;
    private final IndexSequenceProcessor indexSequenceProcessor;

    public SubQueryGeneratedConsumer(@Value("${sub-query-generated-topic}") String topic,
                                     @Value("${sub-query-generated-event-consumer-group}") String consumerGroupId,
                                     QueryDescriptionService queryDescriptionService) {
        this.topic = topic;
        this.consumerGroupId = consumerGroupId;
        this.queryDescriptionService = queryDescriptionService;
        indexSequenceProcessor = new IndexSequenceProcessor();
    }

    @KafkaListener(topics = "#{__listener.topic}", groupId = "#{__listener.consumerGroupId}")
    public void consume(SubQueryGenerated subQueryGenerated) {
        queryDescriptionService.updateQueryDescriptionAndSaveSubQuery(new SubQuery(subQueryGenerated.queryId(), subQueryGenerated.subQueryId(), subQueryGenerated.indexPaths()));
        QueryDescription queryDescription = queryDescriptionService.findQueryDescriptionByQueryId(subQueryGenerated.queryId());
        try {
            indexSequenceProcessor.processIndexSequence(queryDescription.term(),subQueryGenerated.queryId() ,subQueryGenerated.indexPaths());
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
