package com.dqs.eventdrivensearch.queryExecution.consumer;

import com.dqs.eventdrivensearch.queryExecution.event.SubQueryExecuted;
import com.dqs.eventdrivensearch.queryExecution.event.SubQueryGenerated;
import com.dqs.eventdrivensearch.queryExecution.model.QueryDescription;
import com.dqs.eventdrivensearch.queryExecution.model.SubQuery;
import com.dqs.eventdrivensearch.queryExecution.producer.SubQueryExecutedProducer;
import com.dqs.eventdrivensearch.queryExecution.search.index.MultipleIndexSearcher;
import com.dqs.eventdrivensearch.queryExecution.services.QueryDescriptionService;
import org.apache.lucene.queryparser.classic.ParseException;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
public class SubQueryGeneratedConsumer {
    private final QueryDescriptionService queryDescriptionService;
    private final MultipleIndexSearcher multipleIndexSearcher;
    private final SubQueryExecutedProducer subQueryExecutedProducer;

    public SubQueryGeneratedConsumer(QueryDescriptionService queryDescriptionService, SubQueryExecutedProducer producer, MultipleIndexSearcher multipleIndexSearcher) {
        this.queryDescriptionService = queryDescriptionService;
        this.multipleIndexSearcher = multipleIndexSearcher;
        this.subQueryExecutedProducer = producer;
    }

    public void consume(SubQueryGenerated subQueryGenerated) {
        System.out.println(String.format("Sub Query with id %s, having query id %s, for tenant %s is being consumed",subQueryGenerated.subQueryId(),subQueryGenerated.queryId(),subQueryGenerated.tenant()));
        QueryDescription queryDescription = queryDescriptionService.findQueryDescriptionByQueryId(subQueryGenerated.queryId());
        queryDescriptionService.updateQueryDescriptionAndSaveSubQuery(queryDescription,new SubQuery(subQueryGenerated.queryId(), subQueryGenerated.subQueryId(), subQueryGenerated.indexPaths(),subQueryGenerated.totalSubQueries(),subQueryGenerated.tenant()));
        System.out.println(String.format("Sub Query with id %s, having query id %s, for tenant %s has been saved to mongo",subQueryGenerated.subQueryId(),subQueryGenerated.queryId(),subQueryGenerated.tenant()));
        try {
            multipleIndexSearcher.search(queryDescription.term(),subQueryGenerated.queryId() ,subQueryGenerated.indexPaths(),subQueryGenerated.subQueryId());
            System.out.println(String.format("Search for Sub Query with id %s having query id %s for tenant %s is completed",subQueryGenerated.subQueryId(),subQueryGenerated.queryId(),subQueryGenerated.tenant()));
        } catch (Exception e) {
            System.out.println(String.format("Search for Sub Query with id %s having query id %s for tenant %s has an error.",subQueryGenerated.subQueryId(),subQueryGenerated.queryId(),subQueryGenerated.tenant()));
            System.out.println(e.getMessage() + "\n" + e.getStackTrace());
            return;
        }
        subQueryExecutedProducer.produce(new SubQueryExecuted(subQueryGenerated.subQueryId(), subQueryGenerated.queryId(),subQueryGenerated.totalSubQueries(), LocalDateTime.now(), subQueryGenerated.tenant()),queryDescription.tenant());
        System.out.println(String.format("Event for Sub Query with id %s having query id %s for tenant %s is produced",subQueryGenerated.subQueryId(),subQueryGenerated.queryId(),subQueryGenerated.tenant()));

    }
}
