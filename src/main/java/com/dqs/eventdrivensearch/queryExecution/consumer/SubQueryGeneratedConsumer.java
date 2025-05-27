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

@Component
public class SubQueryGeneratedConsumer {
    private final QueryDescriptionService queryDescriptionService;
    private final MultipleIndexSearcher multipleIndexSearcher;
    private final SubQueryExecutedProducer subQueryExecutedProducer;

    public SubQueryGeneratedConsumer(QueryDescriptionService queryDescriptionService, SubQueryExecutedProducer producer) {
        this.queryDescriptionService = queryDescriptionService;
        multipleIndexSearcher = new MultipleIndexSearcher();
        this.subQueryExecutedProducer = producer;
    }

    public void consume(SubQueryGenerated subQueryGenerated) {
        queryDescriptionService.updateQueryDescriptionAndSaveSubQuery(new SubQuery(subQueryGenerated.queryId(), subQueryGenerated.subQueryId(), subQueryGenerated.indexPaths()));
        QueryDescription queryDescription = queryDescriptionService.findQueryDescriptionByQueryId(subQueryGenerated.queryId());
        try {
            multipleIndexSearcher.search(queryDescription.term(),subQueryGenerated.queryId() ,subQueryGenerated.indexPaths());
        } catch (ParseException e) {
            System.out.println(e.getMessage() + "\n" + e.getStackTrace());
        }
        subQueryExecutedProducer.produce(new SubQueryExecuted(subQueryGenerated.subQueryId(), subQueryGenerated.queryId(),subQueryGenerated.totalSubQueries()),queryDescription.tenantId());
    }
}
