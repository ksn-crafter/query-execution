package com.dqs.eventdrivensearch.queryExecution.consumer;

import com.dqs.eventdrivensearch.queryExecution.event.SubQueryGenerated;
import com.dqs.eventdrivensearch.queryExecution.model.QueryDescription;
import com.dqs.eventdrivensearch.queryExecution.model.SubQuery;
import com.dqs.eventdrivensearch.queryExecution.search.index.MultipleIndexSearcher;
import com.dqs.eventdrivensearch.queryExecution.services.QueryDescriptionService;
import com.dqs.eventdrivensearch.queryExecution.services.QueryStatusService;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

@Component
public class SubQueryGeneratedConsumer {
    private final QueryDescriptionService queryDescriptionService;
    private final MultipleIndexSearcher multipleIndexSearcher;
    private final QueryStatusService queryStatusService;

    public SubQueryGeneratedConsumer(QueryDescriptionService queryDescriptionService,
                                     MultipleIndexSearcher multipleIndexSearcher,
                                     QueryStatusService queryStatusService) {
        this.queryDescriptionService = queryDescriptionService;
        this.multipleIndexSearcher = multipleIndexSearcher;
        this.queryStatusService = queryStatusService;
    }

    /**
     * Spring will create a method called poll (or equivalent) around consume -- pseudocode
     * void poll() {
     *      while(true) {
     *          List<SubQueryGenerated> records = poll(); -> polling kafka
     *          for (SubQueryGenerated record: records) {
     *              consume(record);
     *          }
     *      }
     * }
     * In one poll, kafka consumer (or spring's kafka consumer) will poll max.poll.records, records.
     * In our case, we are polling 125 records, each record is taking 7.88 seconds + 2 seconds (buffer + mongo) = ~10 seconds
     * Total time to process all the polled records = 125 * 10 seconds = 1250 seconds
     * Total time to process all the polled records = 20.83 minutes.
     * Hence, poll timeout .. or Kafka coordinator will consider that the consumer is dead because the consumer has not
     * invoked poll method in the last max.poll.interval.ms configured time.
     */
    public void consume(SubQueryGenerated subQueryGenerated) {
        System.out.printf("Sub Query with id %s, having query id %s, for tenant %s is being consumed%n", subQueryGenerated.subQueryId(), subQueryGenerated.queryId(), subQueryGenerated.tenant());
        QueryDescription queryDescription = queryDescriptionService.findQueryDescriptionByQueryId(subQueryGenerated.queryId());
        boolean savedSubQuery = queryDescriptionService.updateQueryDescriptionAndSaveSubQuery(queryDescription, new SubQuery(subQueryGenerated.queryId(), subQueryGenerated.subQueryId(), subQueryGenerated.indexPaths(), subQueryGenerated.totalSubQueries(), subQueryGenerated.tenant()));
        if (savedSubQuery) {
            search(subQueryGenerated, queryDescription);
        }
        Instant now = Instant.now();
        queryStatusService.mayBeCompleteTheQuery(subQueryGenerated.queryId(), subQueryGenerated.subQueryId());
        System.out.printf("Time to (maybe) update query %s is %d ms \n", subQueryGenerated.queryId(), Duration.between(now, Instant.now()).toMillis());
    }

    private void search(SubQueryGenerated subQueryGenerated, QueryDescription queryDescription) {
        System.out.printf("Sub Query with id %s, having query id %s, for tenant %s has been saved to mongo%n", subQueryGenerated.subQueryId(), subQueryGenerated.queryId(), subQueryGenerated.tenant());
        try {
            multipleIndexSearcher.searchV3(queryDescription.term(), subQueryGenerated.queryId(), subQueryGenerated.indexPaths(), subQueryGenerated.subQueryId());
            System.out.printf("Search for Sub Query with id %s having query id %s for tenant %s is completed%n", subQueryGenerated.subQueryId(), subQueryGenerated.queryId(), subQueryGenerated.tenant());
        } catch (Exception e) {
            System.out.printf("Search for Sub Query with id %s having query id %s for tenant %s has an error.%n", subQueryGenerated.subQueryId(), subQueryGenerated.queryId(), subQueryGenerated.tenant());
            System.out.println(e.getMessage() + "\n" + Arrays.toString(e.getStackTrace()));
            return;
        }
        //subQueryExecutedProducer.produce(new SubQueryExecuted(subQueryGenerated.subQueryId(), subQueryGenerated.queryId(), subQueryGenerated.totalSubQueries(), LocalDateTime.now(), subQueryGenerated.tenant()), queryDescription.tenant());
        System.out.printf("Event for Sub Query with id %s having query id %s for tenant %s is produced%n", subQueryGenerated.subQueryId(), subQueryGenerated.queryId(), subQueryGenerated.tenant());
    }
}
