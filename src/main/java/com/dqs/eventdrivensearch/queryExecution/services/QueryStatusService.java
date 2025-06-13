package com.dqs.eventdrivensearch.queryExecution.services;

import com.dqs.eventdrivensearch.queryExecution.model.QueryDescription;
import com.dqs.eventdrivensearch.queryExecution.repository.QueryDescriptionRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Service
public class QueryStatusService {

    private final SubQueryService subQueryService;
    private final QueryDescriptionRepository queryRepository;

    public QueryStatusService(SubQueryService subQueryService, QueryDescriptionRepository queryRepository) {
        this.subQueryService = subQueryService;
        this.queryRepository = queryRepository;
    }

    @Transactional
    public void mayBeCompleteTheQuery(String queryId, String subQueryId) {
        this.subQueryService.completeSubQuery(queryId, subQueryId);
        if (this.subQueryService.areAllSubQueriesDone(queryId)) {
            Optional<QueryDescription> optionalQueryDescription = queryRepository.findByQueryId(queryId);
            if (optionalQueryDescription.isPresent()) {
                System.out.println("Completing the query with queryId " + queryId);

                QueryDescription queryDescription = optionalQueryDescription.get();
                queryDescription.complete();
                queryRepository.save(queryDescription);
            } else {
                throw new RuntimeException(String.format("Query not found for queryId %s", queryId));
            }
        }
    }
}
