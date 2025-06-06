package com.dqs.eventdrivensearch.queryExecution.services;

import java.util.Optional;

import com.dqs.eventdrivensearch.queryExecution.model.QueryDescription;
import com.dqs.eventdrivensearch.queryExecution.model.SubQuery;
import com.dqs.eventdrivensearch.queryExecution.repository.QueryDescriptionRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.annotation.Propagation;

@Service
public class QueryDescriptionService {
    private final SubQueryService subQueryService;
    private final QueryDescriptionRepository queryDescriptionRepository;

    public QueryDescriptionService(SubQueryService subQueryService, QueryDescriptionRepository queryDescriptionRepository) {
        this.subQueryService = subQueryService;
        this.queryDescriptionRepository = queryDescriptionRepository;
    }

    @Transactional
    public boolean updateQueryDescriptionAndSaveSubQuery(QueryDescription queryDescription, SubQuery subQuery) {
        updateQueryDescriptionStatusToInProgress(queryDescription);
        return subQueryService.save(subQuery);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public void updateQueryDescriptionStatusToInProgress(QueryDescription queryDescription) {
        queryDescription.setStatusToInProgress();
        queryDescriptionRepository.save(queryDescription);
    }

    public QueryDescription findQueryDescriptionByQueryId(String queryId) {
        Optional<QueryDescription> queryDescription = queryDescriptionRepository.findByQueryId(queryId);

        if (queryDescription.isPresent()) return queryDescription.get();

        throw new RuntimeException(String.format("Query Description with id %s not found", queryId));
    }
}
