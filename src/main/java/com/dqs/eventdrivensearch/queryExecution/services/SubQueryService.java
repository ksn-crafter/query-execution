package com.dqs.eventdrivensearch.queryExecution.services;

import com.dqs.eventdrivensearch.queryExecution.model.SubQuery;
import com.dqs.eventdrivensearch.queryExecution.model.SubQueryStatus;
import com.dqs.eventdrivensearch.queryExecution.repository.SubQueryRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Service
public class SubQueryService {
    private final SubQueryRepository subQueryRepository;

    public SubQueryService(SubQueryRepository subQueryRepository) {
        this.subQueryRepository = subQueryRepository;
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public boolean save(SubQuery subQuery) {
        boolean isSubQueryExists = subQueryRepository.existsBySubQueryId(subQuery.subQueryId());
        if (!isSubQueryExists) {
            subQueryRepository.save(subQuery);
            return true;
        }
        return false;
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public void completeSubQuery(String queryId, String subQueryId) {
        Optional<SubQuery> optionalSubQuery = subQueryRepository.findByQueryIdAndSubQueryId(queryId, subQueryId);
        if (optionalSubQuery.isPresent()) {
            System.out.println("Completing the subQuery with subQueryId " + subQueryId);
            SubQuery subQuery = optionalSubQuery.get();
            subQuery.complete();
            subQueryRepository.save(subQuery);
        } else {
            throw new RuntimeException(String.format("SubQuery not found for queryId %s and subQueryId %s", queryId, subQueryId));
        }
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public boolean areAllSubQueriesDone(String queryId) {
        long totalSubQueries = subQueryRepository.findFirstByQueryId(queryId).map(SubQuery::totalSubQueries).get();
        long totalCompletedSubQueries = subQueryRepository.countByQueryIdAndStatus(queryId, SubQueryStatus.COMPLETED);

        System.out.println("queryId " + queryId + ", totalSubQueries " + totalSubQueries + ", totalCompletedSubQueries " + totalCompletedSubQueries);
        return totalSubQueries == totalCompletedSubQueries;
    }
}
