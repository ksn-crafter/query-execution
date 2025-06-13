package com.dqs.eventdrivensearch.queryExecution.repository;

import com.dqs.eventdrivensearch.queryExecution.model.SubQuery;
import com.dqs.eventdrivensearch.queryExecution.model.SubQueryStatus;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface SubQueryRepository extends MongoRepository<SubQuery,String> {
    Optional<SubQuery> findBySubQueryId(String subQueryId);

    Optional<List<SubQuery>> findAllBySubQueryId(String subQueryId);

    boolean existsBySubQueryId(String subQueryId);

    // Get any one subquery for a queryId
    Optional<SubQuery> findFirstByQueryId(String queryId);

    Optional<SubQuery> findByQueryIdAndSubQueryId(String queryId, String subQueryId);

    long countByQueryIdAndStatus(String queryId, SubQueryStatus status);
}
