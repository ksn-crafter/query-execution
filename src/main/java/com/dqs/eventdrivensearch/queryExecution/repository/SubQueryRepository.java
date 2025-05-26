package com.dqs.eventdrivensearch.queryExecution.repository;

import com.dqs.eventdrivensearch.queryExecution.model.SubQuery;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;
import java.util.Optional;

@Repository
public interface SubQueryRepository extends MongoRepository<SubQuery,String> {
    Optional<SubQuery> findBySubQueryId(String subQueryId);
}
