package com.dqs.eventdrivensearch.queryExecution.repository;

import com.dqs.eventdrivensearch.queryExecution.model.QueryDescription;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface QueryDescriptionRepository extends MongoRepository<QueryDescription,String> {
    Optional<QueryDescription> findByQueryId(String queryId);

    void deleteByQueryId(String queryId);
}
