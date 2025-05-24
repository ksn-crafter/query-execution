package com.dqs.eventdrivensearch.queryExecution.repository;

import com.dqs.eventdrivensearch.queryExecution.model.SubQuery;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface SubQueryRepository extends MongoRepository<SubQuery,String> {
}
