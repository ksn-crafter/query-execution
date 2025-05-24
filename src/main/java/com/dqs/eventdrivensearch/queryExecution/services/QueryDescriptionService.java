package com.dqs.eventdrivensearch.queryExecution.services;
import java.util.Optional;
import com.dqs.eventdrivensearch.queryExecution.model.QueryDescription;
import com.dqs.eventdrivensearch.queryExecution.model.QueryStatus;
import com.dqs.eventdrivensearch.queryExecution.model.SubQuery;
import com.dqs.eventdrivensearch.queryExecution.repository.QueryDescriptionRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.annotation.Propagation;

@Service
public class QueryDescriptionService {
    private final SubQueryService subQueryService;
    private final QueryDescriptionRepository queryDescriptionRepository;

    public QueryDescriptionService(SubQueryService subQueryService, QueryDescriptionRepository queryDescriptionRepository){
        this.subQueryService = subQueryService;
        this.queryDescriptionRepository = queryDescriptionRepository;
    }

    @Transactional
    public void update(SubQuery subQuery){
        updateQueryDescriptionStatusTo(subQuery.queryId(), QueryStatus.InProgress);
        subQueryService.save(subQuery);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public void updateQueryDescriptionStatusTo(String queryId,QueryStatus status){
      Optional<QueryDescription> queryDescription =  queryDescriptionRepository.findById(queryId);
      if(queryDescription.isPresent()){
          QueryDescription anotherQueryDescription = queryDescription.get();
          anotherQueryDescription.setStatusToInProgress();
          queryDescriptionRepository.save(anotherQueryDescription);
      }else{
          throw new RuntimeException(String.format("Query Description with id %s not found",queryId));
      }
    }
}
