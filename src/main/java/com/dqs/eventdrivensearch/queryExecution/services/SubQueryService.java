package com.dqs.eventdrivensearch.queryExecution.services;

import com.dqs.eventdrivensearch.queryExecution.model.SubQuery;
import com.dqs.eventdrivensearch.queryExecution.repository.SubQueryRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Service
public class SubQueryService {
    private SubQueryRepository subQueryRepository;

    public SubQueryService(SubQueryRepository subQueryRepository){
        this.subQueryRepository = subQueryRepository;
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public void save(SubQuery subQuery){
        boolean isSubQueryExists = subQueryRepository.existsBySubQueryId(subQuery.subQueryId());
        if(isSubQueryExists == false) {
            subQueryRepository.save(subQuery);
        }
    }
}
