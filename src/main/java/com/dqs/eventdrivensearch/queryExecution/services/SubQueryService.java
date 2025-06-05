package com.dqs.eventdrivensearch.queryExecution.services;

import com.dqs.eventdrivensearch.queryExecution.model.SubQuery;
import com.dqs.eventdrivensearch.queryExecution.repository.SubQueryRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

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
}
