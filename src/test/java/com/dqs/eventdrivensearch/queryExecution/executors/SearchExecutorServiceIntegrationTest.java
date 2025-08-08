package com.dqs.eventdrivensearch.queryExecution.executors;


import com.dqs.eventdrivensearch.queryExecution.search.executors.SearchExecutorService;
import com.dqs.eventdrivensearch.queryExecution.search.index.IndexSearcher;
import com.dqs.eventdrivensearch.queryExecution.search.model.SearchTask;
import com.dqs.eventdrivensearch.queryExecution.search.model.SearchTaskWithIndexPath;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.concurrent.TimeUnit;

@ExtendWith(SpringExtension.class)
@Import({SearchExecutorService.class})
@ActiveProfiles("test")
public class SearchExecutorServiceIntegrationTest {

    @Autowired
    private SearchExecutorService searchExecutorService;

    @MockBean
    private IndexSearcher indexSearcher;

    @Test
    void testSearchTaskExecution() {
        SearchTaskWithIndexPath task = new SearchTaskWithIndexPath(null, new SearchTask(null, "query-1", "subquery-1", "path/to/s3"));

        searchExecutorService.submit(task);

        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() ->
                        Mockito.verify(indexSearcher, Mockito.times(1)).search(task));
    }
}
