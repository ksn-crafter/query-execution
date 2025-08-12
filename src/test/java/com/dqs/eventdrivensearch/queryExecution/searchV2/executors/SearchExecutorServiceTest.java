package com.dqs.eventdrivensearch.queryExecution.searchV2.executors;


import com.dqs.eventdrivensearch.queryExecution.searchV2.IndexSearcher;
import com.dqs.eventdrivensearch.queryExecution.model.SearchResult;
import com.dqs.eventdrivensearch.queryExecution.model.SearchTask;
import com.dqs.eventdrivensearch.queryExecution.search.utils.Utilities;
import com.dqs.eventdrivensearch.queryExecution.searchV2.ZippedIndex;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.dqs.eventdrivensearch.queryExecution.search.utils.Utilities.readAndUnzipInDirectory;

@ExtendWith(SpringExtension.class)
@Import({SearchExecutorService.class, IndexSearcher.class, ResultWriterExecutorService.class})
@ActiveProfiles("test")
public class SearchExecutorServiceTest {

    @Autowired
    private SearchExecutorService searchExecutorService;


    static Path indexFilePath;

    @BeforeAll
    static void setup() throws IOException {
        ZippedIndex zippedIndex = new ZippedIndex();
        try (InputStream inputStream = SearchExecutorServiceTest.class.getClassLoader().getResourceAsStream("sample.zip")) {
            indexFilePath = zippedIndex.unzip(inputStream);
        }
    }


    @Test
    void testSearchTaskExecution() throws ExecutionException, InterruptedException, ParseException {
        Query query = Utilities.getQuery("Historical", new StandardAnalyzer());
        SearchTask task = new SearchTask(query, "query-1", "subquery-1", "path/to/s3",indexFilePath);
        CompletableFuture<SearchResult> result = searchExecutorService.submit(task);
        Assertions.assertEquals(1048, result.get().totalHits());
    }
}
