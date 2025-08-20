package com.dqs.eventdrivensearch.queryExecution.search.index;

import com.dqs.eventdrivensearch.queryExecution.search.index.cache.DirectoryCache;
import com.dqs.eventdrivensearch.queryExecution.search.io.S3AsyncIndexDownloader;
import com.dqs.eventdrivensearch.queryExecution.search.metrics.MetricsPublisher;
import com.dqs.eventdrivensearch.queryExecution.search.model.SearchResult;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SingleIndexAsyncSearcher {

    @Autowired
    DirectoryCache directoryCache;

    private S3AsyncIndexDownloader s3AsyncIndexDownloader;
    private MetricsPublisher metricsPublisher;

    public SingleIndexAsyncSearcher(MetricsPublisher metricsPublisher, S3AsyncIndexDownloader s3AsyncIndexDownloader) {
        this.metricsPublisher = metricsPublisher;
        this.s3AsyncIndexDownloader = s3AsyncIndexDownloader;
    }

    private static final String[] DOCUMENT_FIELDS = {"body", "subject", "date", "from", "to", "cc", "bcc"};

    CompletableFuture<SearchResult> search(String zipFilePath, Query query, String queryId) throws IOException {
        Directory directory = directoryCache.get(zipFilePath);

        if(directory == null){
            System.out.println("Its a cache miss for indexPath : " + zipFilePath);
            Path targetTempDirectory = Files.createTempDirectory("tempDirPrefix-");
            return downloadZipAndUnzipInDirectory(zipFilePath, targetTempDirectory, queryId, query);
        }

        Instant start = Instant.now();
        // Verify the index by searching
        DirectoryReader reader = DirectoryReader.open(directory);
        org.apache.lucene.search.IndexSearcher searcher = new org.apache.lucene.search.IndexSearcher(reader);

        SearchResult searchResult = readResults(searcher, query);
        metricsPublisher.putMetricData(MetricsPublisher.MetricNames.SEARCH_SINGLE_INDEX_SHARD, Duration.between(start, Instant.now()).toMillis(), queryId);

        return CompletableFuture.completedFuture(searchResult);
    }

    private void deleteTempDirectory(File directory) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (!file.isDirectory()) {
                    file.delete();
                }
            }
        }
    }

    private void deleteDownloadedFile(Path targetTempDirectory) {
        File file = new File(targetTempDirectory.toAbsolutePath() + ".zip");
        if (file.exists()) {
            file.delete();
        }
    }

    private CompletableFuture<SearchResult> downloadZipAndUnzipInDirectory(String zipFilePath, Path outputDir, String queryId, Query query) throws IOException {
        if (!Files.exists(outputDir)) {
            Files.createDirectories(outputDir);
        }

        return s3AsyncIndexDownloader.downloadAndUnzipIndex(zipFilePath, queryId, outputDir).
                handle((unused, throwable) -> {
                    if (throwable != null) {
                        throw new RuntimeException(throwable.getMessage());
                    }

                    Directory directory;
                    SearchResult searchResult = null;
                    try {
                        directory = new MMapDirectory(outputDir);
                        Instant start = Instant.now();
                        DirectoryReader reader = DirectoryReader.open(directory);
                        org.apache.lucene.search.IndexSearcher searcher = new org.apache.lucene.search.IndexSearcher(reader);

                        searchResult = readResults(searcher, query);
                        metricsPublisher.putMetricData(MetricsPublisher.MetricNames.SEARCH_SINGLE_INDEX_SHARD, Duration.between(start, Instant.now()).toMillis(), queryId);

                        deleteTempDirectory(outputDir.toFile());
                        deleteDownloadedFile(outputDir);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    return searchResult;
                });
    }



    private SearchResult readResults(org.apache.lucene.search.IndexSearcher searcher, Query query) throws IOException {

        final int optimalPageSize = 30000; // Number of results per page
        ScoreDoc lastDoc = null; // Starting point for pagination (null for first page)
        List<String> documentIds = new ArrayList<>();
        Long totalHits = null;

        while (true) {
            // Perform the search, either starting fresh or after the last document of the previous page
            TopDocs results = (lastDoc == null) ? searcher.search(query, optimalPageSize)
                    : searcher.searchAfter(lastDoc, query, optimalPageSize);

            /*
                TO BE DISCUSSED - Mismatch in return types:
                "results.totalHits.value" returns long & "searcher.getIndexReader().numDocs()" returns int
             */

            totalHits = results.totalHits.value;

            // Process the current page of results
            for (ScoreDoc scoreDoc : results.scoreDocs) {
                Document hitDoc = searcher.getIndexReader().storedFields().document(scoreDoc.doc, Set.of("id"));
                documentIds.add(hitDoc.getField("id").stringValue() + "," + scoreDoc.score);
            }

            // Update lastDoc to the last document of the current page
            lastDoc = results.scoreDocs.length > 0 ? results.scoreDocs[results.scoreDocs.length - 1] : null;

            // If fewer results than optimalPageSize were returned, we're done
            if (results.scoreDocs.length < optimalPageSize) {
                break;
            }
        }

        return new SearchResult(documentIds, searcher.getIndexReader().numDocs(), totalHits, documentIds.size());
    }

    Query getQuery(String queryString, StandardAnalyzer analyzer) throws ParseException {
        MultiFieldQueryParser parser = new MultiFieldQueryParser(DOCUMENT_FIELDS, analyzer);
        return parser.parse(queryString);
    }
}

