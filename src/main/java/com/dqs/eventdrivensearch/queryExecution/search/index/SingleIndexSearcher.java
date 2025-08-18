package com.dqs.eventdrivensearch.queryExecution.search.index;

import com.dqs.eventdrivensearch.queryExecution.search.index.cache.DirectoryCache;
import com.dqs.eventdrivensearch.queryExecution.search.io.S3IndexDownloader;
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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
class SingleIndexSearcher {

    @Autowired
    private DirectoryCache directoryCache;

    private final S3IndexDownloader s3IndexDownloader;

    private final MetricsPublisher metricsPublisher;

    public SingleIndexSearcher(MetricsPublisher metricsPublisher, S3IndexDownloader s3IndexDownloader) {
        this.metricsPublisher = metricsPublisher;
        this.s3IndexDownloader = s3IndexDownloader;
    }

    private static final String[] DOCUMENT_FIELDS = {"body", "subject", "date", "from", "to", "cc", "bcc"};

    SearchResult search(String zipFilePath, Query query, String queryId) throws IOException {

        Directory directory = directoryCache.get(zipFilePath);

        if(directory == null) {
            System.out.println("It's a cache miss for path : " + zipFilePath);
            directory = downloadZipAndUnzipInDirectory(zipFilePath, queryId);
        }

        Instant start = Instant.now();
        // Verify the index by searching
        DirectoryReader reader = DirectoryReader.open(directory);
        org.apache.lucene.search.IndexSearcher searcher = new org.apache.lucene.search.IndexSearcher(reader);

        SearchResult searchResult = readResults(searcher, query);
        metricsPublisher.putMetricData(MetricsPublisher.MetricNames.SEARCH_SINGLE_INDEX_SHARD, Duration.between(start, Instant.now()).toMillis(), queryId);

        return searchResult;
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

    private Directory downloadZipAndUnzipInDirectory(String zipFilePath, String queryId) throws IOException {
        Directory memDir = new ByteBuffersDirectory();

        InputStream inputStream = s3IndexDownloader.getInputStream(zipFilePath, queryId);
        Instant start = Instant.now();

        final int OPTIMAL_STREAM_BUFFER_SIZE = 1048576;
        try (ZipInputStream zipIn = new ZipInputStream(new BufferedInputStream(inputStream, OPTIMAL_STREAM_BUFFER_SIZE))) {
            byte[] zipStreamBuffer = new byte[OPTIMAL_STREAM_BUFFER_SIZE];
            ZipEntry entry;
            while ((entry = zipIn.getNextEntry()) != null) {
                if (!entry.isDirectory()) {
                    try (IndexOutput output = memDir.createOutput(entry.getName(), IOContext.DEFAULT)) {
                        int len;
                        while ((len = zipIn.read(zipStreamBuffer)) > 0) {
                            output.writeBytes(zipStreamBuffer, 0, len);
                        }
                    }
                }
                zipIn.closeEntry();
            }
        } finally {
            inputStream.close();
        }
        metricsPublisher.putMetricData(MetricsPublisher.MetricNames.UNZIP_SINGLE_INDEX_SHARD, Duration.between(start, Instant.now()).toMillis(), queryId);

        return memDir;
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
