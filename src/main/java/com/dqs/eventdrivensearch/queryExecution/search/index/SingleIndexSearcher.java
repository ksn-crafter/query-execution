package com.dqs.eventdrivensearch.queryExecution.search.index;

import com.dqs.eventdrivensearch.queryExecution.search.io.S3IndexDownloader;
import com.dqs.eventdrivensearch.queryExecution.search.io.S3SearchResultWriter;
import com.dqs.eventdrivensearch.queryExecution.search.metrics.MetricsPublisher;
import com.dqs.eventdrivensearch.queryExecution.search.model.SearchResult;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
class SingleIndexSearcher {
    private S3IndexDownloader s3IndexDownloader;
    private S3SearchResultWriter resultWriter;
    private MetricsPublisher metricsPublisher;

    public SingleIndexSearcher(MetricsPublisher metricsPublisher, S3IndexDownloader s3IndexDownloader, S3SearchResultWriter writer) {
        this.metricsPublisher = metricsPublisher;
        this.s3IndexDownloader = s3IndexDownloader;
        this.resultWriter = writer;
    }

    private static final String[] splitKeys = {"dqs-indexes/jpmc/splits/split_0", "dqs-indexes/jpmc/splits/split_1", "dqs-indexes/jpmc/splits/split_2", "dqs-indexes/jpmc/splits/split_3", "dqs-indexes/jpmc/splits/split_4", "dqs-indexes/jpmc/splits/split_5", "dqs-indexes/jpmc/splits/split_6", "dqs-indexes/jpmc/splits/split_7"};
    private static final String[] DOCUMENT_FIELDS = {"body", "subject", "date", "from", "to", "cc", "bcc"};

    SearchResult search(String zipFilePath, Query query, String queryId) throws IOException {
        Path targetTempDirectory = Files.createTempDirectory("tempDirPrefix-");
        Directory directory = null;

        downloadZipAndUnzipInDirectory(zipFilePath, targetTempDirectory, queryId);
        directory = new MMapDirectory(targetTempDirectory);

        Instant start = Instant.now();
        // Verify the index by searching
        DirectoryReader reader = DirectoryReader.open(directory);
        org.apache.lucene.search.IndexSearcher searcher = new org.apache.lucene.search.IndexSearcher(reader);

        SearchResult searchResult = readResults(searcher, query);
        metricsPublisher.putMetricData(MetricsPublisher.MetricNames.SEARCH_SINGLE_INDEX_SHARD, Duration.between(start, Instant.now()).toMillis(), queryId);

        deleteTempDirectory(targetTempDirectory.toFile());

        return searchResult;
    }

    void searchV2(String splitPath, Query query, String queryId, String queryResultLocation) throws IOException {
        Path targetTempDirectory = Files.createTempDirectory("tempDirPrefix-");
        downloadAndSearchOnSplits(splitPath, targetTempDirectory, query, queryId, queryResultLocation);
        deleteDirectory(targetTempDirectory.toFile());
    }

    private void downloadAndSearchOnSplits(String splitPath, Path tempDir, Query query, String queryId, String queryResultLocation) throws IOException {
        URL url = new URL(splitPath);
        String bucketName = url.getHost().split("\\.")[0];

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<SearchResult>> futures = new ArrayList<>(splitKeys.length);
            for (String splitKey : splitKeys) {
                futures.add(executor.submit(() -> {

                    String splitName = Paths.get(splitKey).getFileName().toString();

                    try (InputStream inputStream = s3IndexDownloader.downloadFile(splitKey, bucketName)) {

                        // Output dir
                        Path outputDirectory = tempDir.resolve(splitName + "_dir");
                        Files.createDirectories(outputDirectory);

                        // Process
                        readSplitAndWriteLuceneSegment(outputDirectory, inputStream, splitName);

                        System.out.println("Processed (not written) " + splitName + " in thread: " + Thread.currentThread());
                        return searchSingleSplit(query, queryId, outputDirectory);
                    } catch (NoSuchKeyException e) {
                        System.err.println("split not found: " + splitKey);
                        return null;
                    } catch (IOException e) {
                        throw new UncheckedIOException("Error handling file: " + splitName, e);
                    }
                }));
            }

            final SearchResult finalResult = futures.getFirst().get();

            // Wait for all tasks and merge the result in finalResult.
            for (int index = 1; index < futures.size(); index++) {
                SearchResult searchResult = futures.get(index).get();
                finalResult.mergeFrom(searchResult);
            }

            // Avoid using executor, run a virtual thread to write the final result.
            Thread.ofVirtual().start(() -> {
                resultWriter.write(finalResult, queryResultLocation, splitPath);
            }).join();

        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException("Error in VT processing", e.getCause());
        }
    }

    private SearchResult searchSingleSplit(Query query, String queryId, Path outputDirectory) throws IOException {
        // search
        Directory directory = new MMapDirectory(outputDirectory);
        Instant start = Instant.now();
        // Verify the index by searching
        DirectoryReader reader = DirectoryReader.open(directory);
        org.apache.lucene.search.IndexSearcher searcher = new org.apache.lucene.search.IndexSearcher(reader);

        SearchResult searchResult = readResults(searcher, query);
        metricsPublisher.putMetricData(MetricsPublisher.MetricNames.SEARCH_SINGLE_SPLIT, Duration.between(start, Instant.now()).toMillis(), queryId);
        return searchResult;
    }

    private static void readSplitAndWriteLuceneSegment(Path outputDirectory, InputStream inputStream, String splitName) throws IOException {
        String segmentName = splitName.substring("split".length());

        try (DataInputStream in = new DataInputStream(new BufferedInputStream(inputStream))) {
            int cfeLen = in.readInt();
            byte[] cfeBytes = new byte[cfeLen];
            in.readFully(cfeBytes);
            Files.write(outputDirectory.resolve(segmentName + ".cfe"), cfeBytes);

            int cfsLen = in.readInt();
            byte[] cfsBytes = new byte[cfsLen];
            in.readFully(cfsBytes);
            Files.write(outputDirectory.resolve(segmentName + ".cfs"), cfsBytes);

            int siLen = in.readInt();
            byte[] siBytes = new byte[siLen];
            in.readFully(siBytes);
            Files.write(outputDirectory.resolve(segmentName + ".si"), siBytes);

            long generation = in.readLong();

            int segLen = in.readInt();
            byte[] segmentsBytes = new byte[segLen];
            in.readFully(segmentsBytes);
            Files.write(outputDirectory.resolve("segments_" + generation), segmentsBytes);
        }
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

    private void deleteDirectory(File directory) {
        if (directory == null || !directory.exists()) return;

        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteTempDirectory(file);
                }
                if (!file.delete()) {
                    System.err.println("Failed to delete: " + file.getAbsolutePath());
                }
            }
        }

        // Delete the main directory itself
        if (!directory.delete()) {
            System.err.println("Failed to delete directory: " + directory.getAbsolutePath());
        }

    }

    private void downloadZipAndUnzipInDirectory(String zipFilePath, Path outputDir, String queryId) throws IOException {
        if (!Files.exists(outputDir)) {
            Files.createDirectories(outputDir);
        }

        InputStream inputStream = s3IndexDownloader.getInputStream(zipFilePath, queryId);
        Instant start = Instant.now();

        final int OPTIMAL_STREAM_BUFFER_SIZE = 1048576;
        try (ZipInputStream zipIn = new ZipInputStream(new BufferedInputStream(inputStream, OPTIMAL_STREAM_BUFFER_SIZE))) {
            byte[] zipStreamBuffer = new byte[OPTIMAL_STREAM_BUFFER_SIZE];
            ZipEntry entry;
            while ((entry = zipIn.getNextEntry()) != null) {
                Path filePath = outputDir.resolve(entry.getName());
                if (!entry.isDirectory()) {
                    // Extract file
                    try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath.toFile()), OPTIMAL_STREAM_BUFFER_SIZE)) {
                        int len;
                        while ((len = zipIn.read(zipStreamBuffer)) > 0) {
                            bos.write(zipStreamBuffer, 0, len);
                        }
                    }
                } else {
                    // Create directory
                    Files.createDirectories(filePath);
                }
                zipIn.closeEntry();
            }
        } finally {
            inputStream.close();
        }
        metricsPublisher.putMetricData(MetricsPublisher.MetricNames.UNZIP_SINGLE_INDEX_SHARD, Duration.between(start, Instant.now()).toMillis(), queryId);
    }

    private SearchResult readResults(org.apache.lucene.search.IndexSearcher searcher, Query query) throws IOException {

        final int optimalPageSize = 30000; // Number of results per page
        ScoreDoc lastDoc = null; // Starting point for pagination (null for first page)
        List<String> documentIds = new ArrayList<>();
        Long totalHits = null;

        StoredFields storedFields = searcher.getIndexReader().storedFields();
        while (true) {
            // Perform the search, either starting fresh or after the last document of the previous page
            TopDocs results = (lastDoc == null) ? searcher.search(query, optimalPageSize) : searcher.searchAfter(lastDoc, query, optimalPageSize);

            /*
                TO BE DISCUSSED - Mismatch in return types:
                "results.totalHits.value" returns long & "searcher.getIndexReader().numDocs()" returns int
             */

            totalHits = results.totalHits.value;

            // Process the current page of results
            for (ScoreDoc scoreDoc : results.scoreDocs) {
                Document hitDoc = storedFields.document(scoreDoc.doc, Set.of("id"));
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
