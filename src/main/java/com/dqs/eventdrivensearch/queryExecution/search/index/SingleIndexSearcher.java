package com.dqs.eventdrivensearch.queryExecution.search.index;

import com.dqs.eventdrivensearch.queryExecution.search.io.S3IndexDownloader;
import com.dqs.eventdrivensearch.queryExecution.search.io.S3SearchResultWriter;
import com.dqs.eventdrivensearch.queryExecution.search.metrics.MetricsPublisher;
import com.dqs.eventdrivensearch.queryExecution.search.model.SearchResult;
import jakarta.annotation.PreDestroy;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.*;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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
public class SingleIndexSearcher {
    private final S3IndexDownloader s3IndexDownloader;
    private final S3SearchResultWriter resultWriter;
    private final MetricsPublisher metricsPublisher;
    private final ScratchBufferPool scratchBufferPool;
    private final ExecutorService executor;

    public SingleIndexSearcher(MetricsPublisher metricsPublisher,
                               S3IndexDownloader s3IndexDownloader,
                               S3SearchResultWriter writer,
                               ScratchBufferPool scratchBufferPool) {
        this.metricsPublisher = metricsPublisher;
        this.s3IndexDownloader = s3IndexDownloader;
        this.resultWriter = writer;
        this.scratchBufferPool = scratchBufferPool;
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    private static final String[] splitKeys = {"dqs-indexes/jpmc/splits_9_12/split_0", "dqs-indexes/jpmc/splits_9_12/split_1", "dqs-indexes/jpmc/splits_9_12/split_2", "dqs-indexes/jpmc/splits_9_12/split_3", "dqs-indexes/jpmc/splits_9_12/split_4", "dqs-indexes/jpmc/splits_9_12/split_5"};
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
        Path targetTempDirectory = null;
        try {
            targetTempDirectory = Files.createTempDirectory("tempDirPrefix-");
            downloadAndSearchOnSplits(splitPath, targetTempDirectory, query, queryId, queryResultLocation);
        } finally {
            if (targetTempDirectory != null) {
                deleteDirectory(targetTempDirectory.toFile());
            }
        }
    }

    private void downloadAndSearchOnSplits(String indexPath, Path tempDir, Query query, String queryId, String queryResultLocation) throws IOException {
        Instant start = Instant.now();
        URL url = new URL(indexPath);
        String bucketName = url.getHost().split("\\.")[0];

        //TODO: Remove executor and use VirtualThread directly
        try {
            List<Future<SearchResult>> futures = new ArrayList<>(splitKeys.length);
            for (String splitKey : splitKeys) {
                futures.add(this.executor.submit(() -> {
                    String splitName = Paths.get(splitKey).getFileName().toString();
                    Directory directory = null;
                    try (InputStream inputStream = s3IndexDownloader.downloadFile(splitKey, bucketName)) {

                        // Output dir,
                        //TODO: maybe leverage RAM directory on linux/mac
                        Path outputDirectory = tempDir.resolve(splitName + "_dir");
                        Files.createDirectories(outputDirectory);

                        Instant readSplitFromS3Start = Instant.now();
                        byte[] splitBytes = inputStream.readAllBytes();
                        metricsPublisher.putMetricData(MetricsPublisher.MetricNames.READ_SPLIT_FROM_S3, Duration.between(readSplitFromS3Start, Instant.now()).toMillis(), queryId);

                        // Process
                        Instant readSplitStart = Instant.now();
                        directory = readSplit(splitBytes, splitName);
                        metricsPublisher.putMetricData(MetricsPublisher.MetricNames.READ_SPLIT_AND_CREATE_RAM_DIRECTORY, Duration.between(readSplitStart, Instant.now()).toMillis(), queryId);

                        System.out.println("Processed (not written) " + splitName + " in thread: " + Thread.currentThread());
                        return searchSingleSplit(directory, query, queryId, outputDirectory);
                    } catch (NoSuchKeyException e) {
                        System.err.println("split not found: " + splitKey);
                        return null;
                    } catch (IOException e) {
                        throw new UncheckedIOException("Error handling file: " + splitName, e);
                    } finally {
                        if (directory != null) {
                            directory.close();
                        }
                    }
                }));
            }

            final SearchResult finalResult = futures.getFirst().get();

            // Wait for all tasks and merge the result in finalResult.
            for (int index = 1; index < futures.size(); index++) {
                SearchResult searchResult = futures.get(index).get();
                finalResult.mergeFrom(searchResult);
            }

            metricsPublisher.putMetricData(MetricsPublisher.MetricNames.FINAL_RESULT_GENERATION_FOR_SINGLE_SPLIT, Duration.between(start, Instant.now()).toMillis(), queryId);

            // Avoid using executor, run a virtual thread to write the final result.
            Thread.ofVirtual().start(() -> {
                Instant writeStart = Instant.now();
                resultWriter.write(finalResult, queryResultLocation, indexPath, queryId);
                metricsPublisher.putMetricData(MetricsPublisher.MetricNames.WRITE_RESULT_TO_S3_FOR_SINGLE_INDEX, Duration.between(writeStart, Instant.now()).toMillis(), queryId);
            }).join();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException("Error in VT processing", e.getCause());
        }
    }

    private SearchResult searchSingleSplit(Query query, String queryId, Path outputDirectory) throws IOException {
        // search
        Instant start = Instant.now();
        try (Directory directory = new MMapDirectory(outputDirectory)) { //TODO: Is MMap meaningful?
            metricsPublisher.putMetricData(MetricsPublisher.MetricNames.CREATE_MEMORY_MAPPED_DIRECTORY, Duration.between(start, Instant.now()).toMillis(), queryId);

            // Verify the index by searching
            Instant createSearcherStart = Instant.now();
            DirectoryReader reader = DirectoryReader.open(directory);
            org.apache.lucene.search.IndexSearcher searcher = new org.apache.lucene.search.IndexSearcher(reader);
            metricsPublisher.putMetricData(MetricsPublisher.MetricNames.CREATE_SEARCHER, Duration.between(createSearcherStart, Instant.now()).toMillis(), queryId);

            Instant readResultStart = Instant.now();
            SearchResult searchResult = readResults(searcher, query);
            metricsPublisher.putMetricData(MetricsPublisher.MetricNames.READ_RESULTS, Duration.between(readResultStart, Instant.now()).toMillis(), queryId);

            metricsPublisher.putMetricData(MetricsPublisher.MetricNames.SEARCH_SINGLE_SPLIT, Duration.between(start, Instant.now()).toMillis(), queryId);
            return searchResult;
        }
    }

    private SearchResult searchSingleSplit(Directory directory, Query query, String queryId, Path outputDirectory) throws IOException {
        // search
        Instant start = Instant.now();
        metricsPublisher.putMetricData(MetricsPublisher.MetricNames.CREATE_MEMORY_MAPPED_DIRECTORY, Duration.between(start, Instant.now()).toMillis(), queryId);

        // Verify the index by searching
        Instant createSearcherStart = Instant.now();
        DirectoryReader reader = DirectoryReader.open(directory);
        org.apache.lucene.search.IndexSearcher searcher = new org.apache.lucene.search.IndexSearcher(reader);
        metricsPublisher.putMetricData(MetricsPublisher.MetricNames.CREATE_SEARCHER, Duration.between(createSearcherStart, Instant.now()).toMillis(), queryId);

        Instant readResultStart = Instant.now();
        SearchResult searchResult = readResults(searcher, query);
        metricsPublisher.putMetricData(MetricsPublisher.MetricNames.READ_RESULTS, Duration.between(readResultStart, Instant.now()).toMillis(), queryId);

        metricsPublisher.putMetricData(MetricsPublisher.MetricNames.SEARCH_SINGLE_SPLIT, Duration.between(start, Instant.now()).toMillis(), queryId);
        return searchResult;
    }

    void readSplitAndWriteLuceneSegment(Path outputDirectory, byte[] splitBytes, String splitName) throws IOException {
        String segmentName = splitName.substring("split".length());

        try (DataInputStream in = new DataInputStream(new FastByteArrayInputStream(splitBytes))) {
            int cfeLen = in.readInt();
            byte[] cfeBytes = new byte[cfeLen];
            in.readFully(cfeBytes);
            Files.write(outputDirectory.resolve(segmentName + ".cfe"), cfeBytes, StandardOpenOption.CREATE, StandardOpenOption.WRITE);

            int cfsLen = in.readInt();
            byte[] cfsBytes = new byte[cfsLen];
            in.readFully(cfsBytes);
            Files.write(outputDirectory.resolve(segmentName + ".cfs"), cfsBytes, StandardOpenOption.CREATE, StandardOpenOption.WRITE);

            int siLen = in.readInt();
            byte[] siBytes = new byte[siLen];
            in.readFully(siBytes);
            Files.write(outputDirectory.resolve(segmentName + ".si"), siBytes, StandardOpenOption.CREATE, StandardOpenOption.WRITE);

            long generation = in.readLong();

            int segLen = in.readInt();
            byte[] segmentsBytes = new byte[segLen];
            in.readFully(segmentsBytes);
            Files.write(outputDirectory.resolve("segments_" + generation), segmentsBytes, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        }
    }

    void readSplitAndWriteLuceneSegmentV2(Path outputDirectory, byte[] splitBytes, String splitName) throws IOException {
        String segmentName = splitName.substring("split".length());
        ScratchBuffer scratch = this.scratchBufferPool.acquire();

        try (DataInputStream in = new DataInputStream(new FastByteArrayInputStream(splitBytes))) {
            int cfeLen = in.readInt();
            scratch.cfe = scratch.ensureCapacity(scratch.cfe, cfeLen);
            in.readFully(scratch.cfe, 0, cfeLen);
            try (OutputStream out = Files.newOutputStream(outputDirectory.resolve(segmentName + ".cfe"))) {
                out.write(scratch.cfe, 0, cfeLen);
            }

            int cfsLen = in.readInt();
            scratch.cfs = scratch.ensureCapacity(scratch.cfs, cfsLen);
            in.readFully(scratch.cfs, 0, cfsLen);
            try (OutputStream out =
                         Files.newOutputStream(outputDirectory.resolve(segmentName + ".cfs"))) {
                out.write(scratch.cfs, 0, cfsLen);
            }

            int siLen = in.readInt();
            scratch.si = scratch.ensureCapacity(scratch.si, siLen);
            in.readFully(scratch.si, 0, siLen);
            try (OutputStream out = Files.newOutputStream(outputDirectory.resolve(segmentName + ".si"))) {
                out.write(scratch.si, 0, siLen);
            }

            long generation = in.readLong();

            int segLen = in.readInt();
            scratch.segments = scratch.ensureCapacity(scratch.segments, segLen);
            in.readFully(scratch.segments, 0, segLen);
            try (OutputStream out = Files.newOutputStream(outputDirectory.resolve("segments_" + generation))) {
                out.write(scratch.segments, 0, segLen);
            }
        } finally {
            this.scratchBufferPool.release(scratch);
        }
    }

    ByteBuffersDirectory readSplit(byte[] splitBytes, String splitName) throws IOException {
        String segmentName = splitName.substring("split".length());
        ScratchBuffer scratch = this.scratchBufferPool.acquire();

        ByteBuffersDirectory directory = new ByteBuffersDirectory();
        try (DataInputStream in = new DataInputStream(new FastByteArrayInputStream(splitBytes))) {
            int cfeLen = in.readInt();
            scratch.cfe = scratch.ensureCapacity(scratch.cfe, cfeLen);
            in.readFully(scratch.cfe, 0, cfeLen);
            try (IndexOutput output = directory.createOutput(segmentName + ".cfe", IOContext.DEFAULT)) {
                output.writeBytes(scratch.cfe, 0, cfeLen);
            }

            int cfsLen = in.readInt();
            scratch.cfs = scratch.ensureCapacity(scratch.cfs, cfsLen);
            in.readFully(scratch.cfs, 0, cfsLen);
            try (IndexOutput output = directory.createOutput(segmentName + ".cfs", IOContext.DEFAULT)) {
                output.writeBytes(scratch.cfs, 0, cfsLen);
            }

            int siLen = in.readInt();
            scratch.si = scratch.ensureCapacity(scratch.si, siLen);
            in.readFully(scratch.si, 0, siLen);
            try (IndexOutput output = directory.createOutput(segmentName + ".si", IOContext.DEFAULT)) {
                output.writeBytes(scratch.si, 0, siLen);
            }

            long generation = in.readLong();

            int segLen = in.readInt();
            scratch.segments = scratch.ensureCapacity(scratch.segments, segLen);
            in.readFully(scratch.segments, 0, segLen);
            try (IndexOutput output = directory.createOutput("segments_" + generation, IOContext.DEFAULT)) {
                output.writeBytes(scratch.segments, 0, segLen);
            }
        } finally {
            this.scratchBufferPool.release(scratch);
        }
        return directory;
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
                    this.deleteDirectory(file);
                } else {
                    System.out.println("Attempting to delete " + file.getName());
                    if (!file.delete()) {
                        System.out.println("Failed to delete: " + file.getAbsolutePath());
                    }
                }
            }
        }

        System.out.println("Attempting to delete " + directory.getName());
        // Delete the main directory itself
        if (!directory.delete()) {
            System.out.println("Failed to delete directory: " + directory.getAbsolutePath());
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

    @PreDestroy
    public void clean() {
        this.executor.shutdown();
    }
}
