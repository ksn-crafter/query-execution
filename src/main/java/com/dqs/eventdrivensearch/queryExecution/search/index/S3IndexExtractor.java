package com.dqs.eventdrivensearch.queryExecution.search.index;


import com.dqs.eventdrivensearch.queryExecution.search.io.S3IndexDownloader;
import com.dqs.eventdrivensearch.queryExecution.search.model.SearchTask;
import com.dqs.eventdrivensearch.queryExecution.search.queue.SearchTaskQueue;
import jakarta.annotation.PreDestroy;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
public class S3IndexExtractor {
    private static final String[] DOCUMENT_FIELDS = {"body", "subject", "date", "from", "to", "cc", "bcc"};
    @Autowired
    S3IndexDownloader s3IndexDownloader;

    @Autowired
    SearchTaskQueue searchTaskQueue;

    private static final Logger logger = Logger.getLogger(S3IndexExtractor.class.getName());
    private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor(); // TODO:reconsider/unbounded as of now

    public void extract(String queryId, String subQueryId, String query, String[] indexPaths) {
        try {

            for (String indexPath : indexPaths) {
                executorService.submit(() -> {
                    try {
                        Path indexDirectory = prepareIndexDirectory(indexPath, queryId);
                        searchTaskQueue.submitTask(new SearchTask(indexDirectory, getQuery(query), queryId,subQueryId));
                    } catch (IOException e) {
                        logger.log(Level.WARNING, String.format("Index extraction failed for queryId=%s, subQueryId=%s, indexPath=%s", queryId, subQueryId, indexPath), e);
                        throw new RuntimeException(e);
                    } catch (ParseException e) {
                        logger.log(Level.WARNING, String.format("Query parsing failed for queryId=%s, subQueryId=%s, indexPath=%s", queryId, subQueryId, indexPath), e);
                        throw new RuntimeException(e);
                    }
                });
            }

        } catch (Exception e) {
            System.out.println(e.getMessage() + "\n" + e.getStackTrace().toString());
            throw e;
        }
    }

    private Path prepareIndexDirectory(String indexPath, String queryId) throws IOException {
        Path indexDirectory = Files.createTempDirectory("tempDirPrefix-");
        if (!Files.exists(indexDirectory)) {
            Files.createDirectories(indexDirectory);
        }

        InputStream inputStream = s3IndexDownloader.getInputStream(indexPath, queryId);

        final int OPTIMAL_STREAM_BUFFER_SIZE = 1048576;
        try (ZipInputStream zipIn = new ZipInputStream(new BufferedInputStream(inputStream, OPTIMAL_STREAM_BUFFER_SIZE))) {
            byte[] zipStreamBuffer = new byte[OPTIMAL_STREAM_BUFFER_SIZE];
            ZipEntry entry;
            while ((entry = zipIn.getNextEntry()) != null) {
                Path filePath = indexDirectory.resolve(entry.getName());
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

        return indexDirectory;
    }

    Query getQuery(String queryString) throws ParseException {
        MultiFieldQueryParser parser = new MultiFieldQueryParser(DOCUMENT_FIELDS, new StandardAnalyzer());
        return parser.parse(queryString);
    }

    @PreDestroy
    public void shutdown() {
        executorService.shutdown();
    }
}
