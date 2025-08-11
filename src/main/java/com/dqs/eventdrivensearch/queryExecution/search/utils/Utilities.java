package com.dqs.eventdrivensearch.queryExecution.search.utils;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class Utilities {
    private static final String[] DOCUMENT_FIELDS = {"body", "subject", "date", "from", "to", "cc", "bcc"};

    public static void readAndUnzipInDirectory(InputStream inputStream, Path outputDir) throws IOException {
        if (!Files.exists(outputDir)) {
            Files.createDirectories(outputDir);
        }

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
    }

    public static Query getQuery(String queryString, StandardAnalyzer analyzer) throws ParseException {
        MultiFieldQueryParser parser = new MultiFieldQueryParser(DOCUMENT_FIELDS, analyzer);
        return parser.parse(queryString);
    }

}
