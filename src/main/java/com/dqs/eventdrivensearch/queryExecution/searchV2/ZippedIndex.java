package com.dqs.eventdrivensearch.queryExecution.searchV2;

import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@Component
public class ZippedIndex {
     public Path unzip(InputStream zipIndexInputStream) throws IOException {
        Path indexDirectory = Files.createTempDirectory("indexDir-");
        if (!Files.exists(indexDirectory)) {
            Files.createDirectories(indexDirectory);
        }

        final int OPTIMAL_STREAM_BUFFER_SIZE = 1048576;
        try (ZipInputStream zipIn = new ZipInputStream(new BufferedInputStream(zipIndexInputStream, OPTIMAL_STREAM_BUFFER_SIZE))) {
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
            zipIndexInputStream.close();
        }
        return indexDirectory;
    }
}
