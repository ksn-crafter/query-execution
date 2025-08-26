package com.dqs.eventdrivensearch.queryExecution.search.index.cache;

import com.dqs.eventdrivensearch.queryExecution.search.io.S3IndexDownloader;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.lucene.store.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@Component
public class DirectoryCache {
    private final Map<String, Directory> cache = new HashMap<>(64);

    @Autowired
    S3IndexDownloader s3IndexDownloader;

    @Value("${s3_index_url_prefix}")
    private static String indexUrlPrefix;

    String[] indexPaths = {};

    @PostConstruct
    public void fillCache() {
        for (String indexPath : indexPaths) {
            String path = indexUrlPrefix + indexPath;
            cache.put(path, loadDirectoryFromZip(path));
        }
    }

    public Directory get(String indexPath) {
        return cache.get(indexPath);
    }

    private Directory loadDirectoryFromZip(String zipFilePath) {
        //TODO: make this configurable between byte buffers and Mmap
        try {
            //return downloadZipAndUnzipInDirectory(zipFilePath);
            return downloadZipAndCreateMMapDirectory(zipFilePath);
        } catch (Exception e) {
            return null;
        }
    }

    private Directory downloadZipAndCreateMMapDirectory(String zipFilePath) throws IOException {
        Path outputDir = Files.createTempDirectory("tempDirPrefix-");

        if (!Files.exists(outputDir)) {
            Files.createDirectories(outputDir);
        }

        InputStream inputStream = s3IndexDownloader.getInputStream(zipFilePath, "");

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

        return new MMapDirectory(outputDir);
    }

    private Directory downloadZipAndUnzipInDirectory(String zipFilePath) throws IOException {
        Directory byteBuffersDirectory = new ByteBuffersDirectory();

        InputStream inputStream = s3IndexDownloader.getInputStream(zipFilePath, "startup");

        final int OPTIMAL_STREAM_BUFFER_SIZE = 1048576;
        try (ZipInputStream zipIn = new ZipInputStream(new BufferedInputStream(inputStream, OPTIMAL_STREAM_BUFFER_SIZE))) {
            byte[] zipStreamBuffer = new byte[OPTIMAL_STREAM_BUFFER_SIZE];
            ZipEntry entry;
            while ((entry = zipIn.getNextEntry()) != null) {
                if (!entry.isDirectory()) {
                    try (IndexOutput output = byteBuffersDirectory.createOutput(entry.getName(), IOContext.DEFAULT)) {
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
        return byteBuffersDirectory;
    }

    @PreDestroy
    public void cleanup() {
        for (Directory dir : cache.values()) {
            try {
                // Close the Lucene Directory
                dir.close();

                // Delete the underlying temp directory (if FSDirectory or MMapDirectory)
                if (dir instanceof FSDirectory) {
                    File directoryFile = ((FSDirectory) dir).getDirectory().toFile();
                    deleteTempDirectory(directoryFile);
                    // also remove the directory folder itself
                    directoryFile.delete();
                }

            } catch (IOException e) {
                System.err.println("Failed to close directory: " + e.getMessage());
            }
        }
        cache.clear();
    }

    private void deleteTempDirectory(File directory) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    // recursive cleanup
                    deleteTempDirectory(file);
                    file.delete();
                } else {
                    file.delete();
                }
            }
        }
    }


}


