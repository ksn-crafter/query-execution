package com.dqs.eventdrivensearch.queryExecution.search.index.cache;

import com.dqs.eventdrivensearch.queryExecution.search.io.S3IndexDownloader;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.lucene.store.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@Component
public class DirectoryCache {
    private final Map<String, Directory> cache = new HashMap<>(64);

    @Autowired
    S3IndexDownloader s3IndexDownloader;
    private static final String BASE_URL =
            "https://dqs-poc-indexes.s3.us-east-1.amazonaws.com/zip_indexes/128MB/";

//    String[] indexPaths = {
//            "part-00001-a38170e0-af09-4e18-998c-02ddaf6ec1f0-c000-index.zip",
//            "part-00001-830cec59-95a3-4fe7-af17-50102ffaf10e-c000-index.zip",
//            "part-00002-1d899d5b-a92e-4ccd-a8db-39818db5e781-c000-index.zip",
//            "part-00003-1d899d5b-a92e-4ccd-a8db-39818db5e781-c000-index.zip",
//            "part-00002-a38170e0-af09-4e18-998c-02ddaf6ec1f0-c000-index.zip",
//            "part-00002-1d899d5b-a92e-4ccd-a8db-39818db5e781-c000-index.zip",
//            "part-00002-b2b74b54-2968-433b-9f6c-4a07a2f964f9-c000-index.zip",
//            "part-00000-a38170e0-af09-4e18-998c-02ddaf6ec1f0-c000-index.zip",
//            "part-00000-1d899d5b-a92e-4ccd-a8db-39818db5e781-c000-index.zip",
//            "part-00001-830cec59-95a3-4fe7-af17-50102ffaf10e-c000-index.zip",
//            "part-00000-b2b74b54-2968-433b-9f6c-4a07a2f964f9-c000-index.zip",
//            "part-00000-1d899d5b-a92e-4ccd-a8db-39818db5e781-c000-index.zip",
//            "part-00002-830cec59-95a3-4fe7-af17-50102ffaf10e-c000-index.zip",
//            "part-00000-830cec59-95a3-4fe7-af17-50102ffaf10e-c000-index.zip",
//            "part-00002-b2b74b54-2968-433b-9f6c-4a07a2f964f9-c000-index.zip",
//            "part-00000-1d899d5b-a92e-4ccd-a8db-39818db5e781-c000-index.zip",
//            "part-00001-830cec59-95a3-4fe7-af17-50102ffaf10e-c000-index.zip",
//            "part-00001-b2b74b54-2968-433b-9f6c-4a07a2f964f9-c000-index.zip",
//            "part-00002-1d899d5b-a92e-4ccd-a8db-39818db5e781-c000-index.zip",
//            "part-00002-a38170e0-af09-4e18-998c-02ddaf6ec1f0-c000-index.zip",
//            "part-00002-1d899d5b-a92e-4ccd-a8db-39818db5e781-c000-index.zip",
//            "part-00000-b2b74b54-2968-433b-9f6c-4a07a2f964f9-c000-index.zip",
//            "part-00001-b2b74b54-2968-433b-9f6c-4a07a2f964f9-c000-index.zip",
//            "part-00003-830cec59-95a3-4fe7-af17-50102ffaf10e-c000-index.zip",
//            "part-00001-830cec59-95a3-4fe7-af17-50102ffaf10e-c000-index.zip",
//            "part-00000-b2b74b54-2968-433b-9f6c-4a07a2f964f9-c000-index.zip",
//            "part-00003-1d899d5b-a92e-4ccd-a8db-39818db5e781-c000-index.zip",
//            "part-00003-830cec59-95a3-4fe7-af17-50102ffaf10e-c000-index.zip",
//            "part-00002-a38170e0-af09-4e18-998c-02ddaf6ec1f0-c000-index.zip",
//            "part-00001-1d899d5b-a92e-4ccd-a8db-39818db5e781-c000-index.zip",
//            "part-00001-1d899d5b-a92e-4ccd-a8db-39818db5e781-c000-index.zip",
//            "part-00002-1d899d5b-a92e-4ccd-a8db-39818db5e781-c000-index.zip",
//            "part-00000-1d899d5b-a92e-4ccd-a8db-39818db5e781-c000-index.zip",
//            "part-00001-a38170e0-af09-4e18-998c-02ddaf6ec1f0-c000-index.zip",
//            "part-00000-b2b74b54-2968-433b-9f6c-4a07a2f964f9-c000-index.zip",
//            "part-00002-b2b74b54-2968-433b-9f6c-4a07a2f964f9-c000-index.zip",
//            "part-00003-1d899d5b-a92e-4ccd-a8db-39818db5e781-c000-index.zip",
//            "part-00001-a38170e0-af09-4e18-998c-02ddaf6ec1f0-c000-index.zip",
//            "part-00000-1d899d5b-a92e-4ccd-a8db-39818db5e781-c000-index.zip",
//            "part-00002-a38170e0-af09-4e18-998c-02ddaf6ec1f0-c000-index.zip",
//            "part-00000-b2b74b54-2968-433b-9f6c-4a07a2f964f9-c000-index.zip",
//            "part-00000-b2b74b54-2968-433b-9f6c-4a07a2f964f9-c000-index.zip",
//            "part-00000-1d899d5b-a92e-4ccd-a8db-39818db5e781-c000-index.zip",
//            "part-00000-a38170e0-af09-4e18-998c-02ddaf6ec1f0-c000-index.zip",
//            "part-00002-a38170e0-af09-4e18-998c-02ddaf6ec1f0-c000-index.zip",
//            "part-00000-830cec59-95a3-4fe7-af17-50102ffaf10e-c000-index.zip",
//            "part-00002-b2b74b54-2968-433b-9f6c-4a07a2f964f9-c000-index.zip",
//            "part-00000-1d899d5b-a92e-4ccd-a8db-39818db5e781-c000-index.zip",
//            "part-00000-1d899d5b-a92e-4ccd-a8db-39818db5e781-c000-index.zip",
//            "part-00003-830cec59-95a3-4fe7-af17-50102ffaf10e-c000-index.zip",
//            "part-00000-a38170e0-af09-4e18-998c-02ddaf6ec1f0-c000-index.zip",
//            "part-00002-b2b74b54-2968-433b-9f6c-4a07a2f964f9-c000-index.zip",
//            "part-00000-1d899d5b-a92e-4ccd-a8db-39818db5e781-c000-index.zip",
//            "part-00001-830cec59-95a3-4fe7-af17-50102ffaf10e-c000-index.zip",
//            "part-00002-830cec59-95a3-4fe7-af17-50102ffaf10e-c000-index.zip",
//            "part-00001-1d899d5b-a92e-4ccd-a8db-39818db5e781-c000-index.zip",
//            "part-00003-1d899d5b-a92e-4ccd-a8db-39818db5e781-c000-index.zip",
//            "part-00003-830cec59-95a3-4fe7-af17-50102ffaf10e-c000-index.zip",
//            "part-00001-a38170e0-af09-4e18-998c-02ddaf6ec1f0-c000-index.zip",
//            "part-00000-830cec59-95a3-4fe7-af17-50102ffaf10e-c000-index.zip",
//            "part-00002-830cec59-95a3-4fe7-af17-50102ffaf10e-c000-index.zip",
//            "part-00000-a38170e0-af09-4e18-998c-02ddaf6ec1f0-c000-index.zip",
//            "part-00002-1d899d5b-a92e-4ccd-a8db-39818db5e781-c000-index.zip",
//            "part-00000-a38170e0-af09-4e18-998c-02ddaf6ec1f0-c000-index.zip"
//    };

    String[] indexPaths = {
           "part-00002-1d899d5b-a92e-4ccd-a8db-39818db5e781-c000-index.zip",
            "part-00001-830cec59-95a3-4fe7-af17-50102ffaf10e-c000-index.zip",
            "part-00000-1d899d5b-a92e-4ccd-a8db-39818db5e781-c000-index.zip",
            "part-00000-b2b74b54-2968-433b-9f6c-4a07a2f964f9-c000-index.zip",
    };

    @PostConstruct
    public void fillCache() {
        for (String indexPath : indexPaths) {
            String path = BASE_URL + indexPath;
            cache.put(path, loadDirectoryFromZip(path));
        }
    }

    public Directory get(String indexPath) {
        return cache.get(indexPath);
    }

    private Directory loadDirectoryFromZip(String zipFilePath) {
        try {
            return downloadZipAndUnzipInDirectory(zipFilePath);
        } catch (Exception e) {
            return null;
        }
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


