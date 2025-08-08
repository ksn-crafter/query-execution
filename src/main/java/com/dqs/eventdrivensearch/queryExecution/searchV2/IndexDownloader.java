package com.dqs.eventdrivensearch.queryExecution.searchV2;

import com.dqs.eventdrivensearch.queryExecution.search.metrics.MetricsPublisher;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@Component
public class IndexDownloader {
    private final IndexQueue indexPaths;
    private final S3Adapter s3Adapter;
    private final MetricsPublisher metricsPublisher;

    public IndexDownloader(IndexQueue indexPaths,S3Adapter s3Adapter,MetricsPublisher metricsPublisher) {
        this.indexPaths = indexPaths;
        this.s3Adapter = s3Adapter;
        this.metricsPublisher = metricsPublisher;
    }

    public void downloadIndices(String[] s3IndexUrls,String queryId) throws InterruptedException, IOException {
        List<Thread> downloadTasks = new ArrayList<>();
        for (String s3IndexUrl : s3IndexUrls) {
           Thread downloadTask = Thread.startVirtualThread(() ->{
                try {
                    downloadIndex(s3IndexUrl,queryId);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
           downloadTasks.add(downloadTask);
           //TODO: remove this hardcoding of number of virtual threads
           if(downloadTasks.size() == 4){
               do{
                   for(int idx=0;idx<downloadTasks.size();idx++){
                       if(downloadTasks.get(idx).getState() == Thread.State.TERMINATED){
                           downloadTasks.remove(idx);
                           break;
                       }
                   }
               }while(downloadTasks.size() == 4);
           }
        }
    }

    private void downloadIndex(String s3IndexUrl,String queryId) throws InterruptedException, IOException {
        InputStream indexInputStream = s3Adapter.getInputStream(s3IndexUrl,queryId);
        indexPaths.put(unzipToDirectory(indexInputStream,queryId));
    }

    private Path unzipToDirectory(InputStream indexInputStream,String queryId) throws InterruptedException, IOException {
        Instant start = Instant.now();
        Path indexDirectory = Files.createTempDirectory("indexDir-");

        final int OPTIMAL_STREAM_BUFFER_SIZE = 1048576;
        try (ZipInputStream zipIn = new ZipInputStream(new BufferedInputStream(indexInputStream, OPTIMAL_STREAM_BUFFER_SIZE))) {
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
        } catch (IOException e) {
            //TODO: log the exception here
        } finally {
            metricsPublisher.putMetricData(MetricsPublisher.MetricNames.UNZIP_SINGLE_INDEX_SHARD, Duration.between(start, Instant.now()).toMillis(), queryId);

            indexInputStream.close();
        }
        return indexDirectory;
    }
}
