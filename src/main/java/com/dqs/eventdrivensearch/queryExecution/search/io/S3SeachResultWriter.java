package com.dqs.eventdrivensearch.queryExecution.search.io;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class S3SeachResultWriter {

    @Autowired
    private S3Client s3Client;

    final private List<String> documentIds;
    final private int total;
    final private long totalHits;
    final private int totalMatchedDocuments;

    public S3SeachResultWriter(List<String> documentIds, int total, long totalHits, int totalMatchedDocuments) {
        this.documentIds = documentIds;
        this.total = total;
        this.totalHits = totalHits;
        this.totalMatchedDocuments = totalMatchedDocuments;
    }

    public void write(String outPutFolderPath, String documentFilePath) {
        String filePath = getFilePath(outPutFolderPath);

        try {

            URL url = new URL(filePath);
            String bucketName = url.getHost().split("\\.")[0];
            String key = url.getPath().substring(1);

            String content = documentFilePath + "\n" + String.join("\n", documentIds);
            s3Client.putObject(PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build(), RequestBody.fromString(content));

        } catch (MalformedURLException | NoSuchKeyException e) {
            System.out.println(Level.WARNING +  e.getMessage() + "\n" + e.getStackTrace() + "\n" + "outPutFolderPath: " + outPutFolderPath);
            throw new RuntimeException(e);
        }
    }

    private String getFilePath(String folderPath) {
        return folderPath + "/" + UUID.randomUUID() + "_" + total + "_" + totalHits + "_" + totalMatchedDocuments + ".txt";
    }
}