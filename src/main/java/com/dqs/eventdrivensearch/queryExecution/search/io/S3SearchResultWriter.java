package com.dqs.eventdrivensearch.queryExecution.search.io;

import com.dqs.eventdrivensearch.queryExecution.search.model.SearchResult;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.UUID;
import java.util.logging.Level;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class S3SearchResultWriter {

    final private S3Client s3Client;

    public S3SearchResultWriter(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    public void write(SearchResult searchResult, String outPutFolderPath, String indexFilePath) {
        String filePath = getFilePath(searchResult, outPutFolderPath);

        try {

            URL url = new URL(filePath);
            String bucketName = url.getHost().split("\\.")[0];
            String key = url.getPath().substring(1);

            String content = indexFilePath + "\n" + String.join("\n", searchResult.documentIds());
            s3Client.putObject(PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build(), RequestBody.fromString(content));

        } catch (MalformedURLException | NoSuchKeyException e) {
            System.out.println(Level.WARNING + e.getMessage() + "\n" + e.getStackTrace() + "\n" + "outPutFolderPath: " + outPutFolderPath);
            throw new RuntimeException(e);
        }
    }

    private String getFilePath(SearchResult searchResult, String folderPath) {
        return folderPath + "/" + UUID.randomUUID() + "_" + searchResult.total() + "_" + searchResult.totalHits() + "_" + searchResult.totalMatchedDocuments() + ".txt";
    }
}