package com.dqs.eventdrivensearch.queryExecution.search.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

@Configuration
public class S3CRTClientConfiguration {

    @Bean
    @Profile("!test")
    public S3AsyncClient s3AsyncClient() {
        return S3AsyncClient.crtBuilder()
                .region(Region.US_EAST_1)
                .minimumPartSizeInBytes(4 * 1024 * 1024L)
                .build();
    }
}

