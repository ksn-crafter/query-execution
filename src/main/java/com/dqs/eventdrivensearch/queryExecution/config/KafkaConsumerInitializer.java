package com.dqs.eventdrivensearch.queryExecution.config;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class KafkaConsumerInitializer {

    @Autowired
    private DynamicKafkaConsumerConfiguration dynamicKafkaConsumerConfiguration;

    @Value("${tenant_ids}")
    private String tenants;

    @PostConstruct
    public void initializeListeners() {
        List<String> tenantIds = List.of(tenants.split(","));
        for (String suffix : tenantIds) {
            dynamicKafkaConsumerConfiguration.registerConsumerForTopic(suffix);
        }
    }
}
