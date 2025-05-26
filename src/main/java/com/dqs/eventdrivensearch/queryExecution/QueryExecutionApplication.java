package com.dqs.eventdrivensearch.queryExecution;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
public class QueryExecutionApplication {

    public static void main(String[] args) {
        SpringApplication.run(QueryExecutionApplication.class, args);
    }

}
