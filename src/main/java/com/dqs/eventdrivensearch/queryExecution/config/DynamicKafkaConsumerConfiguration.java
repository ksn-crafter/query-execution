package com.dqs.eventdrivensearch.queryExecution.config;


import com.dqs.eventdrivensearch.queryExecution.consumer.SubQueryGeneratedConsumer;
import com.dqs.eventdrivensearch.queryExecution.event.SubQueryGenerated;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;

import java.util.ArrayList;
import java.util.List;

@Configuration
public class DynamicKafkaConsumerConfiguration {

    @Autowired
    private ConsumerFactory<String, SubQueryGenerated> consumerFactory;

    @Autowired
    private SubQueryGeneratedConsumer subQueryGeneratedConsumer;

    private final List<KafkaMessageListenerContainer<String, SubQueryGenerated>> containers = new ArrayList<>();

    public void registerConsumerForTopic(String tenant) {
        String topic = "incoming_sub_queries_" + tenant;
        String groupId = "incoming_sub_queries_group_" + tenant;

        ContainerProperties containerProps = new ContainerProperties(topic);
        containerProps.setGroupId(groupId);
        containerProps.setMessageListener((MessageListener<String, SubQueryGenerated>) record -> {
            SubQueryGenerated event = record.value();
            subQueryGeneratedConsumer.consume(event);
        });

        KafkaMessageListenerContainer<String, SubQueryGenerated> container =
                new KafkaMessageListenerContainer<>(consumerFactory, containerProps);

        container.start();
        containers.add(container);
    }

    @PreDestroy
    public void stopAllContainers() {
        for (KafkaMessageListenerContainer<?, ?> container : containers) {
            container.stop();
        }
    }
}

