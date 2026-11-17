package io.github.yassaminheidari.dispatch.api.config;

import io.github.yassaminheidari.dispatch.common.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicsConfig {

    @Bean
    NewTopic dispatchTopic() {
        return TopicBuilder.name(Topics.DISPATCH).partitions(Topics.PARTITIONS).replicas(1).build();
    }

    @Bean
    NewTopic dispatchDeadLetterTopic() {
        return TopicBuilder.name(Topics.DISPATCH_DLT).partitions(Topics.PARTITIONS).replicas(1).build();
    }

    @Bean
    NewTopic statusTopic() {
        return TopicBuilder.name(Topics.STATUS).partitions(Topics.PARTITIONS).replicas(1).build();
    }
}
