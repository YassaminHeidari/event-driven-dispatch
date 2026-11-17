package io.github.yassaminheidari.dispatch.worker;

import io.github.yassaminheidari.dispatch.common.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Configuration
public class KafkaErrorHandlingConfig {

    /**
     * Retries with exponential backoff, then sends the record to jobs.dispatch.DLT.
     * The recoverer keeps the original partition and adds headers with the exception class and message.
     * Spring Boot plugs this bean into the auto-configured listener container factory.
     */
    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> template,
                                          @Value("${dispatch.worker.retry.max-retries:3}") int maxRetries,
                                          @Value("${dispatch.worker.retry.initial-interval-ms:200}") long initial,
                                          @Value("${dispatch.worker.retry.multiplier:2.0}") double multiplier,
                                          @Value("${dispatch.worker.retry.max-interval-ms:2000}") long maxInterval) {
        // Name the DLT explicitly (the library default suffix differs between Spring Kafka versions)
        // and keep the original partition, so records for one job stay together.
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
                (record, exception) -> new TopicPartition(Topics.DISPATCH_DLT, record.partition()));

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(maxRetries);
        backOff.setInitialInterval(initial);
        backOff.setMultiplier(multiplier);
        backOff.setMaxInterval(maxInterval);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(NonRetryableJobException.class);
        return handler;
    }

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
