package io.github.yassaminheidari.dispatch.worker;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/** Reads every partition of a topic from the start, without a consumer group, and filters by key. */
public class KafkaTopicReader implements AutoCloseable {

    private final KafkaConsumer<String, String> consumer;

    public KafkaTopicReader(String bootstrapServers, String topic) {
        consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));
        List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                .map(p -> new TopicPartition(topic, p.partition()))
                .toList();
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);
    }

    /**
     * Polls until {@code done} is true for the records with this key (or until the timeout),
     * then keeps polling for {@code grace} to catch any unwanted extra record.
     */
    public List<ConsumerRecord<String, String>> await(String key, Predicate<List<ConsumerRecord<String, String>>> done,
                                                      Duration timeout, Duration grace) {
        List<ConsumerRecord<String, String>> matched = new ArrayList<>();
        long end = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < end && !done.test(matched)) {
            poll(key, matched);
        }
        long graceEnd = System.nanoTime() + grace.toNanos();
        while (System.nanoTime() < graceEnd) {
            poll(key, matched);
        }
        return matched;
    }

    private void poll(String key, List<ConsumerRecord<String, String>> matched) {
        for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(200))) {
            if (key.equals(r.key())) {
                matched.add(r);
            }
        }
    }

    @Override
    public void close() {
        consumer.close();
    }
}
