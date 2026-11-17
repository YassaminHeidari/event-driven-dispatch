package io.github.yassaminheidari.dispatch.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.yassaminheidari.dispatch.common.JobDispatchEvent;
import io.github.yassaminheidari.dispatch.common.JobStatus;
import io.github.yassaminheidari.dispatch.common.JobStatusEvent;
import io.github.yassaminheidari.dispatch.common.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs against real Kafka and Redis (docker compose locally, service containers in CI).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
        "dispatch.worker.retry.initial-interval-ms=50",
        "dispatch.worker.retry.max-interval-ms=200"
})
class DispatchListenerIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(45);

    @Autowired
    KafkaTemplate<String, String> kafka;
    @Autowired
    ObjectMapper mapper;
    @Autowired
    StringRedisTemplate redis;
    @Autowired
    WorkerMetrics metrics;
    @Value("${spring.kafka.bootstrap-servers}")
    String bootstrap;

    @Test
    void duplicateDeliveriesOfTheSameEventRunTheJobOnce() throws Exception {
        JobDispatchEvent event = event("echo", "{\"durationMs\":5}");
        long skippedBefore = metrics.duplicatesSkipped.get();

        try (KafkaTopicReader status = new KafkaTopicReader(bootstrap, Topics.STATUS)) {
            String json = mapper.writeValueAsString(event);
            // The same message three times: what an outbox relay retry or a consumer rebalance can produce.
            for (int i = 0; i < 3; i++) {
                kafka.send(Topics.DISPATCH, event.jobId().toString(), json).get();
            }

            List<JobStatusEvent> events = statuses(status.await(event.jobId().toString(),
                    rs -> metrics.duplicatesSkipped.get() - skippedBefore >= 2 && hasStatus(rs, JobStatus.COMPLETED),
                    TIMEOUT, Duration.ofSeconds(2)));

            assertThat(events).filteredOn(e -> e.status() == JobStatus.COMPLETED).hasSize(1);
            assertThat(events).filteredOn(e -> e.status() == JobStatus.RUNNING).hasSize(1);
            assertThat(metrics.duplicatesSkipped.get() - skippedBefore).isEqualTo(2);
            assertThat(redis.opsForValue().get(IdempotencyGuard.key(event.eventId()))).isEqualTo("DONE");
        }
    }

    @Test
    void transientFailuresAreRetriedWithBackoffUntilTheJobSucceeds() throws Exception {
        JobDispatchEvent event = event("flaky", "{\"failTimes\":2}");

        try (KafkaTopicReader status = new KafkaTopicReader(bootstrap, Topics.STATUS)) {
            kafka.send(Topics.DISPATCH, event.jobId().toString(), mapper.writeValueAsString(event)).get();

            List<JobStatusEvent> events = statuses(status.await(event.jobId().toString(),
                    rs -> hasStatus(rs, JobStatus.COMPLETED), TIMEOUT, Duration.ofSeconds(1)));

            assertThat(events).filteredOn(e -> e.status() == JobStatus.RETRYING).hasSize(2);
            assertThat(events).filteredOn(e -> e.status() == JobStatus.COMPLETED)
                    .singleElement()
                    .satisfies(e -> assertThat(e.attempt()).isEqualTo(3));
        }
    }

    @Test
    void permanentFailuresGoStraightToTheDeadLetterTopic() throws Exception {
        JobDispatchEvent event = event("fail", "{}");

        try (KafkaTopicReader dlt = new KafkaTopicReader(bootstrap, Topics.DISPATCH_DLT)) {
            kafka.send(Topics.DISPATCH, event.jobId().toString(), mapper.writeValueAsString(event)).get();

            List<ConsumerRecord<String, String>> records = dlt.await(event.jobId().toString(),
                    rs -> !rs.isEmpty(), TIMEOUT, Duration.ofSeconds(1));

            assertThat(records).hasSize(1);
            ConsumerRecord<String, String> record = records.get(0);
            assertThat(mapper.readValue(record.value(), JobDispatchEvent.class).eventId()).isEqualTo(event.eventId());
            assertThat(header(record, KafkaHeaders.DLT_EXCEPTION_FQCN) + header(record, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN))
                    .contains(NonRetryableJobException.class.getName());
            // Not retryable, so exactly one attempt.
            assertThat(redis.opsForValue().get("attempts:" + event.eventId())).isEqualTo("1");
        }
    }

    @Test
    void retriesThatNeverSucceedEndInTheDeadLetterTopic() throws Exception {
        // failTimes is larger than 1 + max-retries (3), so every attempt fails.
        JobDispatchEvent event = event("flaky", "{\"failTimes\":10}");

        try (KafkaTopicReader dlt = new KafkaTopicReader(bootstrap, Topics.DISPATCH_DLT)) {
            kafka.send(Topics.DISPATCH, event.jobId().toString(), mapper.writeValueAsString(event)).get();

            List<ConsumerRecord<String, String>> records = dlt.await(event.jobId().toString(),
                    rs -> !rs.isEmpty(), TIMEOUT, Duration.ofSeconds(1));

            assertThat(records).hasSize(1);
            assertThat(redis.opsForValue().get("attempts:" + event.eventId())).isEqualTo("4");
        }
    }

    private static JobDispatchEvent event(String type, String payload) {
        return new JobDispatchEvent(UUID.randomUUID(), UUID.randomUUID(), type, payload, Instant.now());
    }

    private boolean hasStatus(List<ConsumerRecord<String, String>> records, JobStatus status) {
        return statuses(records).stream().anyMatch(e -> e.status() == status);
    }

    private List<JobStatusEvent> statuses(List<ConsumerRecord<String, String>> records) {
        return records.stream().map(r -> {
            try {
                return mapper.readValue(r.value(), JobStatusEvent.class);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }).toList();
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null ? "" : new String(h.value(), StandardCharsets.UTF_8);
    }
}
