package io.github.yassaminheidari.dispatch.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.yassaminheidari.dispatch.api.job.CreateJobRequest;
import io.github.yassaminheidari.dispatch.api.job.JobRepository;
import io.github.yassaminheidari.dispatch.api.job.JobService;
import io.github.yassaminheidari.dispatch.api.job.JobView;
import io.github.yassaminheidari.dispatch.common.JobDispatchEvent;
import io.github.yassaminheidari.dispatch.common.JobStatus;
import io.github.yassaminheidari.dispatch.common.JobStatusEvent;
import io.github.yassaminheidari.dispatch.common.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Runs against real PostgreSQL, Kafka and Redis (docker compose locally, service containers in CI).
 * No worker runs here, so the tests play the worker's part by writing to jobs.status and jobs.dispatch.DLT.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class DispatchApiIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Autowired
    TestRestTemplate http;
    @Autowired
    JobService jobService;
    @Autowired
    JobRepository jobs;
    @Autowired
    JdbcTemplate jdbc;
    @Autowired
    TransactionTemplate tx;
    @Autowired
    KafkaTemplate<String, String> kafka;
    @Autowired
    ObjectMapper mapper;
    @Value("${spring.kafka.bootstrap-servers}")
    String bootstrap;
    @LocalServerPort
    int port;

    @Test
    void postedJobIsStoredAndPublishedToKafkaThroughTheOutbox() throws Exception {
        try (KafkaTopicReader dispatch = new KafkaTopicReader(bootstrap, Topics.DISPATCH)) {
            ResponseEntity<JobView> response = http.postForEntity("/api/jobs",
                    Map.of("type", "echo", "payload", Map.of("durationMs", 5)), JobView.class);

            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
            UUID jobId = response.getBody().id();

            List<ConsumerRecord<String, String>> records = dispatch.await(jobId.toString(), rs -> !rs.isEmpty(),
                    TIMEOUT, Duration.ofMillis(500));
            assertThat(records).hasSize(1);
            JobDispatchEvent event = mapper.readValue(records.get(0).value(), JobDispatchEvent.class);
            assertThat(event.jobId()).isEqualTo(jobId);
            assertThat(event.type()).isEqualTo("echo");

            await().atMost(TIMEOUT).until(() -> jobs.findById(jobId).orElseThrow().status() == JobStatus.DISPATCHED);
            assertThat(jdbc.queryForObject(
                    "select count(*) from outbox_events where aggregate_id = ? and published_at is not null",
                    Long.class, jobId)).isEqualTo(1L);
        }
    }

    @Test
    void jobRowAndOutboxRowRollBackTogether() {
        AtomicReference<UUID> jobId = new AtomicReference<>();

        assertThatThrownBy(() -> tx.executeWithoutResult(status -> {
            jobId.set(jobService.create(new CreateJobRequest("echo", null)).id());
            throw new IllegalStateException("simulated failure after the write");
        })).hasMessageContaining("simulated failure");

        assertThat(jobId.get()).isNotNull();
        assertThat(jdbc.queryForObject("select count(*) from jobs where id = ?", Long.class, jobId.get())).isZero();
        assertThat(jdbc.queryForObject("select count(*) from outbox_events where aggregate_id = ?", Long.class,
                jobId.get())).isZero();
    }

    @Test
    void duplicateStatusEventsAreAppliedOnlyOnce() throws Exception {
        UUID jobId = jobService.create(new CreateJobRequest("echo", null)).id();
        JobStatusEvent running = new JobStatusEvent(UUID.randomUUID(), jobId, JobStatus.RUNNING, "worker-test", 1,
                null, Instant.now());
        JobStatusEvent completed = new JobStatusEvent(UUID.randomUUID(), jobId, JobStatus.COMPLETED, "worker-test",
                1, "ok", Instant.now());

        // Each event is delivered twice, as Kafka may do after a producer retry or a rebalance.
        for (JobStatusEvent e : List.of(running, running, completed, completed)) {
            kafka.send(Topics.STATUS, jobId.toString(), mapper.writeValueAsString(e)).get();
        }

        await().atMost(TIMEOUT).until(() -> jobs.findById(jobId).orElseThrow().status() == JobStatus.COMPLETED);
        Thread.sleep(1_000); // give the duplicate of COMPLETED time to arrive

        JobView job = jobs.findById(jobId).orElseThrow();
        assertThat(job.statusEventsApplied()).isEqualTo(2);
        assertThat(job.workerId()).isEqualTo("worker-test");
        assertThat(job.completedAt()).isNotNull();
        assertThat(jdbc.queryForObject("select count(*) from processed_events where event_id in (?, ?)", Long.class,
                running.eventId(), completed.eventId())).isEqualTo(2L);
    }

    @Test
    void lateStatusEventCannotReopenAFinishedJob() throws Exception {
        UUID jobId = jobService.create(new CreateJobRequest("echo", null)).id();
        JobStatusEvent completed = new JobStatusEvent(UUID.randomUUID(), jobId, JobStatus.COMPLETED, "w", 1, "ok",
                Instant.now());
        JobStatusEvent lateRetry = new JobStatusEvent(UUID.randomUUID(), jobId, JobStatus.RETRYING, "w", 1, "late",
                Instant.now());

        kafka.send(Topics.STATUS, jobId.toString(), mapper.writeValueAsString(completed)).get();
        kafka.send(Topics.STATUS, jobId.toString(), mapper.writeValueAsString(lateRetry)).get();

        await().atMost(TIMEOUT).until(() -> jdbc.queryForObject(
                "select count(*) from processed_events where event_id = ?", Long.class, lateRetry.eventId()) == 1L);
        assertThat(jobs.findById(jobId).orElseThrow().status()).isEqualTo(JobStatus.COMPLETED);
    }

    @Test
    void deadLetteredJobIsMarkedFailed() throws Exception {
        JobView job = jobService.create(new CreateJobRequest("fail", null));
        JobDispatchEvent event = new JobDispatchEvent(UUID.randomUUID(), job.id(), "fail", "{}", job.createdAt());
        ProducerRecord<String, String> record = new ProducerRecord<>(Topics.DISPATCH_DLT, job.id().toString(),
                mapper.writeValueAsString(event));
        record.headers().add(KafkaHeaders.DLT_EXCEPTION_MESSAGE, "boom".getBytes(StandardCharsets.UTF_8));

        kafka.send(record).get();

        await().atMost(TIMEOUT).until(() -> jobs.findById(job.id()).orElseThrow().status() == JobStatus.FAILED);
        assertThat(jobs.findById(job.id()).orElseThrow().detail()).contains("boom");
    }

    @Test
    void statusChangesArePushedToWebSocketClients() throws Exception {
        BlockingQueue<String> received = new LinkedBlockingQueue<>();
        WebSocketSession session = new StandardWebSocketClient().execute(new TextWebSocketHandler() {
            @Override
            protected void handleTextMessage(WebSocketSession s, TextMessage message) {
                received.add(message.getPayload());
            }
        }, "ws://localhost:" + port + "/ws/jobs").get(10, TimeUnit.SECONDS);

        try {
            UUID jobId = jobService.create(new CreateJobRequest("echo", null)).id();
            JobStatusEvent completed = new JobStatusEvent(UUID.randomUUID(), jobId, JobStatus.COMPLETED, "w", 1,
                    "ok", Instant.now());
            kafka.send(Topics.STATUS, jobId.toString(), mapper.writeValueAsString(completed)).get();

            String match = null;
            long end = System.nanoTime() + TIMEOUT.toNanos();
            while (match == null && System.nanoTime() < end) {
                String message = received.poll(500, TimeUnit.MILLISECONDS);
                if (message != null && message.contains(jobId.toString()) && message.contains("COMPLETED")) {
                    match = message;
                }
            }
            assertThat(match).as("WebSocket update for job " + jobId).isNotNull();
        } finally {
            session.close();
        }
    }

    @Test
    void invalidJobTypeIsRejected() {
        ResponseEntity<String> response = http.postForEntity("/api/jobs", Map.of("type", "BAD TYPE!"), String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }
}
