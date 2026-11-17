package io.github.yassaminheidari.dispatch.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.yassaminheidari.dispatch.common.JobStatus;
import io.github.yassaminheidari.dispatch.common.JobStatusEvent;
import io.github.yassaminheidari.dispatch.common.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Component
public class StatusPublisher {

    private static final Logger log = LoggerFactory.getLogger(StatusPublisher.class);

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final WorkerIdentity identity;

    public StatusPublisher(KafkaTemplate<String, String> kafka, ObjectMapper mapper, WorkerIdentity identity) {
        this.kafka = kafka;
        this.mapper = mapper;
        this.identity = identity;
    }

    /** Fire-and-forget. Used for progress updates (RUNNING, RETRYING) where a lost update is harmless. */
    public void publishAsync(UUID jobId, JobStatus status, int attempt, String detail) {
        send(jobId, status, attempt, detail).whenComplete((r, e) -> {
            if (e != null) {
                log.warn("Could not publish {} for job {}: {}", status, jobId, e.toString());
            }
        });
    }

    /** Waits for the broker ack. Used for the final COMPLETED update, before the event is marked as done. */
    public void publishAndWait(UUID jobId, JobStatus status, int attempt, String detail) {
        try {
            send(jobId, status, attempt, detail).get(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RetryableJobException("interrupted while publishing status", e);
        } catch (Exception e) {
            throw new RetryableJobException("could not publish status " + status, e);
        }
    }

    private CompletableFuture<?> send(UUID jobId, JobStatus status, int attempt, String detail) {
        try {
            JobStatusEvent event = new JobStatusEvent(UUID.randomUUID(), jobId, status, identity.id(), attempt,
                    detail, Instant.now());
            return kafka.send(Topics.STATUS, jobId.toString(), mapper.writeValueAsString(event));
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }
}
