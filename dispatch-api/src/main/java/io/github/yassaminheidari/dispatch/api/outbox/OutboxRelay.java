package io.github.yassaminheidari.dispatch.api.outbox;

import io.github.yassaminheidari.dispatch.api.job.JobRepository;
import io.github.yassaminheidari.dispatch.common.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Publishes outbox rows to Kafka.
 *
 * <p>Order of work in one transaction: lock rows, send to Kafka, wait for the broker ack, mark rows as
 * published, commit. If the send or the commit fails, the rows stay unpublished and the next run sends
 * them again. So delivery is at-least-once, and consumers must be idempotent.
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxRepository outbox;
    private final JobRepository jobs;
    private final KafkaTemplate<String, String> kafka;
    private final TransactionTemplate tx;
    private final int batchSize;
    private final Duration retention;

    public OutboxRelay(OutboxRepository outbox, JobRepository jobs, KafkaTemplate<String, String> kafka,
                       TransactionTemplate tx,
                       @Value("${dispatch.outbox.batch-size:500}") int batchSize,
                       @Value("${dispatch.outbox.retention-minutes:60}") long retentionMinutes) {
        this.outbox = outbox;
        this.jobs = jobs;
        this.kafka = kafka;
        this.tx = tx;
        this.batchSize = batchSize;
        this.retention = Duration.ofMinutes(retentionMinutes);
    }

    @Scheduled(fixedDelayString = "${dispatch.outbox.poll-interval-ms:20}")
    public void relay() {
        try {
            int sent;
            do {
                Integer n = tx.execute(status -> publishBatch());
                sent = n == null ? 0 : n;
            } while (sent == batchSize);
        } catch (Exception e) {
            log.warn("Outbox relay run failed; rows stay unpublished and will be retried: {}", e.toString());
        }
    }

    int publishBatch() {
        List<OutboxRepository.OutboxRow> rows = outbox.lockUnpublished(batchSize);
        if (rows.isEmpty()) {
            return 0;
        }
        List<CompletableFuture<SendResult<String, String>>> acks = new ArrayList<>(rows.size());
        for (OutboxRepository.OutboxRow row : rows) {
            acks.add(kafka.send(row.topic(), row.key(), row.payload()));
        }
        try {
            CompletableFuture.allOf(acks.toArray(CompletableFuture[]::new)).get(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for Kafka acks", e);
        } catch (Exception e) {
            // Throwing rolls back the transaction, so no row is marked as published.
            throw new IllegalStateException("Kafka did not ack the outbox batch", e);
        }
        List<UUID> ids = new ArrayList<>(rows.size());
        List<UUID> dispatchedJobs = new ArrayList<>(rows.size());
        for (OutboxRepository.OutboxRow row : rows) {
            ids.add(row.id());
            if (Topics.DISPATCH.equals(row.topic())) {
                dispatchedJobs.add(row.aggregateId());
            }
        }
        outbox.markPublished(ids);
        jobs.markDispatched(dispatchedJobs);
        return rows.size();
    }

    @Scheduled(fixedDelay = 60_000)
    public void cleanUp() {
        int deleted = outbox.deletePublishedBefore(Instant.now().minus(retention));
        if (deleted > 0) {
            log.debug("Deleted {} published outbox rows", deleted);
        }
    }
}
