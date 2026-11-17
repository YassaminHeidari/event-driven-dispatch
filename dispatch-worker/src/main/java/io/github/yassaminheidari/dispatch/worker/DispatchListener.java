package io.github.yassaminheidari.dispatch.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.yassaminheidari.dispatch.common.JobDispatchEvent;
import io.github.yassaminheidari.dispatch.common.JobStatus;
import io.github.yassaminheidari.dispatch.common.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class DispatchListener {

    private static final Logger log = LoggerFactory.getLogger(DispatchListener.class);

    private final ObjectMapper mapper;
    private final IdempotencyGuard guard;
    private final JobExecutor executor;
    private final StatusPublisher status;
    private final WorkerMetrics metrics;

    public DispatchListener(ObjectMapper mapper, IdempotencyGuard guard, JobExecutor executor,
                            StatusPublisher status, WorkerMetrics metrics) {
        this.mapper = mapper;
        this.guard = guard;
        this.executor = executor;
        this.status = status;
        this.metrics = metrics;
    }

    /**
     * Any exception thrown here goes to the DefaultErrorHandler (see KafkaErrorHandlingConfig):
     * RetryableJobException is retried with backoff; NonRetryableJobException goes straight to jobs.dispatch.DLT.
     */
    @KafkaListener(topics = Topics.DISPATCH, groupId = "dispatch-workers",
            concurrency = "${dispatch.worker.concurrency:6}")
    public void onDispatch(String message) {
        JobDispatchEvent event = parse(message);

        IdempotencyGuard.Claim claim = guard.claimWaiting(event.eventId());
        switch (claim.outcome()) {
            case ALREADY_DONE -> {
                metrics.duplicatesSkipped.incrementAndGet();
                log.info("Skipping duplicate event {} for job {}", event.eventId(), event.jobId());
                return;
            }
            case IN_PROGRESS_ELSEWHERE ->
                    throw new RetryableJobException("event " + event.eventId() + " is held by another consumer");
            case ACQUIRED -> {
                // continue
            }
        }

        int attempt = guard.nextAttempt(event.eventId());
        metrics.inFlight.incrementAndGet();
        try {
            status.publishAsync(event.jobId(), JobStatus.RUNNING, attempt, null);
            String result = executor.execute(event, attempt);
            status.publishAndWait(event.jobId(), JobStatus.COMPLETED, attempt, result);
            guard.markDone(event.eventId());
            metrics.processed.incrementAndGet();
        } catch (RuntimeException e) {
            metrics.failedAttempts.incrementAndGet();
            guard.release(event.eventId(), claim.token());
            if (e instanceof RetryableJobException) {
                status.publishAsync(event.jobId(), JobStatus.RETRYING, attempt, e.getMessage());
            }
            throw e;
        } finally {
            metrics.inFlight.decrementAndGet();
        }
    }

    private JobDispatchEvent parse(String message) {
        try {
            return mapper.readValue(message, JobDispatchEvent.class);
        } catch (Exception e) {
            throw new NonRetryableJobException("cannot parse dispatch event", e);
        }
    }
}
