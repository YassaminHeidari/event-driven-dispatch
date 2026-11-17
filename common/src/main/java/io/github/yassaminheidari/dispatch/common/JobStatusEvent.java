package io.github.yassaminheidari.dispatch.common;

import java.time.Instant;
import java.util.UUID;

/**
 * A status change reported by a worker.
 *
 * @param eventId    unique id of this message; the API stores it to drop duplicates
 * @param jobId      the job (also the Kafka key)
 * @param status     the new status
 * @param workerId   the worker that sent the update
 * @param attempt    delivery attempt number, starting at 1
 * @param detail     result or error text, may be null
 * @param occurredAt time of the change on the worker
 */
public record JobStatusEvent(UUID eventId, UUID jobId, JobStatus status, String workerId, int attempt,
                             String detail, Instant occurredAt) {
}
