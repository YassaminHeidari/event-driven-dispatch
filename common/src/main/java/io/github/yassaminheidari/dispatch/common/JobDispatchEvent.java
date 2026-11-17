package io.github.yassaminheidari.dispatch.common;

import java.time.Instant;
import java.util.UUID;

/**
 * A job that is ready for a worker.
 *
 * @param eventId   unique id of this message; consumers use it to drop duplicates
 * @param jobId     the job this message is about (also the Kafka key)
 * @param type      job type: echo, flaky or fail
 * @param payload   job input as a JSON string
 * @param createdAt time the job was accepted by the API
 */
public record JobDispatchEvent(UUID eventId, UUID jobId, String type, String payload, Instant createdAt) {
}
