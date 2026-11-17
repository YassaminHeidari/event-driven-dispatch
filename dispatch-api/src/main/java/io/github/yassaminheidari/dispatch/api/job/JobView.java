package io.github.yassaminheidari.dispatch.api.job;

import io.github.yassaminheidari.dispatch.common.JobStatus;

import java.time.Instant;
import java.util.UUID;

public record JobView(UUID id, String type, String payload, JobStatus status, String workerId, int attempts,
                      String detail, int statusEventsApplied, Instant createdAt, Instant dispatchedAt,
                      Instant startedAt, Instant completedAt) {
}
