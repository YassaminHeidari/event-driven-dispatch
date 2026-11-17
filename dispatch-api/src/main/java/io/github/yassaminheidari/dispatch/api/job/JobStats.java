package io.github.yassaminheidari.dispatch.api.job;

import java.time.Instant;
import java.util.Map;

/** Pipeline numbers used by the dashboard and the benchmark script. Latencies are in milliseconds. */
public record JobStats(Instant since, long total, long inFlight, long completed, long failed,
                       Map<String, Long> byStatus, double endToEndP50Ms, double endToEndP95Ms,
                       double endToEndP99Ms, double completedPerSecond) {
}
