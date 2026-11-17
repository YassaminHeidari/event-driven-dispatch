package io.github.yassaminheidari.dispatch.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;

/** Writes this worker's live state to Redis every 2 seconds. The key expires if the worker stops. */
@Component
public class Heartbeat {

    private static final Logger log = LoggerFactory.getLogger(Heartbeat.class);

    private final StringRedisTemplate redis;
    private final WorkerIdentity identity;
    private final WorkerMetrics metrics;
    private final int concurrency;

    public Heartbeat(StringRedisTemplate redis, WorkerIdentity identity, WorkerMetrics metrics,
                     @Value("${dispatch.worker.concurrency:6}") int concurrency) {
        this.redis = redis;
        this.identity = identity;
        this.metrics = metrics;
        this.concurrency = concurrency;
    }

    @Scheduled(fixedRate = 2_000)
    public void beat() {
        try {
            String key = "worker:" + identity.id();
            redis.opsForHash().putAll(key, Map.of(
                    "lastSeen", String.valueOf(System.currentTimeMillis()),
                    "processed", String.valueOf(metrics.processed.get()),
                    "duplicatesSkipped", String.valueOf(metrics.duplicatesSkipped.get()),
                    "failedAttempts", String.valueOf(metrics.failedAttempts.get()),
                    "inFlight", String.valueOf(metrics.inFlight.get()),
                    "concurrency", String.valueOf(concurrency)));
            redis.expire(key, Duration.ofSeconds(10));
            redis.opsForSet().add("workers", identity.id());
        } catch (Exception e) {
            log.warn("Heartbeat failed: {}", e.toString());
        }
    }
}
