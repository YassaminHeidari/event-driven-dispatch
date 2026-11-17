package io.github.yassaminheidari.dispatch.worker;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

/**
 * Makes the worker an idempotent consumer, using Redis as the store of processed event ids.
 *
 * <p>Each event id moves through two states in Redis:
 * <ol>
 *   <li>{@code IN_PROGRESS:<token>} – set with SET NX and a short lease, so only one consumer runs the job.
 *       If the worker dies, the lease expires and a redelivered copy can run.</li>
 *   <li>{@code DONE} – set after the job finished and its status was published. Any later copy of the
 *       same message is acknowledged and skipped.</li>
 * </ol>
 */
@Component
public class IdempotencyGuard {

    public enum Outcome { ACQUIRED, ALREADY_DONE, IN_PROGRESS_ELSEWHERE }

    public record Claim(Outcome outcome, String token) {
    }

    static final String DONE = "DONE";

    /** Delete the key only if this consumer still holds the lease. */
    private static final DefaultRedisScript<Long> RELEASE = new DefaultRedisScript<>(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
            Long.class);

    private final StringRedisTemplate redis;
    private final WorkerIdentity identity;
    private final Duration lease;
    private final Duration doneTtl;

    public IdempotencyGuard(StringRedisTemplate redis, WorkerIdentity identity,
                            @Value("${dispatch.worker.idempotency.lease-seconds:30}") long leaseSeconds,
                            @Value("${dispatch.worker.idempotency.done-ttl-hours:168}") long doneTtlHours) {
        this.redis = redis;
        this.identity = identity;
        this.lease = Duration.ofSeconds(leaseSeconds);
        this.doneTtl = Duration.ofHours(doneTtlHours);
    }

    public Claim claim(UUID eventId) {
        String key = key(eventId);
        String token = "IN_PROGRESS:" + identity.id() + ":" + UUID.randomUUID();
        Boolean acquired = redis.opsForValue().setIfAbsent(key, token, lease);
        if (Boolean.TRUE.equals(acquired)) {
            return new Claim(Outcome.ACQUIRED, token);
        }
        String current = redis.opsForValue().get(key);
        if (DONE.equals(current)) {
            return new Claim(Outcome.ALREADY_DONE, null);
        }
        if (current == null) {
            // The lease expired between the two calls. Try once more.
            return claim(eventId);
        }
        return new Claim(Outcome.IN_PROGRESS_ELSEWHERE, null);
    }

    /**
     * Like {@link #claim}, but if another consumer holds the lease (possible for a short time during a
     * consumer-group rebalance), waits until that consumer finishes (DONE) or its lease expires.
     */
    public Claim claimWaiting(UUID eventId) {
        long deadline = System.nanoTime() + lease.plusSeconds(1).toNanos();
        Claim claim = claim(eventId);
        while (claim.outcome() == Outcome.IN_PROGRESS_ELSEWHERE && System.nanoTime() < deadline) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            claim = claim(eventId);
        }
        return claim;
    }

    public void markDone(UUID eventId) {
        redis.opsForValue().set(key(eventId), DONE, doneTtl);
    }

    public void release(UUID eventId, String token) {
        redis.execute(RELEASE, List.of(key(eventId)), token);
    }

    /** Counts delivery attempts per event in Redis, so the count survives a worker restart. */
    public int nextAttempt(UUID eventId) {
        String key = "attempts:" + eventId;
        Long n = redis.opsForValue().increment(key);
        redis.expire(key, Duration.ofDays(1));
        return n == null ? 1 : n.intValue();
    }

    static String key(UUID eventId) {
        return "processed:" + eventId;
    }
}
