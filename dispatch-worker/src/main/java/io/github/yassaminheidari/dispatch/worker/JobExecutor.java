package io.github.yassaminheidari.dispatch.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.yassaminheidari.dispatch.common.JobDispatchEvent;
import org.springframework.stereotype.Component;

/**
 * Runs a job. The job types simulate real work and real failures:
 * <ul>
 *   <li>echo  – sleeps {@code durationMs} (default 10) and succeeds</li>
 *   <li>flaky – fails with a retryable error on the first {@code failTimes} attempts (default 2), then succeeds</li>
 *   <li>fail  – fails with a non-retryable error, so the record goes straight to the DLT</li>
 * </ul>
 */
@Component
public class JobExecutor {

    private static final long MAX_DURATION_MS = 10_000;

    private final ObjectMapper mapper;

    public JobExecutor(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public String execute(JobDispatchEvent job, int attempt) {
        JsonNode payload = parse(job.payload());
        return switch (job.type()) {
            case "echo" -> {
                long duration = Math.min(Math.max(payload.path("durationMs").asLong(10), 0), MAX_DURATION_MS);
                sleep(duration);
                yield "echo ok after " + duration + " ms";
            }
            case "flaky" -> {
                int failTimes = payload.path("failTimes").asInt(2);
                if (attempt <= failTimes) {
                    throw new RetryableJobException("simulated transient failure on attempt " + attempt);
                }
                yield "flaky ok on attempt " + attempt;
            }
            case "fail" -> throw new NonRetryableJobException("job type 'fail' always fails");
            default -> throw new NonRetryableJobException("unknown job type '" + job.type() + "'");
        };
    }

    private JsonNode parse(String payload) {
        try {
            return mapper.readTree(payload == null ? "{}" : payload);
        } catch (Exception e) {
            throw new NonRetryableJobException("payload is not valid JSON", e);
        }
    }

    private static void sleep(long ms) {
        if (ms <= 0) {
            return;
        }
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RetryableJobException("interrupted", e);
        }
    }
}
