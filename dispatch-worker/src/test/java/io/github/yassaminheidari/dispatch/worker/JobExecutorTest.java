package io.github.yassaminheidari.dispatch.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.yassaminheidari.dispatch.common.JobDispatchEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JobExecutorTest {

    private final JobExecutor executor = new JobExecutor(new ObjectMapper());

    private static JobDispatchEvent job(String type, String payload) {
        return new JobDispatchEvent(UUID.randomUUID(), UUID.randomUUID(), type, payload, Instant.now());
    }

    @Test
    void echoSucceeds() {
        assertThat(executor.execute(job("echo", "{\"durationMs\":0}"), 1)).contains("echo ok");
    }

    @Test
    void flakyFailsWithARetryableErrorUntilFailTimesIsReached() {
        JobDispatchEvent flaky = job("flaky", "{\"failTimes\":2}");
        assertThatThrownBy(() -> executor.execute(flaky, 1)).isInstanceOf(RetryableJobException.class);
        assertThatThrownBy(() -> executor.execute(flaky, 2)).isInstanceOf(RetryableJobException.class);
        assertThat(executor.execute(flaky, 3)).contains("attempt 3");
    }

    @Test
    void failAndUnknownTypesAreNotRetryable() {
        assertThatThrownBy(() -> executor.execute(job("fail", "{}"), 1)).isInstanceOf(NonRetryableJobException.class);
        assertThatThrownBy(() -> executor.execute(job("nope", "{}"), 1)).isInstanceOf(NonRetryableJobException.class);
    }

    @Test
    void invalidPayloadIsNotRetryable() {
        assertThatThrownBy(() -> executor.execute(job("echo", "{not json"), 1))
                .isInstanceOf(NonRetryableJobException.class);
    }
}
