package io.github.yassaminheidari.dispatch.api.job;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

/**
 * @param type    echo (succeeds), flaky (fails N times, then succeeds) or fail (always fails, goes to the DLT)
 * @param payload free JSON, for example {"durationMs": 20} or {"failTimes": 2}
 */
public record CreateJobRequest(
        @NotBlank @Pattern(regexp = "[a-z][a-z0-9-]{0,63}") String type,
        JsonNode payload) {
}
