package io.github.yassaminheidari.dispatch.api.live;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.yassaminheidari.dispatch.api.job.JobView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * Sends a job update to every API instance through Redis pub/sub. Each instance then pushes the update
 * to its own WebSocket clients. Only one API instance consumes a given Kafka status event (consumer group),
 * so without this fan-out, clients on the other instances would miss the update.
 */
@Component
public class JobUpdatePublisher {

    public static final String CHANNEL = "job-updates";

    private static final Logger log = LoggerFactory.getLogger(JobUpdatePublisher.class);

    private final StringRedisTemplate redis;
    private final ObjectMapper mapper;

    public JobUpdatePublisher(StringRedisTemplate redis, ObjectMapper mapper) {
        this.redis = redis;
        this.mapper = mapper;
    }

    public void publish(JobView job) {
        try {
            redis.convertAndSend(CHANNEL, mapper.writeValueAsString(job));
        } catch (Exception e) {
            // Live updates are best effort. The database stays correct, and clients can re-read the job.
            log.warn("Could not publish live update for job {}: {}", job.id(), e.toString());
        }
    }
}
