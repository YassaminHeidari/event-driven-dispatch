package io.github.yassaminheidari.dispatch.api.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.yassaminheidari.dispatch.api.outbox.OutboxRepository;
import io.github.yassaminheidari.dispatch.common.JobDispatchEvent;
import io.github.yassaminheidari.dispatch.common.Topics;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

@Service
public class JobService {

    private final JobRepository jobs;
    private final OutboxRepository outbox;
    private final ObjectMapper mapper;

    public JobService(JobRepository jobs, OutboxRepository outbox, ObjectMapper mapper) {
        this.jobs = jobs;
        this.outbox = outbox;
        this.mapper = mapper;
    }

    /**
     * Transactional outbox: the job row and the outbox row commit together or not at all.
     * The service never calls Kafka here, so a Kafka outage cannot leave a job without its message,
     * and a database rollback cannot leave a message without its job.
     */
    @Transactional
    public JobView create(CreateJobRequest request) {
        UUID jobId = UUID.randomUUID();
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        String payload = request.payload() == null || request.payload().isNull() ? "{}" : request.payload().toString();

        jobs.insert(jobId, request.type(), payload, now);

        JobDispatchEvent event = new JobDispatchEvent(UUID.randomUUID(), jobId, request.type(), payload, now);
        outbox.append(jobId, Topics.DISPATCH, jobId.toString(), toJson(event), now);

        return jobs.findById(jobId).orElseThrow();
    }

    private String toJson(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
