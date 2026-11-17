package io.github.yassaminheidari.dispatch.api.status;

import io.github.yassaminheidari.dispatch.api.job.JobRepository;
import io.github.yassaminheidari.dispatch.api.job.JobView;
import io.github.yassaminheidari.dispatch.common.JobStatusEvent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Service
public class StatusService {

    private final JdbcTemplate jdbc;
    private final JobRepository jobs;

    public StatusService(JdbcTemplate jdbc, JobRepository jobs) {
        this.jdbc = jdbc;
        this.jobs = jobs;
    }

    /**
     * Idempotent consumer. The event id goes into processed_events in the same transaction as the job update.
     * If the id is already there, the event is a duplicate and nothing changes.
     *
     * @return the updated job, or empty if the event was a duplicate or did not change the job
     */
    @Transactional
    public Optional<JobView> apply(JobStatusEvent event) {
        int inserted = jdbc.update(
                "insert into processed_events (event_id, processed_at) values (?, now()) on conflict do nothing",
                event.eventId());
        if (inserted == 0) {
            return Optional.empty();
        }
        if (!jobs.applyStatus(event)) {
            return Optional.empty();
        }
        return jobs.findById(event.jobId());
    }

    @Transactional
    public Optional<JobView> fail(java.util.UUID jobId, String detail) {
        if (!jobs.markFailed(jobId, detail)) {
            return Optional.empty();
        }
        return jobs.findById(jobId);
    }
}
