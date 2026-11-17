package io.github.yassaminheidari.dispatch.api.job;

import io.github.yassaminheidari.dispatch.common.JobStatus;
import io.github.yassaminheidari.dispatch.common.JobStatusEvent;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Repository
public class JobRepository {

    private static final RowMapper<JobView> MAPPER = JobRepository::map;

    private final NamedParameterJdbcTemplate jdbc;

    public JobRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void insert(UUID id, String type, String payload, Instant now) {
        jdbc.update("""
                insert into jobs (id, type, payload, status, created_at, updated_at)
                values (:id, :type, :payload, 'PENDING', :now, :now)
                """, new MapSqlParameterSource()
                .addValue("id", id)
                .addValue("type", type)
                .addValue("payload", payload)
                .addValue("now", Timestamp.from(now)));
    }

    public Optional<JobView> findById(UUID id) {
        return jdbc.query("select * from jobs where id = :id", Map.of("id", id), MAPPER).stream().findFirst();
    }

    /** Moves jobs from PENDING to DISPATCHED. A job that a worker already moved further is left alone. */
    public void markDispatched(Collection<UUID> ids) {
        if (ids.isEmpty()) {
            return;
        }
        jdbc.update("""
                update jobs set status = 'DISPATCHED', dispatched_at = now(), updated_at = now()
                where id in (:ids) and status = 'PENDING'
                """, Map.of("ids", ids));
    }

    /**
     * Applies a worker status event. Terminal jobs (COMPLETED, FAILED) never change again,
     * so a late or reordered event cannot move a finished job backwards.
     *
     * @return true if a row changed
     */
    public boolean applyStatus(JobStatusEvent e) {
        Timestamp at = Timestamp.from(e.occurredAt() != null ? e.occurredAt() : Instant.now());
        int rows = jdbc.update("""
                update jobs set
                    status = :status,
                    worker_id = coalesce(:workerId, worker_id),
                    attempts = greatest(attempts, :attempt),
                    detail = :detail,
                    status_events_applied = status_events_applied + 1,
                    started_at = case when :running and started_at is null then :at else started_at end,
                    completed_at = case when :terminal then :at else completed_at end,
                    updated_at = now()
                where id = :id and status not in ('COMPLETED', 'FAILED')
                """, new MapSqlParameterSource()
                .addValue("status", e.status().name())
                .addValue("workerId", e.workerId())
                .addValue("attempt", e.attempt())
                .addValue("detail", e.detail())
                .addValue("running", e.status() == JobStatus.RUNNING)
                .addValue("terminal", e.status().isTerminal())
                .addValue("at", at)
                .addValue("id", e.jobId()));
        return rows > 0;
    }

    public boolean markFailed(UUID id, String detail) {
        int rows = jdbc.update("""
                update jobs set status = 'FAILED', detail = :detail, completed_at = now(), updated_at = now()
                where id = :id and status not in ('COMPLETED', 'FAILED')
                """, new MapSqlParameterSource().addValue("id", id).addValue("detail", detail));
        return rows > 0;
    }

    public JobStats stats(Instant since) {
        Map<String, Object> params = Map.of("since", Timestamp.from(since));
        Map<String, Long> byStatus = new LinkedHashMap<>();
        for (JobStatus s : JobStatus.values()) {
            byStatus.put(s.name(), 0L);
        }
        jdbc.query("select status, count(*) as n from jobs where created_at >= :since group by status", params,
                rs -> {
                    byStatus.put(rs.getString("status"), rs.getLong("n"));
                });
        long total = byStatus.values().stream().mapToLong(Long::longValue).sum();
        long inFlight = byStatus.get("PENDING") + byStatus.get("DISPATCHED") + byStatus.get("RUNNING")
                + byStatus.get("RETRYING");

        // End-to-end latency: from the moment the API accepted the job to the moment a worker finished it.
        Map<String, Object> row = jdbc.queryForMap("""
                select
                  count(*) as completed,
                  percentile_cont(0.50) within group (order by extract(epoch from (completed_at - created_at)) * 1000) as p50,
                  percentile_cont(0.95) within group (order by extract(epoch from (completed_at - created_at)) * 1000) as p95,
                  percentile_cont(0.99) within group (order by extract(epoch from (completed_at - created_at)) * 1000) as p99,
                  extract(epoch from (max(completed_at) - min(created_at))) as window_seconds
                from jobs
                where created_at >= :since and status = 'COMPLETED'
                """, params);
        long completed = ((Number) row.get("completed")).longValue();
        double windowSeconds = toDouble(row.get("window_seconds"));
        double throughput = windowSeconds > 0 ? completed / windowSeconds : 0;
        return new JobStats(since, total, inFlight, completed, byStatus.get("FAILED"), byStatus,
                toDouble(row.get("p50")), toDouble(row.get("p95")), toDouble(row.get("p99")), throughput);
    }

    public List<JobView> recent(int limit) {
        return jdbc.query("select * from jobs order by created_at desc limit :limit", Map.of("limit", limit), MAPPER);
    }

    private static double toDouble(Object o) {
        return o == null ? 0 : ((Number) o).doubleValue();
    }

    private static JobView map(ResultSet rs, int i) throws SQLException {
        return new JobView(
                rs.getObject("id", UUID.class),
                rs.getString("type"),
                rs.getString("payload"),
                JobStatus.valueOf(rs.getString("status")),
                rs.getString("worker_id"),
                rs.getInt("attempts"),
                rs.getString("detail"),
                rs.getInt("status_events_applied"),
                instant(rs, "created_at"),
                instant(rs, "dispatched_at"),
                instant(rs, "started_at"),
                instant(rs, "completed_at"));
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        Timestamp t = rs.getTimestamp(column);
        return t == null ? null : t.toInstant();
    }
}
