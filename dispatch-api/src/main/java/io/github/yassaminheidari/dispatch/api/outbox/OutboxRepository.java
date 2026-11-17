package io.github.yassaminheidari.dispatch.api.outbox;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Repository
public class OutboxRepository {

    public record OutboxRow(UUID id, UUID aggregateId, String topic, String key, String payload) {
    }

    private final NamedParameterJdbcTemplate jdbc;

    public OutboxRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Must run inside the same transaction as the business write. */
    public void append(UUID aggregateId, String topic, String key, String payload, Instant now) {
        jdbc.update("""
                insert into outbox_events (id, aggregate_id, topic, msg_key, payload, created_at)
                values (:id, :aggregateId, :topic, :key, :payload, :now)
                """, new MapSqlParameterSource()
                .addValue("id", UUID.randomUUID())
                .addValue("aggregateId", aggregateId)
                .addValue("topic", topic)
                .addValue("key", key)
                .addValue("payload", payload)
                .addValue("now", Timestamp.from(now)));
    }

    /**
     * Locks the oldest unpublished rows. SKIP LOCKED lets several API instances run the relay
     * at the same time without sending the same row twice.
     */
    public List<OutboxRow> lockUnpublished(int limit) {
        return jdbc.query("""
                select id, aggregate_id, topic, msg_key, payload from outbox_events
                where published_at is null
                order by created_at
                limit :limit
                for update skip locked
                """, Map.of("limit", limit), (rs, i) -> new OutboxRow(
                rs.getObject("id", UUID.class),
                rs.getObject("aggregate_id", UUID.class),
                rs.getString("topic"),
                rs.getString("msg_key"),
                rs.getString("payload")));
    }

    public void markPublished(Collection<UUID> ids) {
        if (!ids.isEmpty()) {
            jdbc.update("update outbox_events set published_at = now() where id in (:ids)", Map.of("ids", ids));
        }
    }

    public int deletePublishedBefore(Instant cutoff) {
        return jdbc.update("delete from outbox_events where published_at is not null and published_at < :cutoff",
                Map.of("cutoff", Timestamp.from(cutoff)));
    }

    public long countUnpublished() {
        Long n = jdbc.queryForObject("select count(*) from outbox_events where published_at is null", Map.of(),
                Long.class);
        return n == null ? 0 : n;
    }
}
