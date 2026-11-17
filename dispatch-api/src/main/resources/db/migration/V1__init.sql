-- Jobs: the source of truth for job state.
create table jobs (
    id                    uuid primary key,
    type                  varchar(64)  not null,
    payload               text         not null,
    status                varchar(32)  not null,
    worker_id             varchar(128),
    attempts              int          not null default 0,
    detail                text,
    status_events_applied int          not null default 0,
    created_at            timestamptz  not null,
    dispatched_at         timestamptz,
    started_at            timestamptz,
    completed_at          timestamptz,
    updated_at            timestamptz  not null
);
create index jobs_status_idx on jobs (status);
create index jobs_created_at_idx on jobs (created_at);

-- Transactional outbox: written in the same transaction as the job row,
-- then published to Kafka by OutboxRelay.
create table outbox_events (
    id           uuid primary key,
    aggregate_id uuid         not null,
    topic        varchar(128) not null,
    msg_key      varchar(128) not null,
    payload      text         not null,
    created_at   timestamptz  not null,
    published_at timestamptz
);
create index outbox_unpublished_idx on outbox_events (created_at) where published_at is null;

-- Idempotent consumer: one row per status event that was already applied.
create table processed_events (
    event_id     uuid primary key,
    processed_at timestamptz not null
);
