package io.github.yassaminheidari.dispatch.common;

/** Kafka topic names shared by every service. */
public final class Topics {

    /** Jobs that are ready for a worker. Key = job id. Written only by the outbox relay. */
    public static final String DISPATCH = "jobs.dispatch";

    /** Dead-letter topic for jobs that failed every retry or failed with a non-retryable error. */
    public static final String DISPATCH_DLT = "jobs.dispatch.DLT";

    /** Status changes reported by workers. Key = job id, so all updates for one job stay in order. */
    public static final String STATUS = "jobs.status";

    /** The DLT must have at least as many partitions as the source topic (records keep their partition). */
    public static final int PARTITIONS = 12;

    private Topics() {
    }
}
