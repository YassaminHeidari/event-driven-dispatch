package io.github.yassaminheidari.dispatch.common;

public enum JobStatus {
    PENDING,
    DISPATCHED,
    RUNNING,
    RETRYING,
    COMPLETED,
    FAILED;

    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED;
    }
}
