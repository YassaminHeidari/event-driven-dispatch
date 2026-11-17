package io.github.yassaminheidari.dispatch.worker;

/** A permanent failure (bad input, unknown job type). The record goes straight to the dead-letter topic. */
public class NonRetryableJobException extends RuntimeException {

    public NonRetryableJobException(String message) {
        super(message);
    }

    public NonRetryableJobException(String message, Throwable cause) {
        super(message, cause);
    }
}
