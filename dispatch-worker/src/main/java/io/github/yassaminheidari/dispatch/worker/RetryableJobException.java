package io.github.yassaminheidari.dispatch.worker;

/** A transient failure. The error handler retries the record with exponential backoff. */
public class RetryableJobException extends RuntimeException {

    public RetryableJobException(String message) {
        super(message);
    }

    public RetryableJobException(String message, Throwable cause) {
        super(message, cause);
    }
}
