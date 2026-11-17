package io.github.yassaminheidari.dispatch.worker;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

@Component
public class WorkerMetrics {

    public final AtomicLong processed = new AtomicLong();
    public final AtomicLong duplicatesSkipped = new AtomicLong();
    public final AtomicLong failedAttempts = new AtomicLong();
    public final AtomicLong inFlight = new AtomicLong();
}
