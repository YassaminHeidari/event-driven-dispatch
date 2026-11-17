package io.github.yassaminheidari.dispatch.worker;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class WorkerIdentity {

    private final String id;

    public WorkerIdentity(@Value("${dispatch.worker.id:}") String configured) {
        String host = System.getenv("HOSTNAME");
        if (configured != null && !configured.isBlank()) {
            this.id = configured;
        } else if (host != null && !host.isBlank()) {
            this.id = "worker-" + host;
        } else {
            this.id = "worker-" + UUID.randomUUID().toString().substring(0, 8);
        }
    }

    public String id() {
        return id;
    }
}
