package io.github.yassaminheidari.dispatch.api.job;

import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private final JobService service;
    private final JobRepository jobs;

    public JobController(JobService service, JobRepository jobs) {
        this.service = service;
        this.jobs = jobs;
    }

    @PostMapping
    public ResponseEntity<JobView> create(@Valid @RequestBody CreateJobRequest request) {
        JobView job = service.create(request);
        return ResponseEntity.created(URI.create("/api/jobs/" + job.id())).body(job);
    }

    @GetMapping("/{id}")
    public ResponseEntity<JobView> get(@PathVariable UUID id) {
        return jobs.findById(id).map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build());
    }

    @GetMapping
    public List<JobView> recent(@RequestParam(defaultValue = "50") int limit) {
        return jobs.recent(Math.max(1, Math.min(limit, 500)));
    }

    /** @param since ISO-8601 instant, for example 2026-01-01T00:00:00Z. Default: all jobs. */
    @GetMapping("/stats")
    public JobStats stats(@RequestParam(required = false) String since) {
        Instant from = since == null || since.isBlank() ? Instant.EPOCH : Instant.parse(since);
        return jobs.stats(from);
    }
}
