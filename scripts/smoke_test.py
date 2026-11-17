#!/usr/bin/env python3
"""End-to-end smoke test for the docker compose stack.

Submits one job of each type and checks the final state:
  echo  -> COMPLETED on attempt 1
  flaky -> COMPLETED on attempt 3 (two retries)
  fail  -> FAILED via the dead-letter topic
"""
import json
import sys
import time
import urllib.request

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8080"


def call(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read() or b"null")


def fail(message):
    # The ::error:: line shows on the GitHub Actions run page.
    print(f"::error title=Smoke test::{message}")
    raise SystemExit(message)


def wait_for(check, timeout, what):
    end = time.time() + timeout
    last = None
    while time.time() < end:
        try:
            last = check()
            if last:
                return last
        except Exception as e:  # service still starting
            last = e
        time.sleep(2)
    fail(f"Timed out waiting for {what}. Last value: {last}")


wait_for(lambda: call("GET", "/actuator/health")["status"] == "UP", 240, "API health")
wait_for(lambda: len(call("GET", "/api/workers")) >= 2, 180, "two workers in Redis")
print("API is up, workers:", [w["id"] for w in call("GET", "/api/workers")])

expected = {
    "echo": ("COMPLETED", 1),
    "flaky": ("COMPLETED", 3),
    "fail": ("FAILED", None),
}
payloads = {"echo": {"durationMs": 10}, "flaky": {"failTimes": 2}, "fail": {}}
ids = {t: call("POST", "/api/jobs", {"type": t, "payload": payloads[t]})["id"] for t in expected}

for job_type, job_id in ids.items():
    status, attempts = expected[job_type]
    job = wait_for(lambda: (j := call("GET", f"/api/jobs/{job_id}"))["status"] in ("COMPLETED", "FAILED") and j,
                   90, f"{job_type} job to finish")
    print(f"{job_type:6} -> {job['status']} attempts={job['attempts']} detail={job['detail']}")
    if job["status"] != status:
        fail(f"{job_type}: expected {status}, got {job['status']} ({job['detail']})")
    if attempts is not None and job["attempts"] != attempts:
        fail(f"{job_type}: expected {attempts} attempts, got {job['attempts']}")

print("Smoke test passed")
