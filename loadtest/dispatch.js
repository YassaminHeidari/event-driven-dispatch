// k6 load test: submits jobs at a constant arrival rate.
//
//   k6 run -e RATE=300 -e DURATION=60s loadtest/dispatch.js
//
// k6 measures the HTTP side (throughput, p99 of POST /api/jobs).
// scripts/benchmark.sh then reads /api/jobs/stats for the end-to-end side
// (API accepted -> outbox -> Kafka -> worker -> status -> PostgreSQL).
import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const RATE = parseInt(__ENV.RATE || '300', 10);
const DURATION = __ENV.DURATION || '60s';
const JOB_MS = parseInt(__ENV.JOB_MS || '5', 10);

export const options = {
  discardResponseBodies: true,
  scenarios: {
    submit_jobs: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: 50,
      maxVUs: 500,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(99)<500'],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

const params = { headers: { 'Content-Type': 'application/json' } };

export default function () {
  const body = JSON.stringify({ type: 'echo', payload: { durationMs: JOB_MS } });
  const res = http.post(`${BASE_URL}/api/jobs`, body, params);
  check(res, { 'status is 201': (r) => r.status === 201 });
}
