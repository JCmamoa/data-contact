import http from 'k6/http';
import { check, group, sleep } from 'k6';
import { Rate, Trend } from 'k6/metrics';

// ==============================================================================
// k6 Load & Resilience Test — Centralized Semantic Layer API
// Usage:
//   k6 run tests/load/k6_semantic_api.js
//   BASE_URL=http://localhost:8000 k6 run tests/load/k6_semantic_api.js
// ==============================================================================

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';

// Custom per-endpoint telemetry
const healthLatency = new Trend('semantic_api_health_duration', true);
const catalogLatency = new Trend('semantic_api_metrics_catalog_duration', true);
const queryLatency = new Trend('semantic_api_query_duration', true);
const semanticValidationSuccess = new Rate('semantic_api_validation_success');

export const options = {
  stages: [
    { duration: '15s', target: 10 }, // Warm-up
    { duration: '30s', target: 25 }, // Baseline steady traffic (BI dashboards)
    { duration: '30s', target: 50 }, // Peak burst (AI agents + executive refresh)
    { duration: '30s', target: 50 }, // Sustained peak load
    { duration: '15s', target: 0 },  // Graceful cool-down
  ],
  thresholds: {
    http_req_failed: ['rate<0.01'],           // <1% HTTP errors
    http_req_duration: ['p(95)<250', 'p(99)<500'], // Strict p95 < 250ms SLA
    semantic_api_validation_success: ['rate>0.99'],
    semantic_api_query_duration: ['p(95)<250'],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

const SEMANTIC_QUERIES = [
  { metric: 'net_revenue', dimensions: ['plan_tier'] },
  { metric: 'net_revenue', dimensions: ['country', 'plan_tier'] },
  { metric: 'gross_revenue', dimensions: ['order_month'] },
  { metric: 'average_order_value', dimensions: ['payment_method'] },
  { metric: 'take_rate_pct', dimensions: ['category'] },
  { metric: 'retention_rate', dimensions: ['plan_tier'] },
  { metric: 'churn_rate', dimensions: ['country'] },
];

export default function () {
  const headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'X-Client-App': 'k6-load-test',
  };

  group('1. Health Probe', () => {
    const res = http.get(`${BASE_URL}/health`, {
      tags: { endpoint: 'health' },
    });
    healthLatency.add(res.timings.duration);

    const ok = check(res, {
      'health status is 200': (r) => r.status === 200,
      'health payload reports healthy': (r) => {
        try {
          const body = r.json();
          return body.status === 'ok' || body.status === 'healthy';
        } catch (_) {
          return false;
        }
      },
    });
    semanticValidationSuccess.add(ok);
  });

  group('2. Semantic Metrics Catalog', () => {
    const res = http.get(`${BASE_URL}/api/v1/semantic/metrics`, {
      headers,
      tags: { endpoint: 'semantic_metrics_catalog' },
    });
    catalogLatency.add(res.timings.duration);

    const ok = check(res, {
      'metrics catalog status is 200': (r) => r.status === 200,
      'metrics catalog returns non-empty list': (r) => {
        try {
          const body = r.json();
          const metrics = Array.isArray(body) ? body : body.metrics;
          return Array.isArray(metrics) && metrics.length > 0;
        } catch (_) {
          return false;
        }
      },
    });
    semanticValidationSuccess.add(ok);
  });

  group('3. Semantic Metric Query Execution', () => {
    const payload = SEMANTIC_QUERIES[Math.floor(Math.random() * SEMANTIC_QUERIES.length)];

    const res = http.post(
      `${BASE_URL}/api/v1/semantic/query`,
      JSON.stringify(payload),
      {
        headers,
        tags: { endpoint: 'semantic_query', metric: payload.metric },
      }
    );
    queryLatency.add(res.timings.duration);

    const ok = check(res, {
      'semantic query status is 200': (r) => r.status === 200,
      'semantic query returns governed SQL & rows': (r) => {
        try {
          const body = r.json();
          return (
            typeof body.metric_name === 'string' &&
            typeof body.sql === 'string' &&
            Array.isArray(body.rows)
          );
        } catch (_) {
          return false;
        }
      },
    });
    semanticValidationSuccess.add(ok);
  });

  // Realistic think-time jitter between BI/agent requests (100ms - 400ms)
  sleep(0.1 + Math.random() * 0.3);
}
