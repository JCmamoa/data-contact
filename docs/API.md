# Semantic Layer & Pipeline API Reference

Base URL: `http://localhost:8000` (Swagger UI: `/docs` when running with FastAPI)

---

## 1. `GET /health`

Returns service health and number of registered metrics in `semantic_layer/metrics.yml`.

```json
{
  "status": "ok",
  "service": "semantic-layer",
  "metrics_count": 6
}
```

---

## 2. `GET /metrics`

Exposes Prometheus text metrics (`text/plain; version=0.0.4; charset=utf-8`):
- `pipeline_runs_total`
- `contract_violations_total`
- `semantic_queries_total`
- `semantic_query_duration_seconds`

---

## 3. `GET /api/v1/semantic/metrics`

Lists all centralized metrics, bilingual labels (`label_pt`, `label_en`), units, backing models, and allowed dimensions.

---

## 4. `POST /api/v1/semantic/query`

Executes a governed metric query against `mart_revenue_analytics` or `dim_customers`.

```bash
curl -s -X POST http://localhost:8000/api/v1/semantic/query \
  -H "Content-Type: application/json" \
  -d '{
    "metric": "net_revenue",
    "dimensions": ["country"],
    "filters": {"plan_tier": "Enterprise"}
  }' | jq .
```

### Error Responses
- `400 Bad Request`: unknown metric, disallowed dimension/filter, or malformed JSON.
- `500 Internal Server Error`: warehouse query failure.
