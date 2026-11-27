# Changelog & Releases

All notable changes to **Data Contracts & Semantic Layer Analytics Platform (`data-contact`)** are documented in this file.

## [v1.0.0] - 2026-11-26

### Added
- **Shift-Left Data Contracts (`contracts/`, `ingestion/`):** ODCS-inspired YAML contracts (`raw_customers`, `raw_products`, `raw_orders`, `raw_payments`), pure-Python validator (`DataContractValidator`), quarantine dead-letter persistence, structured alert dispatcher, deterministic Jan–Jun 2026 dataset generator, and PySpark mirror (`ingestion/spark_ingest_mirror.py`).
- **dbt Project & Local Warehouse (`dbt/`, `pipeline/`):** Complete dbt Core project (`view` staging, `ephemeral` intermediate, `table` dimensions, `incremental` OBT mart & facts, SCD Type 2 snapshots, macros, singular tests) plus executable SQLite `WarehouseRunner` with 20 automated quality checks.
- **Centralized Semantic Layer & Observability (`semantic_layer/`, `observability/`):** Governed metrics catalog (`metrics.yml`), allowlisted query engine with caching, FastAPI + stdlib HTTP API (`/health`, `/metrics`, `/api/v1/semantic/metrics`, `/api/v1/semantic/query`), `SpanTracer`, JSON correlation-id logger, Prometheus exposition, and Grafana dashboard.
- **Cloud Mirror, IaC & Load Tests (`infra/`, `tests/load/`):** Terraform AWS blueprint (`S3`, `Glue`, `Redshift Serverless`, `Athena`, `SNS`), Databricks Lakeflow workflow JSON, Docker Compose stack, GitHub Actions CI, and k6 load test script (`p(95) < 250ms`).
- **Documentation & Showcase (`docs/`):** `ARCHITECTURE_TRADEOFFS.md`, `API.md`, `DEMO.md`, `index.html`, SVG charts, ADRs `001..003`, `LICENSE`, `SECURITY.md`, and `CONTRIBUTING.md`.

### Verification
- `python3 -m unittest discover -s tests -p "test_*.py" -v`: 27 / 27 tests passing (20 / 20 warehouse quality checks passing).
