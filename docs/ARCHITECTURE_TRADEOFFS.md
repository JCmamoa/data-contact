# Architecture Trade-offs & Design Deep Dive

This document captures the Principal-level engineering trade-offs behind the **Data Contracts & Semantic Platform**. Every architectural choice optimizes for three invariants:

1. **Zero Silent Data Corruption** — malformed or contract-violating records never reach analytical marts.
2. **Predictable Cost-to-Latency Ratio** — warehouse materializations match access patterns and data growth curves.
3. **Single Source of Metric Truth** — BI dashboards, Python notebooks, and AI/LLM agents evaluate identical SQL definitions.

---

## 1. Warehouse Materialization Trade-offs (`ephemeral` vs `view` vs `table` vs `incremental`)

Choosing the wrong dbt/SQL materialization is the #1 cause of runaway cloud warehouse spend and missed dashboard SLAs. Our platform deliberately applies **different materializations per Medallion / dimensional layer**:

### 1.1 Comparative Matrix

| Dimension | `ephemeral` (CTE Inlining) | `view` | `table` | `incremental` (`merge` / `delete+insert`) |
| :--- | :--- | :--- | :--- | :--- |
| **Physical Storage** | None (0 bytes) | Metadata only (0 bytes) | Full snapshot rebuilt every run | Partitioned/clustered delta appended or merged |
| **Build Compute Cost** | Zero standalone build | Near-zero (`CREATE VIEW`) | $O(N)$ full scan + sort/write every run | $O(\Delta N)$ proportional to new/changed rows |
| **Query Read Latency** | Paid by downstream caller | Paid at query time (re-evaluates joins/casts) | Sub-second (`SELECT` from precomputed storage) | Sub-second (with partition pruning / Z-order) |
| **Lineage & Debuggability** | Harder to inspect directly in SQL IDE | Easy (`SELECT * FROM stg_...`) | Easy; deterministic point-in-time state | Requires watermark/audit columns (`_ingested_at`) |
| **Schema Evolution Blast Radius** | Recompiled into downstream models | Immediate; can break if upstream drops column | Controlled on full refresh | High risk: requires `on_schema_change: sync_all_columns` or backfill |
| **Ideal Platform Layer** | **Intermediate (`int_*`)** reusable logic | **Staging (`stg_*`)** 1:1 source projections | **Dimensions (`dim_*`)** & small/medium marts | **Facts (`fct_*`)**, event streams, wide **OBT** |

---

### 1.2 Layer-by-Layer Strategy & Engineering Rationale

#### A. Staging Layer (`stg_customers`, `stg_orders`, `stg_payments`, `stg_products`) → **`view`**
* **What it does:** Renames columns to canonical snake_case, casts data types, parses timestamps to UTC, and computes lightweight surrogate keys without cross-table joins.
* **Why `view` instead of `table`:** Raw/Bronze tables already exist physically. Materializing staging as `table` doubles storage footprint and adds I/O latency for zero analytical gain. Because staging transformations are $O(1)$ per-row projections, `view` guarantees freshness the moment Bronze lands.
* **When to promote to `incremental`:** Only if staging performs expensive JSON unpacking (`VARIANT` / `JSONB` lateral flattening) on >100M events/day.

#### B. Intermediate Layer (`int_order_payments`) → **`ephemeral`**
* **What it does:** Encapsulates complex multi-step business joins and financial aggregations (e.g., reconciling order lines with payment gateway settlements, fees, and refunds in `int_order_payments`).
* **Why `ephemeral` instead of `view` or `table`:**
  * Keeps the warehouse schema catalog clean—analysts and BI tools cannot accidentally query half-baked intermediate tables.
  * Allows the query optimizer (Postgres planner, Redshift, Spark Catalyst) to push predicates (`WHERE order_date >= ...`) across CTE boundaries into the underlying fact scan.
* **Trade-off & Guardrail:** Deeply nested `ephemeral` DAGs (>4 levels or referenced by multiple downstream facts simultaneously) cause **CTE duplication** and query plan explosion. Once an `int_*` model is fanned out to $\ge 3$ downstream marts, promote it to a physical `table` or `incremental` model.

#### C. Dimensions Layer (`dim_customers`, `dim_products`) → **`table` (with SCD Type 2 snapshots)**
* **What it does:** Conformed entities backed by dbt snapshots (`snap_customers`, `snap_products`).
* **Why `table` instead of `view`:** Dimension tables are small-to-medium (thousands to low millions of rows) and heavily joined by facts and semantic queries. Precomputing indexes (Postgres B-Tree), sort keys (Redshift), or Z-Order/Liquid Clustering (Databricks) on surrogate keys eliminates repeated deduplication and window-function overhead at query time.

#### D. Facts & Revenue Mart (`fct_orders`, `mart_revenue_analytics`) → **`incremental`**
* **What it does:** High-volume transactional and revenue analytics models (`fct_orders`, `mart_revenue_analytics`) configured with `incremental_strategy: delete+insert` and `unique_key: order_id`.
* **Why `incremental` instead of `table`:** Full-refreshing a 500M-row order/revenue fact table every hour costs $O(N)$ I/O and compute, creating lock contention and linear cost growth. Incremental processing bounds build cost to $O(\Delta N)$ (the configurable `incremental_lookback_days` window).

#### E. Choosing the Right Incremental Strategy

| Strategy | Mechanism | Best Engine Fit | Pros | Cons / Pitfalls |
| :--- | :--- | :--- | :--- | :--- |
| **`merge`** | `MERGE INTO target USING source ON unique_key` | Databricks Delta, Snowflake, Redshift, Postgres 15+ | Row-level upserts; handles late updates cleanly | Expensive shuffle if `unique_key` isn't partitioned/clustered |
| **`delete+insert`** | `DELETE WHERE key IN (...)` then `INSERT` inside a transaction | PostgreSQL, Redshift, DuckDB | Simpler lock semantics; avoids `MERGE` edge cases with duplicate source keys | Needs careful transaction boundary to avoid partial reads |
| **`insert_overwrite`** | Replaces entire date/partition directories atomically | Databricks Spark/Delta, BigQuery, Athena/Iceberg | Zero row-level scan for existing partitions; idempotent partition replays | Rewrites entire partition even if 1 row changed |

* **Late-Arriving Data Guardrail:** Always include a configurable **lookback window** (e.g., `>= max(event_ts) - interval '3 days'`) to catch webhook retries or delayed payment settlements, paired with periodic weekend `--full-refresh` or partition reconciliation checks.

---

## 2. Shift-Left Data Contracts vs. Post-Load dbt Tests

Traditional pipelines follow **ELT ("Extract, Load, Hope")**: ingest arbitrary payloads into the warehouse and run dbt tests *after* loading.

```
Traditional ELT:  Source ──▶ Warehouse Raw ──▶ dbt Transform ──▶ dbt test (FAILS TOO LATE)
                                  └─ Corrupt rows already persisted & downstream SLAs missed

Shift-Left Gate:  Source ──▶ [YAML Data Contract Gate] ──┬─ Valid rows ──▶ Bronze/Raw ──▶ dbt ──▶ Marts
                                                         └─ Invalid rows ──▶ Quarantine (Dead-Letter) + Alert
```

### Trade-off Analysis

| Criterion | Post-Load dbt Tests Only | Shift-Left Data Contracts (`contracts/*.yml`) |
| :--- | :--- | :--- |
| **Mean Time to Detect (MTTD)** | Minutes to hours after ingestion finishes | **Milliseconds** at ingestion boundary |
| **Blast Radius** | Entire downstream DAG halts or serves stale/corrupt numbers | Valid records flow unimpeded; toxic records isolated to `quarantine` |
| **Producer Accountability** | Data Engineering debugs upstream app bugs alone | Contract YAML serves as versioned SLA (`contract_version`) owned jointly with producers |
| **Recovery / Replay** | Requires manual `DELETE` + backfill in warehouse | Replay sanitized JSONL/Parquet batches directly from Quarantine once fixed |
| **Ingestion Overhead** | Zero validation CPU at ingest | Modest schema/rule check overhead (~1–3 ms per micro-batch in Python/PySpark) |

### Platform Severity Policy (`shared_models.py`)
Our `ContractValidationReport` differentiates violation severity:
* **`WARNING`:** Non-breaking anomalies (e.g., unexpected optional enum value, minor formatting drift). Logged to telemetry, row may pass with coercion or warning tag.
* **`CRITICAL`:** Breaking schema/semantic violations (e.g., `amount_cents < 0` on non-refund invoice, null `customer_id`, missing required field, invalid currency ISO code). Rows are routed to `quarantine_dir` (locally JSONL; in AWS S3 `quarantine` bucket; in Databricks Unity Catalog Volume), `alert_dispatched` fires (webhook / AWS SNS), and if the batch error rate exceeds the contract's `max_invalid_ratio` threshold, `blocked = True` halts promotion to Bronze.

---

## 3. Slowly Changing Dimensions: SCD Type 2 vs. SCD Type 1 for Subscription Tier History

When a SaaS customer upgrades from `starter` ($49/mo) to `enterprise` ($999/mo) mid-year, **SCD Type 1** (in-place `UPDATE`) overwrites `plan_tier = 'enterprise'` across all historical joins.

* **The Type 1 Failure Mode:** Historical Q1 invoices suddenly appear attributed to the `enterprise` segment, inflating historical Enterprise ARPU and distorting cohort retention and expansion MRR analyses.
* **Our Choice — SCD Type 2 (`dim_subscriptions_history`):**
  * Tracks every state transition with `valid_from`, `valid_to`, `is_current`, and a deterministic `subscription_version_sk`.
  * Fact rows join via point-in-time range predicate: `fct.event_ts >= dim.valid_from AND fct.event_ts < coalesce(dim.valid_to, '9999-12-31')`.
* **Mitigating Type 2 Join Friction:** Because range joins are verbose for ad-hoc analysts and slower on large fact tables, we expose two complementary interfaces:
  1. **Point-in-time facts (`fct_mrr_movements`)** pre-resolve `subscription_version_sk` at ETL time (storing the surrogate key directly on the fact row for $O(1)$ equi-joins).
  2. **Current-state view (`dim_subscriptions_current`)** filters `WHERE is_current = TRUE` for operational CRM/CS dashboards needing only today's tier.

---

## 4. Centralized Semantic Layer: Eliminating Metric Drift

Without a centralized semantic layer, business logic fractures across tools:

* **Finance (Excel/Redshift):** `MRR = SUM(invoice_amount) / 100` (includes one-time setup fees).
* **Product (Looker/Metabase):** `MRR = SUM(recurring_amount)` (excludes annual amortized contracts).
* **AI Copilot (Text-to-SQL):** Hallucinates `AVG(amount)` across cancelled subscriptions.

### How Our Centralized Semantic Layer Solves This

Defined declaratively in `semantic_layer/metrics.yml` (`net_revenue`, `gross_revenue`, `average_order_value`, `take_rate_pct`, `retention_rate`, `churn_rate`) and served via `/api/v1/semantic/query` returning `SemanticMetricResult`:
1. **Metrics-as-Code:** Numerators, denominators, filters (`order_status = 'paid'`, `status = 'active'`), time grains, bilingual labels (`label_pt`, `label_en`), and allowed dimensions (`order_month`, `country`, `plan_tier`, `category`, `payment_method`) are version-controlled in Git and CI-tested.
2. **Safe SQL Compilation:** Clients request `{metric: "net_revenue", dimensions: ["plan_tier"]}` rather than writing raw SQL. The engine compiles parameterized, read-only SQL against governed Gold marts (`mart_revenue_analytics`, `dim_customers`).
3. **AI-Ready Grounding:** LLM agents query `/api/v1/semantic/metrics` for the governed catalog and execute `/api/v1/semantic/query`, eliminating hallucinated joins and inconsistent KPI definitions.
4. **Latency & Caching:** Frequently accessed executive KPIs return cached results (`cached=True`) within `<15ms`, keeping `p(95) < 250ms` under concurrent load (verified via `tests/load/k6_semantic_api.js`).

---

## 5. Local ↔ Cloud Mirror Parity Matrix

Every local component has a 1:1 cloud equivalent in **AWS (`infra/aws/main.tf`)** and **Databricks (`infra/databricks/workflow.json`)**:

| Platform Capability | Local Stack (Docker / Python) | AWS Cloud Mirror (`infra/aws/main.tf`) | Databricks Lakehouse Mirror (`infra/databricks/workflow.json`) |
| :--- | :--- | :--- | :--- |
| **Raw Landing Zone** | Local CSV / JSON streams | **S3 `raw` bucket** (KMS + Glacier lifecycle) | **Unity Catalog Volume / Auto Loader** (`Bronze`) |
| **Shift-Left Contract Gate** | Python Contract Validator (`contracts/*.yml`) | **ECS / AWS Glue PySpark** job | **Databricks Job Cluster PySpark** (`ingest_and_validate_contracts`) |
| **Dead-Letter Quarantine** | `data/quarantine/*.jsonl` | **S3 `quarantine` bucket** (180d lifecycle) | **Unity Catalog Quarantine Volume** (`/Volumes/.../quarantine`) |
| **Contract Breach Alerts** | Structured logs + optional webhook (`alert_dispatched`) | **Amazon SNS Topic** (KMS-encrypted) | **Databricks Job Webhook / PagerDuty / Email Alerts** |
| **Metadata & Schema Catalog** | YAML contracts + SQLite/Postgres `information_schema` | **AWS Glue Data Catalog** (`raw`, `staging`, `marts`, `semantic`) | **Unity Catalog** (`dcsp_prod` metastore + lineage) |
| **Analytical Warehouse** | **PostgreSQL 16** (or SQLite fallback) | **Redshift Serverless** + **Amazon Athena Workgroup** | **Databricks SQL Serverless Warehouse** + **Delta Lake** |
| **SQL Transformations** | **dbt Core** (`dbt-postgres`) | **dbt Core** (`dbt-redshift` / `dbt-athena`) | **dbt Core** (`dbt-databricks`) + **Delta Live Tables (DLT)** |
| **Semantic Layer** | FastAPI `/api/v1/semantic/*` + `metrics.yml` | ECS/Fargate Semantic API over Redshift/Athena | **Unity Catalog Metric Views** (`publish_semantic_catalog_views`) |
| **Observability** | **Prometheus** + **Grafana** | CloudWatch / Amazon Managed Prometheus & Grafana | Databricks System Tables + Lakehouse Monitoring |
