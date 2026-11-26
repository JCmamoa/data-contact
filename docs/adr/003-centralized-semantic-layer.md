# ADR-003: Centralized Headless Semantic Layer for BI & AI Agents

* **Status:** Accepted
* **Date:** 2026-11-26
* **Decision Makers:** Principal Data Architecture & Platform Engineering

## Context

In multi-consumer data platforms, KPIs such as Monthly Recurring Revenue (`mrr`), Net Revenue Retention (`net_revenue_retention`), and Churn Rate (`churn_rate`) frequently diverge across BI dashboards, ad-hoc SQL notebooks, and emerging AI/LLM analytics agents. Each consumer reimplements filters (e.g., excluding trial or voided invoices), currency conversions, and denominator logic independently—eroding executive trust.

## Decision

We establish a **Centralized Headless Semantic Layer** as the single governed access plane for analytical metrics:

1. **Declarative Metrics-as-Code (`semantic_layer/metrics.yml`):** Every metric defines its canonical SQL expression, grain, allowed dimensions, unit (`currency`, `ratio`, `count`), and localized display labels (`label_pt`, `label_en`).
2. **Governed REST Execution Plane (`/api/v1/semantic/metrics`, `/api/v1/semantic/query`):** All downstream consumers—BI tools, Python services, and AI agents—discover and query metrics through the Semantic API, receiving standardized `SemanticMetricResult` payloads containing both results and the compiled SQL for full lineage transparency.
3. **Cloud Parity:** In Databricks, `metrics.yml` definitions mirror directly to **Unity Catalog Metric Views** (`infra/databricks/workflow.json`); in AWS, the Semantic API executes against **Redshift Serverless** / **Athena** with FinOps scan cutoffs.
4. **SLA Enforcement:** API latency and availability are continuously verified against a strict `p(95) < 250ms` and `< 1%` failure threshold via `tests/load/k6_semantic_api.js` and monitored via Prometheus/Grafana.

## Consequences

### Positive
* **Zero Metric Drift:** Changing a metric definition in `metrics.yml` updates every dashboard and AI agent simultaneously after CI validation.
* **Safer AI / Text-to-SQL Integration:** LLM agents select validated metric names and dimensions instead of generating arbitrary joins over raw tables.
* **Built-in Observability:** Every metric query is metered by Prometheus (`semantic_api_query_duration`) and audit-logged.

### Negative & Mitigations
* **API Availability Dependency:** The Semantic API becomes a critical-path service for BI/agents. *Mitigation:* Stateless horizontal scaling (`workers >= 2`), response caching (`cached` flag in `SemanticMetricResult`), healthchecks, and k6 load validation.
* **Expressiveness Boundary:** Complex exploratory ad-hoc queries may not fit predefined metrics. *Mitigation:* Analysts still retain read-only SQL access to Gold `marts` (`fct_*`, `dim_*`) while official KPIs remain locked to the Semantic Layer.

## Alternatives Considered

1. **Duplicating metric formulas inside each BI tool (Rejected):** Guarantees metric drift whenever business rules evolve.
2. **Direct LLM Text-to-SQL against warehouse tables (Rejected):** High hallucination rate on SCD Type 2 joins and proration rules.
