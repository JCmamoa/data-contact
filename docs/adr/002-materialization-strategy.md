# ADR-002: Layer-Specific Warehouse Materialization Strategy

* **Status:** Accepted
* **Date:** 2026-11-26
* **Decision Makers:** Principal Data Architecture & Analytics Engineering

## Context

Applying a uniform materialization across an entire dbt project leads to either excessive warehouse compute bills (rebuilding multi-terabyte fact tables via `table` full refreshes) or degraded BI query latency (stacking deep `view` chains on top of raw JSON payloads). Additionally, subscription businesses require accurate point-in-time historical attribution across tier upgrades and downgrades.

## Decision

We standardize warehouse materializations by architectural layer:

1. **Staging (`stg_*`) → `view`:** Pure 1:1 projections (casting, renaming, UTC normalization) with zero joins. Avoids duplicating Bronze storage.
2. **Intermediate (`int_*`) → `ephemeral`:** Encapsulates multi-step business logic (proration, MRR movement classification) as inlined CTEs. Prevents schema clutter and enables predicate pushdown. *Rule:* Promote to `table` if fanned out to $\ge 3$ downstream models.
3. **Dimensions (`dim_*`) → `table` (SCD Type 2 for `dim_subscriptions`):** Materialized physically for fast indexed/clustered joins. Subscription tier history uses **SCD Type 2** (`valid_from`, `valid_to`, `is_current`) to preserve point-in-time MRR/NRR accuracy, paired with a lightweight current-state view.
4. **Facts & OBT (`fct_*`, `obt_*`) → `incremental` (`merge` / `delete+insert`):** Processes only changed/new records within a configurable lookback window (default 3 days) to handle late-arriving billing events.

## Consequences

### Positive
* **Predictable FinOps:** Compute scales with $\Delta N$ (daily events) rather than $N$ (all-time history).
* **Sub-second BI Queries:** Gold marts and dimensions are pre-materialized and clustered.
* **Audit-Grade Revenue History:** SCD Type 2 prevents historical plan-tier overwrites from distorting cohort retention and expansion metrics.

### Negative & Mitigations
* **Incremental Drift Risk:** Late-arriving records outside the lookback window or schema changes can cause subtle drift. *Mitigation:* Configure `on_schema_change = 'sync_all_columns'`, maintain a 3-day lookback window, and schedule periodic reconciliation checks.
* **CTE Plan Complexity:** Deep `ephemeral` chains can inflate query compile time. *Mitigation:* Enforce a max depth of 3 intermediate hops in code review.

## Alternatives Considered

1. **Materialize all models as `table` (Rejected):** Unacceptable compute cost and latency on growing event volumes.
2. **SCD Type 1 for subscriptions (Rejected):** Overwrites historical plan tiers, corrupting historical ARPU, expansion MRR, and cohort churn metrics.
