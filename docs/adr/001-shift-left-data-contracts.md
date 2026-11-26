# ADR-001: Shift-Left Data Contracts with Dead-Letter Quarantine

* **Status:** Accepted
* **Date:** 2026-11-26
* **Decision Makers:** Principal Data Architecture & Platform Engineering

## Context

In conventional ELT pipelines, raw payloads from upstream microservices and billing webhooks are landed directly into the warehouse before validation. When an upstream team renames a field, emits negative subscription amounts, or sends malformed ISO currency codes, failures only surface hours later during post-load `dbt test` execution—by which time corrupt records have either broken downstream mart builds or silently skewed executive KPIs.

## Decision

We enforce **Shift-Left Data Contracts** at the ingestion boundary prior to writing records into the Bronze/Raw warehouse layer:

1. Every dataset is governed by a versioned YAML contract under `contracts/` specifying schema types, nullability, range constraints, allowed enums, and severity (`CRITICAL` vs `WARNING`).
2. Incoming batches are validated record-by-record by the contract engine (`ContractValidationReport` & `ContractViolation` in `shared_models.py`).
3. **Record-level Quarantine:** Rows failing `CRITICAL` rules are stripped from the valid stream and serialized with violation metadata and `correlation_id` to `data/quarantine/` (locally) or S3 Quarantine / Unity Catalog Volumes (cloud).
4. **Circuit Breaker:** If the invalid ratio exceeds the contract threshold, the batch sets `blocked = True`, preventing promotion to the warehouse and triggering immediate alerting (`alert_dispatched = True` via webhook locally or AWS SNS in production).

## Consequences

### Positive
* **Zero Silent Corruption:** Invalid records cannot enter `stg_*` or `fct_*` tables.
* **Minimal Blast Radius:** Valid records continue flowing on schedule when isolated malformed records are quarantined.
* **Fast Feedback Loop:** Producers receive immediate correlation-tracked alerts within seconds of emission rather than hours later.
* **Deterministic Replay:** Quarantined files retain raw payloads + violation context for automated replay after remediation.

### Negative & Mitigations
* **Compute Overhead at Ingestion:** Validating every record adds slight CPU overhead (~1–3 ms per micro-batch). *Mitigation:* Vectorized validation in PySpark/Polars for high-volume cloud streams.
* **Contract Maintenance Governance:** Schema changes require PR coordination. *Mitigation:* Enforce SemVer rules in CI so backward-compatible additions (`WARNING`/minor) never block deployments while breaking changes require major contract bumps.

## Alternatives Considered

1. **Post-load `dbt test` only (Rejected):** Catches errors too late; mixes valid and toxic records in raw tables and stalls entire DAGs.
2. **Strict drop-on-error without quarantine (Rejected):** Causes silent data loss and makes financial reconciliation impossible.
