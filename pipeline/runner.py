"""End-to-end orchestrator running Data Contracts -> Warehouse (SCD2 + Marts) -> Quality Tests -> Semantic Layer."""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

from ingestion.contracts import DataContractValidator
from ingestion.generator import generate_customer_snapshots, generate_sample_datasets
from observability.telemetry import (
    MetricsRegistry,
    SpanTracer,
    correlation_id_context,
    get_logger,
    log_event,
)
from pipeline.warehouse import WarehouseRunner
from semantic_layer.engine import SemanticLayerEngine
from shared_models import PlatformConfig


def run_full_pipeline(
    config: PlatformConfig | None = None,
    seed: int = 42,
    test_corrupted_contract: bool = True,
    metrics_registry: MetricsRegistry | None = None,
) -> dict[str, Any]:
    """Execute the full platform pipeline and return structured execution telemetry."""
    cfg = config or PlatformConfig.from_env()
    corr_id = uuid.uuid4().hex
    logger = get_logger("dcsp.pipeline", cfg.log_level)
    registry = metrics_registry or MetricsRegistry()
    tracer = SpanTracer(service_name="dcsp-pipeline")

    with correlation_id_context(corr_id):
        with tracer.start_span("full_pipeline_run", correlation_id=corr_id) as root_span:
            log_event(
                logger,
                "INFO",
                "pipeline_started",
                correlation_id=corr_id,
                db_path=cfg.warehouse_sqlite_path,
            )

            # 1. Generate deterministic valid datasets & chronological customer snapshots (for SCD2)
            with tracer.start_span("generate_datasets", correlation_id=corr_id):
                snap_1, snap_2 = generate_customer_snapshots(seed=seed)
                datasets = generate_sample_datasets(seed=seed, include_corrupted=False)

            # 2. Validate incoming batches against YAML Data Contracts
            contract_reports: dict[str, Any] = {}
            validator = DataContractValidator(
                contracts_dir=cfg.contracts_dir,
                quarantine_dir=cfg.quarantine_dir,
                alerts_dir=str(Path(cfg.quarantine_dir).parent / "alerts"),
            )
            with tracer.start_span("validate_data_contracts", correlation_id=corr_id):
                for name in ("raw_customers", "raw_products", "raw_orders", "raw_payments"):
                    report = validator.validate(
                        dataset=name,
                        records=datasets[name],
                        correlation_id=f"{corr_id}_{name}",
                    )
                    contract_reports[name] = report.to_dict()
                    registry.inc_counter(
                        "pipeline_runs_total",
                        labels={"stage": "contract_check", "dataset": name},
                    )
                    if report.blocked:
                        registry.inc_counter(
                            "contract_violations_total",
                            value=float(len(report.violations)),
                            labels={"dataset": name, "severity": "CRITICAL"},
                        )
                        raise RuntimeError(
                            f"Valid batch unexpectedly blocked by contract on {name}"
                        )

            # Optional negative test proving shift-left contract blocking & quarantine
            blocked_demo_report: dict[str, Any] | None = None
            if test_corrupted_contract:
                corrupted = generate_sample_datasets(seed=seed, include_corrupted=True)
                bad_report = validator.validate(
                    dataset="raw_orders",
                    records=corrupted["raw_orders"],
                    correlation_id=f"{corr_id}_corrupted_orders",
                )
                blocked_demo_report = bad_report.to_dict()
                if bad_report.violations:
                    registry.inc_counter(
                        "contract_violations_total",
                        value=float(len(bad_report.violations)),
                        labels={"dataset": "raw_orders", "severity": "CRITICAL"},
                    )

            # 3. Load Warehouse & Run Transformations (Snapshot 1 -> Snapshot 2 for SCD2 + Incremental Mart)
            warehouse = WarehouseRunner(db_path=cfg.warehouse_sqlite_path)
            with tracer.start_span("warehouse_transform_scd2", correlation_id=corr_id):
                # First pass: initial customer snapshot + products/orders/payments
                warehouse.load_raw(
                    {
                        "raw_customers": snap_1,
                        "raw_products": datasets["raw_products"],
                        "raw_orders": datasets["raw_orders"],
                        "raw_payments": datasets["raw_payments"],
                    }
                )
                first_run = warehouse.run_transformations()

                # Second pass: apply customer upgrades & churn snapshot + incremental mart check
                warehouse.load_raw({"raw_customers": snap_2})
                second_run = warehouse.run_transformations(incremental_since="2026-03-01")
                registry.inc_counter(
                    "pipeline_runs_total",
                    labels={"stage": "warehouse_transform", "dataset": "all"},
                )

            # 4. Run Automated Warehouse Quality Tests
            with tracer.start_span("warehouse_quality_tests", correlation_id=corr_id):
                quality_results = warehouse.run_quality_tests()
                failed_tests = [q for q in quality_results if not q.passed]

            # 5. Query Centralized Semantic Layer Metrics
            semantic = SemanticLayerEngine(
                db_path=cfg.warehouse_sqlite_path,
                metrics_yaml_path=cfg.metrics_yaml_path,
                postgres_dsn=cfg.postgres_dsn,
                metrics_registry=registry,
            )
            semantic_summary: dict[str, Any] = {}
            with tracer.start_span("semantic_layer_queries", correlation_id=corr_id):
                for metric_meta in semantic.list_metrics():
                    m_name = metric_meta["name"]
                    res = semantic.query_metric(m_name, correlation_id=corr_id)
                    semantic_summary[m_name] = res.to_dict()

                net_by_country = semantic.query_metric(
                    "net_revenue",
                    dimensions=["country"],
                    correlation_id=corr_id,
                ).to_dict()
                net_by_month = semantic.query_metric(
                    "net_revenue",
                    dimensions=["order_month"],
                    correlation_id=corr_id,
                ).to_dict()

            root_span.attributes["quality_tests_total"] = len(quality_results)
            root_span.attributes["quality_tests_failed"] = len(failed_tests)

            log_event(
                logger,
                "INFO",
                "pipeline_completed",
                correlation_id=corr_id,
                quality_passed=len(quality_results) - len(failed_tests),
                quality_total=len(quality_results),
            )

    return {
        "correlation_id": corr_id,
        "contract_reports": contract_reports,
        "blocked_contract_demo": blocked_demo_report,
        "warehouse_initial_run": first_run,
        "warehouse_incremental_run": second_run,
        "quality_tests": [q.to_dict() for q in quality_results],
        "quality_passed_count": len(quality_results) - len(failed_tests),
        "quality_total_count": len(quality_results),
        "semantic_metrics": semantic_summary,
        "net_revenue_by_country": net_by_country,
        "net_revenue_by_month": net_by_month,
        "spans": [s.to_dict() for s in tracer.get_spans()],
        "prometheus_metrics": registry.render_prometheus(),
    }


if __name__ == "__main__":
    import json

    summary = run_full_pipeline()
    print(
        json.dumps(
            {
                "correlation_id": summary["correlation_id"],
                "quality_passed": f"{summary['quality_passed_count']}/{summary['quality_total_count']}",
                "scd2_versions_added": summary["warehouse_incremental_run"]["scd2_new_customer_versions"],
                "net_revenue_total": summary["semantic_metrics"]["net_revenue"]["total_value"],
                "retention_rate": summary["semantic_metrics"]["retention_rate"]["total_value"],
            },
            indent=2,
        )
    )
