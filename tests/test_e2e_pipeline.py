"""End-to-end tests spanning Data Contracts -> Warehouse (SCD2 + OBT) -> Quality Tests -> Semantic API."""

from __future__ import annotations

import json
import os
import tempfile
import unittest

from pipeline.runner import run_full_pipeline
from semantic_layer.api import dispatch_request
from semantic_layer.engine import SemanticLayerEngine
from shared_models import PlatformConfig


class TestEndToEndDataPlatform(unittest.TestCase):
    """Verify full platform integration with real data contracts, SCD2, quality tests, and metrics."""

    def test_full_pipeline_and_api_reconciliation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            cfg = PlatformConfig(
                warehouse_sqlite_path=os.path.join(tmp_dir, "warehouse.db"),
                quarantine_dir=os.path.join(tmp_dir, "quarantine"),
                contracts_dir="contracts",
                metrics_yaml_path="semantic_layer/metrics.yml",
            )
            result = run_full_pipeline(cfg, seed=42, test_corrupted_contract=True)

            # 1. Valid contracts pass, corrupted batch blocked & quarantined
            for dataset in ("raw_customers", "raw_products", "raw_orders", "raw_payments"):
                self.assertFalse(result["contract_reports"][dataset]["blocked"])
                self.assertGreater(result["contract_reports"][dataset]["valid_rows_count"], 0)

            blocked = result["blocked_contract_demo"]
            self.assertIsNotNone(blocked)
            self.assertTrue(blocked["blocked"])
            self.assertTrue(blocked["alert_dispatched"])
            self.assertTrue(os.path.exists(blocked["quarantine_file"]))

            # 2. SCD Type 2 tracked customer upgrades & churn across snapshots
            self.assertGreater(
                result["warehouse_incremental_run"]["scd2_new_customer_versions"], 0
            )
            self.assertEqual(
                result["warehouse_incremental_run"]["tables"]["dim_customers"], 20
            )
            self.assertGreater(
                result["warehouse_incremental_run"]["tables"]["snap_customers_scd2"], 20
            )

            # 3. All 20 automated warehouse quality checks passed
            self.assertEqual(result["quality_total_count"], 20)
            self.assertEqual(result["quality_passed_count"], 20)

            # 4. Semantic layer metric reconciliation across dimensions
            total_net = result["semantic_metrics"]["net_revenue"]["total_value"]
            country_sum = round(
                sum(r["value"] for r in result["net_revenue_by_country"]["rows"]), 2
            )
            month_sum = round(
                sum(r["value"] for r in result["net_revenue_by_month"]["rows"]), 2
            )
            self.assertAlmostEqual(total_net, country_sum, places=1)
            self.assertAlmostEqual(total_net, month_sum, places=1)

            ret = result["semantic_metrics"]["retention_rate"]["total_value"]
            churn = result["semantic_metrics"]["churn_rate"]["total_value"]
            self.assertAlmostEqual(ret + churn, 100.0, places=2)

            # 5. API dispatch check (/health, /metrics, /api/v1/semantic/query)
            engine = SemanticLayerEngine(
                db_path=cfg.warehouse_sqlite_path,
                metrics_yaml_path=cfg.metrics_yaml_path,
            )
            status, _, body = dispatch_request(engine, "GET", "/health")
            self.assertEqual(status, 200)
            self.assertEqual(json.loads(body)["metrics_count"], 6)

            q_status, _, q_body = dispatch_request(
                engine,
                "POST",
                "/api/v1/semantic/query",
                json.dumps({"metric": "net_revenue", "dimensions": ["category"]}).encode("utf-8"),
            )
            self.assertEqual(q_status, 200)
            q_json = json.loads(q_body)
            self.assertAlmostEqual(
                round(sum(r["value"] for r in q_json["rows"]), 2),
                total_net,
                places=1,
            )

            m_status, _, m_body = dispatch_request(engine, "GET", "/metrics")
            self.assertEqual(m_status, 200)
            self.assertIn("semantic_queries_total", m_body.decode("utf-8"))


if __name__ == "__main__":
    unittest.main()
