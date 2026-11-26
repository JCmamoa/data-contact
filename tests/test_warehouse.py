"""Tests for dbt project structure and SQLite WarehouseRunner pipeline."""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from pipeline.warehouse import WarehouseRunner
from shared_models import QualityTestResult


class TestWarehouseRunner(unittest.TestCase):
    """End-to-end unit tests for staging, SCD Type 2, incremental OBT, and quality checks."""

    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmpdir.name) / "test_warehouse.db")
        self.runner = WarehouseRunner(db_path=self.db_path)

        self.base_datasets = {
            "customers": [
                {
                    "customer_id": "cust_001",
                    "customer_name": "  Acme Corp  ",
                    "email": "BILLING@ACME.IO ",
                    "segment": "Mid-Market",
                    "plan_tier": "Starter",
                    "status": "ACTIVE",
                    "country": "us",
                    "created_at": "2026-01-01T00:00:00Z",
                    "updated_at": "2026-01-01T00:00:00Z",
                },
                {
                    "customer_id": "cust_002",
                    "customer_name": "Beta Labs",
                    "email": "finance@betalabs.com",
                    "segment": "SMB",
                    "plan_tier": "Growth",
                    "status": "active",
                    "country": "br",
                    "created_at": "2026-01-05T00:00:00Z",
                    "updated_at": "2026-01-05T00:00:00Z",
                },
            ],
            "products": [
                {
                    "product_id": "prod_001",
                    "product_name": "Analytics Core",
                    "category": "Platform",
                    "unit_price": 500.00,
                    "currency": "usd",
                    "is_active": True,
                    "updated_at": "2026-01-01T00:00:00Z",
                },
                {
                    "product_id": "prod_002",
                    "product_name": "Enterprise Governance",
                    "category": "Add-on",
                    "unit_price": 1200.00,
                    "currency": "usd",
                    "is_active": True,
                    "updated_at": "2026-01-01T00:00:00Z",
                },
            ],
            "orders": [
                {
                    "order_id": "ord_001",
                    "customer_id": "cust_001",
                    "product_id": "prod_001",
                    "order_date": "2026-01-10",
                    "quantity": 1,
                    "gross_amount": 500.00,
                    "discount_amount": 50.00,
                    "tax_amount": 25.00,
                    "order_status": "COMPLETED",
                    "currency": "usd",
                    "updated_at": "2026-01-10T10:00:00Z",
                },
                {
                    "order_id": "ord_002",
                    "customer_id": "cust_002",
                    "product_id": "prod_002",
                    "order_date": "2026-01-15",
                    "quantity": 1,
                    "gross_amount": 1200.00,
                    "discount_amount": 100.00,
                    "tax_amount": 60.00,
                    "order_status": "completed",
                    "currency": "usd",
                    "updated_at": "2026-01-15T12:00:00Z",
                },
            ],
            "payments": [
                {
                    "payment_id": "pay_001",
                    "order_id": "ord_001",
                    "payment_method": "CREDIT_CARD",
                    "payment_status": "SUCCEEDED",
                    "amount_paid": 425.00,
                    "gateway_fee": 12.50,
                    "paid_at": "2026-01-10T10:01:00Z",
                    "updated_at": "2026-01-10T10:01:00Z",
                },
                {
                    "payment_id": "pay_002",
                    "order_id": "ord_002",
                    "payment_method": "pix",
                    "payment_status": "succeeded",
                    "amount_paid": 1040.00,
                    "gateway_fee": 10.00,
                    "paid_at": "2026-01-15T12:05:00Z",
                    "updated_at": "2026-01-15T12:05:00Z",
                },
            ],
        }

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def _query(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            return conn.execute(sql, params).fetchall()

    def test_staging_views_normalization_and_fact_net_revenue(self) -> None:
        self.runner.load_raw(self.base_datasets)
        summary = self.runner.run_transformations()

        self.assertEqual(summary["mode"], "full_refresh")
        self.assertEqual(summary["tables"]["stg_customers"], 2)
        self.assertEqual(summary["tables"]["mart_revenue_analytics"], 2)

        stg_cust = self._query(
            "SELECT customer_name, email, status, country FROM stg_customers WHERE customer_id = 'cust_001'"
        )[0]
        self.assertEqual(stg_cust["customer_name"], "Acme Corp")
        self.assertEqual(stg_cust["email"], "billing@acme.io")
        self.assertEqual(stg_cust["status"], "active")
        self.assertEqual(stg_cust["country"], "US")

        views = self._query(
            "SELECT name FROM sqlite_master WHERE type = 'view' AND name LIKE 'stg_%' ORDER BY name"
        )
        self.assertEqual(
            [v["name"] for v in views],
            ["stg_customers", "stg_orders", "stg_payments", "stg_products"],
        )

        # ord_001: 500 - 50 - 25 - 12.50 = 412.50
        mart_row = self._query(
            "SELECT net_revenue, order_month, plan_tier FROM mart_revenue_analytics WHERE order_id = 'ord_001'"
        )[0]
        self.assertAlmostEqual(mart_row["net_revenue"], 412.50, places=2)
        self.assertEqual(mart_row["order_month"], "2026-01-01")
        self.assertEqual(mart_row["plan_tier"], "Starter")

    def test_scd2_historical_versioning_starter_to_enterprise(self) -> None:
        self.runner.load_raw(self.base_datasets)
        first_run = self.runner.run_transformations()
        self.assertEqual(first_run["scd2_new_customer_versions"], 2)

        # Re-running without changes must be idempotent for SCD2
        noop_run = self.runner.run_transformations()
        self.assertEqual(noop_run["scd2_new_customer_versions"], 0)

        # Batch 2: cust_001 upgrades from Starter -> Enterprise
        self.runner.load_raw(
            {
                "customers": [
                    {
                        "customer_id": "cust_001",
                        "customer_name": "Acme Corp",
                        "email": "billing@acme.io",
                        "segment": "Enterprise",
                        "plan_tier": "Enterprise",
                        "status": "active",
                        "country": "US",
                        "created_at": "2026-01-01T00:00:00Z",
                        "updated_at": "2026-02-01T00:00:00Z",
                    }
                ]
            }
        )
        upgrade_run = self.runner.run_transformations()
        self.assertEqual(upgrade_run["scd2_new_customer_versions"], 1)

        history = self._query(
            """
            SELECT plan_tier, dbt_valid_from, dbt_valid_to, is_current
            FROM snap_customers_scd2
            WHERE customer_id = 'cust_001'
            ORDER BY dbt_valid_from ASC
            """
        )
        self.assertEqual(len(history), 2)

        self.assertEqual(history[0]["plan_tier"], "Starter")
        self.assertEqual(history[0]["dbt_valid_from"], "2026-01-01T00:00:00Z")
        self.assertEqual(history[0]["dbt_valid_to"], "2026-02-01T00:00:00Z")
        self.assertEqual(history[0]["is_current"], 0)

        self.assertEqual(history[1]["plan_tier"], "Enterprise")
        self.assertEqual(history[1]["dbt_valid_from"], "2026-02-01T00:00:00Z")
        self.assertIsNone(history[1]["dbt_valid_to"])
        self.assertEqual(history[1]["is_current"], 1)

        # dim_customers exposes only the current version
        dim_rows = self._query(
            "SELECT plan_tier, is_current FROM dim_customers WHERE customer_id = 'cust_001'"
        )
        self.assertEqual(len(dim_rows), 1)
        self.assertEqual(dim_rows[0]["plan_tier"], "Enterprise")
        self.assertEqual(dim_rows[0]["is_current"], 1)

    def test_incremental_mart_idempotency_and_upsert(self) -> None:
        self.runner.load_raw(self.base_datasets)
        self.runner.run_transformations()

        # Batch 2: update ord_002 discount and insert ord_003 after 2026-02-01
        self.runner.load_raw(
            {
                "orders": [
                    {
                        "order_id": "ord_002",
                        "customer_id": "cust_002",
                        "product_id": "prod_002",
                        "order_date": "2026-01-15",
                        "quantity": 1,
                        "gross_amount": 1200.00,
                        "discount_amount": 200.00,
                        "tax_amount": 60.00,
                        "order_status": "completed",
                        "currency": "USD",
                        "updated_at": "2026-02-02T09:00:00Z",
                    },
                    {
                        "order_id": "ord_003",
                        "customer_id": "cust_001",
                        "product_id": "prod_002",
                        "order_date": "2026-02-03",
                        "quantity": 1,
                        "gross_amount": 1200.00,
                        "discount_amount": 0.00,
                        "tax_amount": 60.00,
                        "order_status": "completed",
                        "currency": "USD",
                        "updated_at": "2026-02-03T11:00:00Z",
                    },
                ],
                "payments": [
                    {
                        "payment_id": "pay_003",
                        "order_id": "ord_003",
                        "payment_method": "wire_transfer",
                        "payment_status": "succeeded",
                        "amount_paid": 1140.00,
                        "gateway_fee": 15.00,
                        "paid_at": "2026-02-03T11:05:00Z",
                        "updated_at": "2026-02-03T11:05:00Z",
                    }
                ],
            }
        )

        inc_1 = self.runner.run_transformations(incremental_since="2026-02-01T00:00:00Z")
        self.assertEqual(inc_1["mode"], "incremental")
        self.assertEqual(inc_1["mart_upserted_rows"], 2)
        self.assertEqual(inc_1["tables"]["mart_revenue_analytics"], 3)

        # Re-run the same incremental window: must be idempotent (no duplicate rows)
        inc_2 = self.runner.run_transformations(incremental_since="2026-02-01T00:00:00Z")
        self.assertEqual(inc_2["tables"]["mart_revenue_analytics"], 3)

        ord_002 = self._query(
            "SELECT discount_amount, net_revenue FROM mart_revenue_analytics WHERE order_id = 'ord_002'"
        )[0]
        self.assertAlmostEqual(ord_002["discount_amount"], 200.00, places=2)
        # 1200 - 200 - 60 - 10 = 930.00
        self.assertAlmostEqual(ord_002["net_revenue"], 930.00, places=2)

    def test_all_quality_tests_pass_and_detect_regressions(self) -> None:
        self.runner.load_raw(self.base_datasets)
        self.runner.run_transformations()

        results = self.runner.run_quality_tests()
        self.assertTrue(len(results) >= 15)
        self.assertTrue(all(isinstance(r, QualityTestResult) for r in results))

        failures = [r for r in results if not r.passed]
        self.assertEqual(failures, [], f"Unexpected quality test failures: {failures}")

        names = {r.test_name for r in results}
        self.assertIn("scd2_single_current_row_snap_customers_scd2", names)
        self.assertIn("net_revenue_consistency_mart_revenue_analytics", names)
        self.assertIn("relationships_fct_orders_customer_id", names)

        # Introduce a negative net_revenue anomaly and verify net_revenue_consistency catches it
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE mart_revenue_analytics SET net_revenue = -10.0 WHERE order_id = 'ord_001'"
            )

        after_results = {r.test_name: r for r in self.runner.run_quality_tests()}
        net_rev_check = after_results["net_revenue_consistency_mart_revenue_analytics"]
        self.assertFalse(net_rev_check.passed)
        self.assertEqual(net_rev_check.failing_rows, 1)

    def test_dbt_project_files_present_and_well_formed(self) -> None:
        root = Path(__file__).resolve().parent.parent / "dbt"
        expected_files = [
            "dbt_project.yml",
            "profiles.yml",
            "macros/calculate_net_revenue.sql",
            "macros/incremental_window.sql",
            "models/staging/sources.yml",
            "models/staging/stg_customers.sql",
            "models/staging/stg_products.sql",
            "models/staging/stg_orders.sql",
            "models/staging/stg_payments.sql",
            "models/staging/schema.yml",
            "models/intermediate/int_order_payments.sql",
            "models/marts/dim_customers.sql",
            "models/marts/dim_products.sql",
            "models/marts/fct_orders.sql",
            "models/marts/mart_revenue_analytics.sql",
            "models/marts/schema.yml",
            "snapshots/snap_customers.sql",
            "snapshots/snap_products.sql",
            "tests/assert_net_revenue_non_negative.sql",
            "tests/assert_scd2_single_current_row.sql",
        ]
        for rel in expected_files:
            path = root / rel
            self.assertTrue(path.exists(), f"Missing dbt file: {path}")
            self.assertGreater(path.stat().st_size, 0, f"Empty dbt file: {path}")


if __name__ == "__main__":
    unittest.main()
