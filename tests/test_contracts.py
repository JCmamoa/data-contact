"""Unit tests for YAML data contracts, pure-Python validator, quarantine, alerts, and generator."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from ingestion.contracts import (
    AlertDispatcher,
    DataContractValidator,
    load_contract,
    parse_yaml_lite,
)
from ingestion.generator import (
    generate_customer_snapshots,
    generate_sample_datasets,
)
from ingestion import spark_ingest_mirror
from shared_models import PlatformConfig

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONTRACTS_DIR = PROJECT_ROOT / "contracts"


class TestYamlLiteParser(unittest.TestCase):
    """Tests for the zero-dependency YAML + JSON parser."""

    def test_parses_all_contract_files(self) -> None:
        expected_datasets = {
            "raw_customers": {"customer_id", "name", "email", "plan_tier", "country", "status", "updated_at"},
            "raw_products": {"product_id", "sku", "product_name", "category", "unit_price", "updated_at"},
            "raw_orders": {
                "order_id",
                "customer_id",
                "product_id",
                "order_date",
                "quantity",
                "gross_amount",
                "discount_amount",
                "tax_amount",
                "order_status",
                "currency",
            },
            "raw_payments": {
                "payment_id",
                "order_id",
                "payment_method",
                "gateway_fee",
                "paid_amount",
                "payment_status",
                "paid_at",
            },
        }
        for dataset, expected_cols in expected_datasets.items():
            raw_text = (CONTRACTS_DIR / f"{dataset}.yaml").read_text(encoding="utf-8")
            parsed = parse_yaml_lite(raw_text)
            self.assertEqual(parsed["dataset"], dataset)
            self.assertEqual(parsed["version"], "1.0.0")
            self.assertTrue(parsed["block_on_critical"])
            self.assertIsInstance(parsed["sla"], dict)
            self.assertEqual(set(parsed["columns"].keys()), expected_cols)

    def test_parses_json_comments_and_inline_collections(self) -> None:
        json_doc = '{"dataset": "test_ds", "block_on_critical": true, "columns": {}}'
        self.assertEqual(parse_yaml_lite(json_doc)["dataset"], "test_ds")

        yaml_doc = """
        # Top-level comment
        dataset: demo_ds # inline comment
        tags: [a, b, "c#d"]
        meta: {env: prod, retries: 3}
        rules:
          - name: col_a
            nullable: false
            pattern: '^\\d{4}-\\d{2}-\\d{2}$'
        """
        parsed = parse_yaml_lite(yaml_doc)
        self.assertEqual(parsed["dataset"], "demo_ds")
        self.assertEqual(parsed["tags"], ["a", "b", "c#d"])
        self.assertEqual(parsed["meta"], {"env": "prod", "retries": 3})
        self.assertEqual(parsed["rules"][0]["name"], "col_a")
        self.assertFalse(parsed["rules"][0]["nullable"])
        self.assertEqual(parsed["rules"][0]["pattern"], r"^\d{4}-\d{2}-\d{2}$")

    def test_load_contract_normalizes_list_columns(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            p = Path(tmpdir) / "custom.yaml"
            p.write_text(
                "dataset: custom\nversion: '2.0'\ncolumns:\n  - name: id\n    type: str\n    nullable: false\n",
                encoding="utf-8",
            )
            loaded = load_contract(p)
            self.assertIn("id", loaded["columns"])
            self.assertEqual(loaded["columns"]["id"]["type"], "str")


class TestDataContractValidator(unittest.TestCase):
    """Tests for batch validation, blocking, quarantine files, and alerts."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmp.name)
        self.quarantine_dir = self.tmp_path / "quarantine"
        self.alerts_dir = self.tmp_path / "alerts"
        self.validator = DataContractValidator(
            contracts_dir=CONTRACTS_DIR,
            quarantine_dir=self.quarantine_dir,
            alerts_dir=self.alerts_dir,
        )

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_valid_generated_batches_pass_all_contracts(self) -> None:
        datasets = generate_sample_datasets(seed=42, include_corrupted=False)
        for dataset, rows in datasets.items():
            valid_rows, report = self.validator.validate_batch(
                dataset, rows, correlation_id=f"valid_{dataset}"
            )
            self.assertFalse(report.blocked, f"{dataset} unexpectedly blocked: {report.violations}")
            self.assertEqual(report.total_rows, len(rows))
            self.assertEqual(report.valid_rows_count, len(rows))
            self.assertEqual(report.invalid_rows_count, 0)
            self.assertEqual(report.violations, [])
            self.assertEqual(report.quarantine_file, "")
            self.assertFalse(report.alert_dispatched)
            self.assertEqual(len(valid_rows), len(rows))

    def test_type_coercion_and_empty_batch(self) -> None:
        empty_rows, empty_report = self.validator.validate_batch("raw_products", [])
        self.assertEqual(empty_rows, [])
        self.assertFalse(empty_report.blocked)
        self.assertEqual(empty_report.total_rows, 0)

        coerced_batch = [
            {
                "product_id": "P-1",
                "sku": "SKU-1",
                "product_name": "Starter",
                "category": "SaaS",
                "unit_price": "49.50",
                "updated_at": "2026-01-01T00:00:00Z",
            }
        ]
        valid_rows, report = self.validator.validate_batch("raw_products", coerced_batch)
        self.assertFalse(report.blocked)
        self.assertIsInstance(valid_rows[0]["unit_price"], float)
        self.assertEqual(valid_rows[0]["unit_price"], 49.5)

    def test_missing_required_column_blocks_pipeline_and_alerts(self) -> None:
        datasets = generate_sample_datasets(seed=42, include_corrupted=False)
        orders = [dict(r) for r in datasets["raw_orders"][:5]]
        for row in orders:
            del row["gross_amount"]

        valid_rows, report = self.validator.validate_batch(
            "raw_orders", orders, correlation_id="missing_col_001"
        )
        self.assertTrue(report.blocked)
        self.assertEqual(valid_rows, [])
        self.assertEqual(report.valid_rows_count, 0)
        self.assertEqual(report.invalid_rows_count, 5)
        self.assertTrue(report.alert_dispatched)
        self.assertTrue(any(v.rule == "missing_column" and v.field_name == "gross_amount" for v in report.violations))

        q_file = Path(report.quarantine_file)
        self.assertTrue(q_file.is_file())
        self.assertEqual(q_file.name, "raw_orders_missing_col_001.json")
        q_payload = json.loads(q_file.read_text(encoding="utf-8"))
        self.assertEqual(len(q_payload["bad_records"]), 5)
        self.assertTrue(q_payload["report"]["blocked"])

        alert_file = self.alerts_dir / "raw_orders_missing_col_001.json"
        self.assertTrue(alert_file.is_file())
        alert_payload = json.loads(alert_file.read_text(encoding="utf-8"))
        self.assertEqual(alert_payload["severity"], "CRITICAL")
        self.assertEqual(alert_payload["dataset"], "raw_orders")

    def test_invalid_enums_nulls_negative_values_and_patterns_quarantined(self) -> None:
        bad_customers = [
            {
                "customer_id": "CUST-OK",
                "name": "Good Corp",
                "email": "good@corp.com",
                "plan_tier": "Growth",
                "country": "BR",
                "status": "active",
                "updated_at": "2026-02-01T00:00:00Z",
            },
            {
                "customer_id": None,
                "name": "Bad Corp",
                "email": "not-an-email",
                "plan_tier": "Free",
                "country": "AR",
                "status": "paused",
                "updated_at": "2026-02-01T00:00:00Z",
            },
        ]
        valid_rows, report = self.validator.validate_batch(
            "raw_customers", bad_customers, correlation_id="cust_bad_01"
        )
        self.assertTrue(report.blocked)
        self.assertEqual(valid_rows, [])
        self.assertEqual(report.valid_rows_count, 1)
        self.assertEqual(report.invalid_rows_count, 1)
        rules_triggered = {(v.field_name, v.rule) for v in report.violations}
        self.assertIn(("customer_id", "not_null"), rules_triggered)
        self.assertIn(("email", "pattern"), rules_triggered)
        self.assertIn(("plan_tier", "allowed_values"), rules_triggered)
        self.assertIn(("country", "allowed_values"), rules_triggered)
        self.assertIn(("status", "allowed_values"), rules_triggered)

        q_doc = json.loads(Path(report.quarantine_file).read_text(encoding="utf-8"))
        self.assertEqual(len(q_doc["bad_records"]), 1)
        self.assertEqual(q_doc["bad_records"][0]["row_index"], 1)

    def test_negative_and_zero_values_rejected(self) -> None:
        bad_products = [
            {
                "product_id": "P-ZERO",
                "sku": "SKU-0",
                "product_name": "Freebie",
                "category": "SaaS",
                "unit_price": 0.0,
                "updated_at": "2026-01-01T00:00:00Z",
            },
            {
                "product_id": "P-NEG",
                "sku": "SKU-NEG",
                "product_name": "Negative",
                "category": "SaaS",
                "unit_price": -10.5,
                "updated_at": "2026-01-01T00:00:00Z",
            },
        ]
        report = self.validator.validate("raw_products", bad_products, correlation_id="prod_neg")
        self.assertTrue(report.blocked)
        self.assertEqual(report.invalid_rows_count, 2)
        self.assertTrue(report.alert_dispatched)

    def test_corrupted_generator_datasets_all_block_and_quarantine(self) -> None:
        corrupted = generate_sample_datasets(seed=42, include_corrupted=True)
        for dataset, rows in corrupted.items():
            _, report = self.validator.validate_batch(
                dataset, rows, correlation_id=f"corrupt_{dataset}"
            )
            self.assertTrue(report.blocked, f"Expected {dataset} corrupted batch to block")
            self.assertGreaterEqual(report.invalid_rows_count, 1)
            self.assertTrue(Path(report.quarantine_file).is_file())
            self.assertTrue(report.alert_dispatched)

    def test_alert_webhook_post_when_configured(self) -> None:
        dispatcher = AlertDispatcher(
            alerts_dir=self.alerts_dir,
            webhook_url="https://hooks.example.test/alerts",
        )
        validator = DataContractValidator(
            contracts_dir=CONTRACTS_DIR,
            quarantine_dir=self.quarantine_dir,
            alert_dispatcher=dispatcher,
        )
        mock_resp = MagicMock()
        mock_resp.status = 202
        mock_resp.__enter__.return_value = mock_resp
        mock_resp.__exit__.return_value = False

        with patch("urllib.request.urlopen", return_value=mock_resp) as mock_urlopen:
            report = validator.validate(
                "raw_payments",
                [
                    {
                        "payment_id": "PAY-1",
                        "order_id": "ORD-1",
                        "payment_method": "crypto",
                        "gateway_fee": -1.0,
                        "paid_amount": -5.0,
                        "payment_status": "captured",
                        "paid_at": "2026-01-01T00:00:00Z",
                    }
                ],
                correlation_id="webhook_test",
            )
            self.assertTrue(report.blocked)
            self.assertTrue(report.alert_dispatched)
            mock_urlopen.assert_called_once()
            req = mock_urlopen.call_args[0][0]
            self.assertEqual(req.full_url, "https://hooks.example.test/alerts")
            self.assertEqual(dispatcher.last_alert["webhook_status"], 202)

    def test_from_config_factory(self) -> None:
        cfg = PlatformConfig(
            contracts_dir=str(CONTRACTS_DIR),
            quarantine_dir=str(self.quarantine_dir),
        )
        v = DataContractValidator.from_config(cfg)
        self.assertEqual(v.contracts_dir, CONTRACTS_DIR)
        self.assertEqual(v.quarantine_dir, self.quarantine_dir)


class TestSampleDatasetGenerator(unittest.TestCase):
    """Tests for deterministic multi-month dataset generation and SCD2 snapshots."""

    def test_generator_is_deterministic(self) -> None:
        ds_a = generate_sample_datasets(seed=42, include_corrupted=False)
        ds_b = generate_sample_datasets(seed=42, include_corrupted=False)
        self.assertEqual(ds_a, ds_b)

        ds_other = generate_sample_datasets(seed=99, include_corrupted=False)
        self.assertNotEqual(ds_a["raw_orders"], ds_other["raw_orders"])

    def test_generator_spans_jan_to_jun_2026_and_scd2_upgrades(self) -> None:
        ds = generate_sample_datasets(seed=42, include_corrupted=False)
        months = {row["order_date"][:7] for row in ds["raw_orders"]}
        self.assertEqual(
            months,
            {"2026-01", "2026-02", "2026-03", "2026-04", "2026-05", "2026-06"},
        )

        # Verify SCD Type 2 plan upgrades appear across customer snapshots
        plans_by_customer: dict[str, list[str]] = {}
        for row in ds["raw_customers"]:
            plans_by_customer.setdefault(row["customer_id"], []).append(row["plan_tier"])
        upgraded_customers = [cid for cid, plans in plans_by_customer.items() if len(set(plans)) > 1]
        self.assertGreaterEqual(len(upgraded_customers), 4)

        snap1, snap2 = generate_customer_snapshots(seed=42)
        self.assertEqual(len(snap1), 20)
        self.assertGreaterEqual(len(snap2), 6)

    def test_spark_mirror_module_compiles_without_pyspark(self) -> None:
        self.assertTrue(hasattr(spark_ingest_mirror, "SparkDataContractIngestor"))
        self.assertTrue(hasattr(spark_ingest_mirror, "build_spark_schema"))


if __name__ == "__main__":
    unittest.main()
