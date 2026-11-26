"""Tests for the Centralized Semantic Layer and Observability stack."""

from __future__ import annotations

import io
import json
import sqlite3
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from observability.telemetry import (
    MetricsRegistry,
    SpanTracer,
    correlation_id_context,
    get_logger,
    log_event,
)
from semantic_layer.api import create_http_server
from semantic_layer.engine import SemanticLayerEngine, parse_yaml_subset

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _seed_warehouse_db(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE mart_revenue_analytics (
                order_id TEXT PRIMARY KEY,
                order_month TEXT NOT NULL,
                country TEXT NOT NULL,
                plan_tier TEXT NOT NULL,
                category TEXT NOT NULL,
                payment_method TEXT NOT NULL,
                order_status TEXT NOT NULL,
                gross_revenue REAL NOT NULL,
                gateway_fee REAL NOT NULL,
                net_revenue REAL NOT NULL
            );

            CREATE TABLE dim_customers (
                customer_id TEXT PRIMARY KEY,
                country TEXT NOT NULL,
                plan_tier TEXT NOT NULL,
                status TEXT NOT NULL
            );

            INSERT INTO mart_revenue_analytics VALUES
                ('ord-1', '2026-01', 'BR', 'pro',        'saas',    'pix',         'paid',     1000.0, 30.0,  970.0),
                ('ord-2', '2026-01', 'BR', 'enterprise', 'addons',  'credit_card', 'paid',     2000.0, 80.0, 1920.0),
                ('ord-3', '2026-02', 'US', 'pro',        'saas',    'credit_card', 'paid',     1500.0, 45.0, 1455.0),
                ('ord-4', '2026-02', 'US', 'starter',    'support', 'boleto',      'refunded',  500.0, 15.0,  485.0);

            INSERT INTO dim_customers VALUES
                ('cust-1', 'BR', 'pro',        'active'),
                ('cust-2', 'BR', 'enterprise', 'active'),
                ('cust-3', 'BR', 'starter',    'churned'),
                ('cust-4', 'US', 'pro',        'active'),
                ('cust-5', 'US', 'starter',    'churned'),
                ('cust-6', 'US', 'enterprise', 'active'),
                ('cust-7', 'US', 'pro',        'active'),
                ('cust-8', 'BR', 'pro',        'churned');
            """
        )
        conn.commit()
    finally:
        conn.close()


class TestSemanticLayerAndObservability(unittest.TestCase):
    """End-to-end verification of semantic metrics, allowlists, API, tracing, logs, and Prometheus."""

    def setUp(self) -> None:
        self._tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self._tmp_dir.name) / "warehouse.db")
        _seed_warehouse_db(self.db_path)

        self.log_stream = io.StringIO()
        self.logger = get_logger("test.semantic_layer", level="INFO", stream=self.log_stream)
        self.registry = MetricsRegistry()
        self.tracer = SpanTracer(service_name="test-semantic-platform", logger=self.logger)
        self.metrics_yml = str(PROJECT_ROOT / "semantic_layer" / "metrics.yml")
        self.engine = SemanticLayerEngine(
            db_path=self.db_path,
            metrics_yaml_path=self.metrics_yml,
            metrics_registry=self.registry,
            tracer=self.tracer,
            logger=self.logger,
        )

    def tearDown(self) -> None:
        self._tmp_dir.cleanup()

    def test_builtin_yaml_parser_loads_all_metrics_and_dimensions(self) -> None:
        parsed = parse_yaml_subset(Path(self.metrics_yml).read_text(encoding="utf-8"))
        self.assertEqual(
            parsed["allowed_dimensions"],
            ["order_month", "country", "plan_tier", "category", "payment_method"],
        )
        expected_metrics = {
            "net_revenue",
            "gross_revenue",
            "average_order_value",
            "take_rate_pct",
            "retention_rate",
            "churn_rate",
        }
        self.assertEqual(set(parsed["metrics"].keys()), expected_metrics)

        listed = {m["name"]: m for m in self.engine.list_metrics()}
        self.assertEqual(set(listed.keys()), expected_metrics)
        self.assertEqual(listed["net_revenue"]["unit"], "currency")
        self.assertEqual(listed["retention_rate"]["unit"], "percent")

    def test_metric_consistency_and_reconciliation(self) -> None:
        total_net = self.engine.query_metric("net_revenue")
        by_country = self.engine.query_metric("net_revenue", dimensions=["country"])
        br_net = self.engine.query_metric("net_revenue", filters={"country": "BR"})
        us_net = self.engine.query_metric("net_revenue", filters={"country": "US"})

        self.assertEqual(total_net.total_value, 4345.0)
        self.assertEqual(by_country.total_value, total_net.total_value)
        self.assertAlmostEqual(
            sum(row["value"] for row in by_country.rows),
            total_net.total_value or 0.0,
            places=2,
        )
        self.assertAlmostEqual(
            (br_net.total_value or 0.0) + (us_net.total_value or 0.0),
            total_net.total_value or 0.0,
            places=2,
        )

        # Ratio and average metrics maintain exact total_value reconciliation when grouped
        gross = self.engine.query_metric("gross_revenue")
        self.assertEqual(gross.total_value, 5000.0)

        aov_total = self.engine.query_metric("average_order_value")
        aov_grouped = self.engine.query_metric("average_order_value", dimensions=["country"])
        self.assertEqual(aov_total.total_value, 1448.33)
        self.assertEqual(aov_grouped.total_value, aov_total.total_value)

        take_total = self.engine.query_metric("take_rate_pct")
        take_grouped = self.engine.query_metric("take_rate_pct", dimensions=["order_month"])
        self.assertEqual(take_total.total_value, 3.4)
        self.assertEqual(take_grouped.total_value, take_total.total_value)

        # Retention + churn == 100.0 globally and per filtered segment
        retention = self.engine.query_metric("retention_rate")
        churn = self.engine.query_metric("churn_rate")
        self.assertEqual(retention.total_value, 62.5)
        self.assertEqual(churn.total_value, 37.5)
        self.assertAlmostEqual(
            (retention.total_value or 0.0) + (churn.total_value or 0.0),
            100.0,
            places=2,
        )

        retention_br = self.engine.query_metric("retention_rate", filters={"country": "BR"})
        churn_br = self.engine.query_metric("churn_rate", filters={"country": "BR"})
        self.assertAlmostEqual(
            (retention_br.total_value or 0.0) + (churn_br.total_value or 0.0),
            100.0,
            places=2,
        )

    def test_allowlist_rejection_and_sql_injection_protection(self) -> None:
        with self.assertRaises(ValueError):
            self.engine.query_metric("unknown_metric")

        with self.assertRaises(ValueError):
            self.engine.query_metric("net_revenue", dimensions=["order_id"])

        with self.assertRaises(ValueError):
            self.engine.query_metric(
                "net_revenue", dimensions=["country; DROP TABLE mart_revenue_analytics"]
            )

        with self.assertRaises(ValueError):
            self.engine.query_metric("retention_rate", dimensions=["payment_method"])

        with self.assertRaises(ValueError):
            self.engine.query_metric("net_revenue", filters={"secret_column": "x"})

        # Malicious filter value is safely bound as parameter and matches 0 rows
        safe_result = self.engine.query_metric(
            "net_revenue", filters={"country": "BR' OR '1'='1"}
        )
        self.assertIsNone(safe_result.total_value)

    def test_deterministic_query_caching(self) -> None:
        first = self.engine.query_metric(
            "net_revenue", dimensions=["country"], filters={"order_month": "2026-01"}
        )
        second = self.engine.query_metric(
            "net_revenue", dimensions=["country"], filters={"order_month": "2026-01"}
        )
        self.assertFalse(first.cached)
        self.assertTrue(second.cached)
        self.assertEqual(first.total_value, second.total_value)
        self.assertEqual(first.rows, second.rows)

    def test_prometheus_metrics_and_span_tracer_and_json_logging(self) -> None:
        self.registry.inc_counter(
            "pipeline_runs_total", labels={"dataset": "orders", "status": "success"}
        )
        self.registry.inc_counter(
            "contract_violations_total",
            value=2,
            labels={"dataset": "orders", "severity": "WARNING", "rule": "min_value"},
        )

        with correlation_id_context("corr-test-123"):
            self.engine.query_metric("net_revenue", dimensions=["country"])
            with self.assertRaises(ValueError):
                self.engine.query_metric("net_revenue", dimensions=["bad_dim"])

        prom_text = self.registry.render_prometheus()
        self.assertIn("# TYPE pipeline_runs_total counter", prom_text)
        self.assertIn('pipeline_runs_total{dataset="orders",status="success"} 1', prom_text)
        self.assertIn(
            'contract_violations_total{dataset="orders",rule="min_value",severity="WARNING"} 2',
            prom_text,
        )
        self.assertIn("# TYPE semantic_queries_total counter", prom_text)
        self.assertIn(
            'semantic_queries_total{cached="false",metric="net_revenue",status="ok"} 1',
            prom_text,
        )
        self.assertIn(
            'semantic_queries_total{cached="false",metric="net_revenue",status="error"} 1',
            prom_text,
        )
        self.assertIn("# TYPE semantic_query_duration_seconds histogram", prom_text)
        self.assertIn('semantic_query_duration_seconds_bucket{metric="net_revenue",le="+Inf"} 2', prom_text)
        self.assertIn('semantic_query_duration_seconds_count{metric="net_revenue"} 2', prom_text)

        spans = self.tracer.get_spans()
        self.assertEqual(len(spans), 2)
        self.assertEqual(spans[0].correlation_id, "corr-test-123")
        self.assertEqual(spans[0].status, "OK")
        self.assertGreaterEqual(spans[0].duration_ms, 0.0)
        self.assertEqual(spans[1].status, "ERROR")
        self.assertIn("ValueError", spans[1].error or "")

        # Verify every log line emitted is valid JSON carrying correlation_id
        log_event(self.logger, "INFO", "manual_audit_event", correlation_id="corr-audit-99", actor="qa")
        lines = [line for line in self.log_stream.getvalue().splitlines() if line.strip()]
        self.assertGreaterEqual(len(lines), 4)
        parsed_logs = [json.loads(line) for line in lines]
        self.assertTrue(any(entry["correlation_id"] == "corr-test-123" for entry in parsed_logs))
        audit_entry = next(entry for entry in parsed_logs if entry["message"] == "manual_audit_event")
        self.assertEqual(audit_entry["correlation_id"], "corr-audit-99")
        self.assertEqual(audit_entry["actor"], "qa")

    def test_stdlib_http_api_endpoints(self) -> None:
        server = create_http_server(host="127.0.0.1", port=0, engine=self.engine)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base_url = f"http://127.0.0.1:{port}"

        try:
            with urllib.request.urlopen(f"{base_url}/health") as resp:
                self.assertEqual(resp.status, 200)
                health = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(health["status"], "ok")
                self.assertEqual(health["metrics_count"], 6)

            with urllib.request.urlopen(f"{base_url}/api/v1/semantic/metrics") as resp:
                self.assertEqual(resp.status, 200)
                catalog = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(len(catalog["metrics"]), 6)

            req = urllib.request.Request(
                f"{base_url}/api/v1/semantic/query",
                data=json.dumps(
                    {
                        "metric": "net_revenue",
                        "dimensions": ["country"],
                        "filters": {"order_month": "2026-01"},
                    }
                ).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "X-Correlation-ID": "http-corr-42",
                },
                method="POST",
            )
            with urllib.request.urlopen(req) as resp:
                self.assertEqual(resp.status, 200)
                self.assertEqual(resp.headers.get("X-Correlation-ID"), "http-corr-42")
                body = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(body["metric_name"], "net_revenue")
                self.assertEqual(body["total_value"], 2890.0)
                self.assertFalse(body["cached"])

            bad_req = urllib.request.Request(
                f"{base_url}/api/v1/semantic/query",
                data=json.dumps(
                    {"metric": "net_revenue", "dimensions": ["non_existent_dim"]}
                ).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                urllib.request.urlopen(bad_req)
            self.assertEqual(ctx.exception.code, 400)
            ctx.exception.close()

            with urllib.request.urlopen(f"{base_url}/metrics") as resp:
                self.assertEqual(resp.status, 200)
                self.assertIn("text/plain", resp.headers.get("Content-Type", ""))
                prom_body = resp.read().decode("utf-8")
                self.assertIn("semantic_queries_total", prom_body)
                self.assertIn("semantic_query_duration_seconds_bucket", prom_body)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_observability_config_files_exist_and_valid(self) -> None:
        prom_cfg = PROJECT_ROOT / "observability" / "prometheus.yml"
        grafana_cfg = PROJECT_ROOT / "observability" / "grafana" / "dashboard.json"
        self.assertTrue(prom_cfg.exists())
        self.assertTrue(grafana_cfg.exists())
        self.assertIn("/metrics", prom_cfg.read_text(encoding="utf-8"))
        dashboard = json.loads(grafana_cfg.read_text(encoding="utf-8"))
        self.assertEqual(len(dashboard["panels"]), 4)


if __name__ == "__main__":
    unittest.main()
