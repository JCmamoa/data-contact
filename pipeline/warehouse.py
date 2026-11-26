"""SQLite local warehouse runner mirroring the dbt staging, SCD2, star-schema, and OBT layers."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from shared_models import QualityTestResult


class WarehouseRunner:
    """Executes raw ingestion, SQLite transformations (SCD2, dims, facts, incremental OBT), and quality tests."""

    def __init__(self, db_path: str = "data/warehouse.db") -> None:
        self.db_path = db_path
        if db_path != ":memory:":
            parent = Path(db_path).parent
            if str(parent) and str(parent) != ".":
                parent.mkdir(parents=True, exist_ok=True)
        self._init_raw_tables()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_raw_tables(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS raw_customers (
                    customer_id TEXT PRIMARY KEY,
                    customer_name TEXT,
                    email TEXT,
                    segment TEXT,
                    plan_tier TEXT,
                    status TEXT,
                    country TEXT,
                    created_at TEXT,
                    updated_at TEXT
                );

                CREATE TABLE IF NOT EXISTS raw_products (
                    product_id TEXT PRIMARY KEY,
                    product_name TEXT,
                    category TEXT,
                    unit_price REAL,
                    currency TEXT,
                    is_active INTEGER,
                    updated_at TEXT
                );

                CREATE TABLE IF NOT EXISTS raw_orders (
                    order_id TEXT PRIMARY KEY,
                    customer_id TEXT,
                    product_id TEXT,
                    order_date TEXT,
                    quantity INTEGER,
                    gross_amount REAL,
                    discount_amount REAL,
                    tax_amount REAL,
                    order_status TEXT,
                    currency TEXT,
                    updated_at TEXT
                );

                CREATE TABLE IF NOT EXISTS raw_payments (
                    payment_id TEXT PRIMARY KEY,
                    order_id TEXT,
                    payment_method TEXT,
                    payment_status TEXT,
                    amount_paid REAL,
                    gateway_fee REAL,
                    paid_at TEXT,
                    updated_at TEXT
                );
                """
            )

    def load_raw(self, datasets: dict[str, list[dict[str, Any]]]) -> dict[str, int]:
        """Persist validated landing datasets into raw_* tables via primary-key upsert."""
        self._init_raw_tables()
        loaded_counts: dict[str, int] = {}

        table_aliases = {
            "customers": "raw_customers",
            "raw_customers": "raw_customers",
            "products": "raw_products",
            "raw_products": "raw_products",
            "orders": "raw_orders",
            "raw_orders": "raw_orders",
            "payments": "raw_payments",
            "raw_payments": "raw_payments",
        }

        with self._connect() as conn:
            for key, rows in datasets.items():
                table_name = table_aliases.get(key)
                if table_name is None:
                    raise ValueError(f"Unsupported dataset key: {key}")

                if table_name == "raw_customers":
                    records = [
                        (
                            row["customer_id"],
                            row.get("customer_name") or row.get("name", ""),
                            row.get("email", ""),
                            row.get("segment", "SMB"),
                            row.get("plan_tier", "Starter"),
                            row.get("status", "active"),
                            row.get("country", "US"),
                            row.get("created_at", ""),
                            row.get("updated_at") or row.get("created_at", ""),
                        )
                        for row in rows
                    ]
                    conn.executemany(
                        """
                        INSERT OR REPLACE INTO raw_customers (
                            customer_id, customer_name, email, segment,
                            plan_tier, status, country, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        records,
                    )
                elif table_name == "raw_products":
                    records = [
                        (
                            row["product_id"],
                            row.get("product_name") or row.get("name", ""),
                            row.get("category", "Subscription"),
                            float(row.get("unit_price", 0.0)),
                            row.get("currency", "USD"),
                            int(bool(row.get("is_active", True))),
                            row.get("updated_at", ""),
                        )
                        for row in rows
                    ]
                    conn.executemany(
                        """
                        INSERT OR REPLACE INTO raw_products (
                            product_id, product_name, category, unit_price,
                            currency, is_active, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        records,
                    )
                elif table_name == "raw_orders":
                    records = [
                        (
                            row["order_id"],
                            row["customer_id"],
                            row["product_id"],
                            row.get("order_date", ""),
                            int(row.get("quantity", 1)),
                            float(row.get("gross_amount", 0.0)),
                            float(row.get("discount_amount", 0.0)),
                            float(row.get("tax_amount", 0.0)),
                            row.get("order_status") or row.get("status", "completed"),
                            row.get("currency", "USD"),
                            row.get("updated_at") or row.get("order_date", ""),
                        )
                        for row in rows
                    ]
                    conn.executemany(
                        """
                        INSERT OR REPLACE INTO raw_orders (
                            order_id, customer_id, product_id, order_date,
                            quantity, gross_amount, discount_amount, tax_amount,
                            order_status, currency, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        records,
                    )
                elif table_name == "raw_payments":
                    records = [
                        (
                            row["payment_id"],
                            row["order_id"],
                            row.get("payment_method", "credit_card"),
                            row.get("payment_status") or row.get("status", "succeeded"),
                            float(
                                row["amount_paid"]
                                if row.get("amount_paid") is not None
                                else (
                                    row["paid_amount"]
                                    if row.get("paid_amount") is not None
                                    else row.get("amount", 0.0)
                                )
                            ),
                            float(row.get("gateway_fee", 0.0)),
                            row.get("paid_at", ""),
                            row.get("updated_at") or row.get("paid_at", ""),
                        )
                        for row in rows
                    ]
                    conn.executemany(
                        """
                        INSERT OR REPLACE INTO raw_payments (
                            payment_id, order_id, payment_method, payment_status,
                            amount_paid, gateway_fee, paid_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        records,
                    )

                loaded_counts[table_name] = len(rows)

        return loaded_counts

    def run_transformations(self, incremental_since: str | None = None) -> dict[str, Any]:
        """Run staging views, SCD Type 2 snapshots, dimensions, facts, and incremental OBT mart."""
        with self._connect() as conn:
            self._create_staging_views(conn)
            new_customer_versions = self._apply_customers_scd2(conn)
            new_product_versions = self._apply_products_scd2(conn)
            self._build_dimensions(conn)
            self._build_facts(conn, incremental_since=incremental_since)
            mart_upserted_rows = self._build_mart_revenue_analytics(
                conn, incremental_since=incremental_since
            )

            table_names = [
                "stg_customers",
                "stg_products",
                "stg_orders",
                "stg_payments",
                "snap_customers_scd2",
                "snap_products_scd2",
                "dim_customers",
                "dim_products",
                "fct_orders",
                "fct_payments",
                "mart_revenue_analytics",
            ]
            counts = {
                name: conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
                for name in table_names
            }

        return {
            "mode": "incremental" if incremental_since else "full_refresh",
            "incremental_since": incremental_since,
            "scd2_new_customer_versions": new_customer_versions,
            "scd2_new_product_versions": new_product_versions,
            "mart_upserted_rows": mart_upserted_rows,
            "tables": counts,
        }

    def _create_staging_views(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            DROP VIEW IF EXISTS stg_customers;
            CREATE VIEW stg_customers AS
            SELECT
                TRIM(CAST(customer_id AS TEXT)) AS customer_id,
                TRIM(CAST(customer_name AS TEXT)) AS customer_name,
                LOWER(TRIM(CAST(email AS TEXT))) AS email,
                TRIM(CAST(segment AS TEXT)) AS segment,
                TRIM(CAST(plan_tier AS TEXT)) AS plan_tier,
                LOWER(TRIM(CAST(status AS TEXT))) AS status,
                UPPER(TRIM(CAST(country AS TEXT))) AS country,
                NULLIF(TRIM(CAST(created_at AS TEXT)), '') AS created_at,
                COALESCE(
                    NULLIF(TRIM(CAST(updated_at AS TEXT)), ''),
                    NULLIF(TRIM(CAST(created_at AS TEXT)), ''),
                    strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                ) AS updated_at
            FROM raw_customers
            WHERE customer_id IS NOT NULL;

            DROP VIEW IF EXISTS stg_products;
            CREATE VIEW stg_products AS
            SELECT
                TRIM(CAST(product_id AS TEXT)) AS product_id,
                TRIM(CAST(product_name AS TEXT)) AS product_name,
                TRIM(CAST(category AS TEXT)) AS category,
                ROUND(CAST(COALESCE(unit_price, 0.0) AS REAL), 2) AS unit_price,
                UPPER(TRIM(COALESCE(NULLIF(CAST(currency AS TEXT), ''), 'USD'))) AS currency,
                COALESCE(CAST(is_active AS INTEGER), 1) AS is_active,
                COALESCE(
                    NULLIF(TRIM(CAST(updated_at AS TEXT)), ''),
                    strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                ) AS updated_at
            FROM raw_products
            WHERE product_id IS NOT NULL;

            DROP VIEW IF EXISTS stg_orders;
            CREATE VIEW stg_orders AS
            SELECT
                TRIM(CAST(order_id AS TEXT)) AS order_id,
                TRIM(CAST(customer_id AS TEXT)) AS customer_id,
                TRIM(CAST(product_id AS TEXT)) AS product_id,
                TRIM(CAST(order_date AS TEXT)) AS order_date,
                COALESCE(CAST(quantity AS INTEGER), 1) AS quantity,
                ROUND(CAST(COALESCE(gross_amount, 0.0) AS REAL), 2) AS gross_amount,
                ROUND(CAST(COALESCE(discount_amount, 0.0) AS REAL), 2) AS discount_amount,
                ROUND(CAST(COALESCE(tax_amount, 0.0) AS REAL), 2) AS tax_amount,
                LOWER(TRIM(CAST(order_status AS TEXT))) AS order_status,
                UPPER(TRIM(COALESCE(NULLIF(CAST(currency AS TEXT), ''), 'USD'))) AS currency,
                COALESCE(
                    NULLIF(TRIM(CAST(updated_at AS TEXT)), ''),
                    NULLIF(TRIM(CAST(order_date AS TEXT)), ''),
                    strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                ) AS updated_at
            FROM raw_orders
            WHERE order_id IS NOT NULL;

            DROP VIEW IF EXISTS stg_payments;
            CREATE VIEW stg_payments AS
            SELECT
                TRIM(CAST(payment_id AS TEXT)) AS payment_id,
                TRIM(CAST(order_id AS TEXT)) AS order_id,
                LOWER(TRIM(CAST(payment_method AS TEXT))) AS payment_method,
                LOWER(TRIM(CAST(payment_status AS TEXT))) AS payment_status,
                ROUND(CAST(COALESCE(amount_paid, 0.0) AS REAL), 2) AS amount_paid,
                ROUND(CAST(COALESCE(gateway_fee, 0.0) AS REAL), 2) AS gateway_fee,
                NULLIF(TRIM(CAST(paid_at AS TEXT)), '') AS paid_at,
                COALESCE(
                    NULLIF(TRIM(CAST(updated_at AS TEXT)), ''),
                    NULLIF(TRIM(CAST(paid_at AS TEXT)), ''),
                    strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                ) AS updated_at
            FROM raw_payments
            WHERE payment_id IS NOT NULL;
            """
        )

    def _apply_customers_scd2(self, conn: sqlite3.Connection) -> int:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS snap_customers_scd2 (
                customer_sk TEXT PRIMARY KEY,
                customer_id TEXT NOT NULL,
                customer_name TEXT,
                email TEXT,
                segment TEXT,
                plan_tier TEXT,
                status TEXT,
                country TEXT,
                created_at TEXT,
                updated_at TEXT,
                dbt_valid_from TEXT NOT NULL,
                dbt_valid_to TEXT,
                is_current INTEGER NOT NULL
            )
            """
        )
        conn.execute("DROP TABLE IF EXISTS tmp_customer_scd_changes")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_customer_scd_changes AS
            SELECT
                s.customer_id,
                s.customer_name,
                s.email,
                s.segment,
                s.plan_tier,
                s.status,
                s.country,
                s.created_at,
                s.updated_at,
                CASE
                    WHEN cur.customer_id IS NULL THEN
                        COALESCE(s.updated_at, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                    WHEN s.updated_at IS NOT NULL AND s.updated_at > cur.dbt_valid_from THEN
                        s.updated_at
                    ELSE
                        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                END AS effective_from,
                (
                    SELECT COUNT(*)
                    FROM snap_customers_scd2 h
                    WHERE h.customer_id = s.customer_id
                ) + 1 AS version_num,
                CASE WHEN cur.customer_id IS NULL THEN 0 ELSE 1 END AS has_existing_current
            FROM stg_customers s
            LEFT JOIN snap_customers_scd2 cur
                ON s.customer_id = cur.customer_id
               AND cur.is_current = 1
            WHERE cur.customer_id IS NULL
               OR s.plan_tier IS NOT cur.plan_tier
               OR s.status IS NOT cur.status
               OR s.segment IS NOT cur.segment
               OR s.country IS NOT cur.country
               OR s.customer_name IS NOT cur.customer_name
               OR s.email IS NOT cur.email
            """
        )

        new_versions = conn.execute(
            "SELECT COUNT(*) FROM tmp_customer_scd_changes"
        ).fetchone()[0]

        if new_versions > 0:
            conn.execute(
                """
                UPDATE snap_customers_scd2
                SET dbt_valid_to = (
                        SELECT c.effective_from
                        FROM tmp_customer_scd_changes c
                        WHERE c.customer_id = snap_customers_scd2.customer_id
                    ),
                    is_current = 0
                WHERE is_current = 1
                  AND customer_id IN (
                      SELECT customer_id
                      FROM tmp_customer_scd_changes
                      WHERE has_existing_current = 1
                  )
                """
            )
            conn.execute(
                """
                INSERT INTO snap_customers_scd2 (
                    customer_sk, customer_id, customer_name, email, segment,
                    plan_tier, status, country, created_at, updated_at,
                    dbt_valid_from, dbt_valid_to, is_current
                )
                SELECT
                    customer_id || '_v' || version_num AS customer_sk,
                    customer_id,
                    customer_name,
                    email,
                    segment,
                    plan_tier,
                    status,
                    country,
                    created_at,
                    updated_at,
                    effective_from AS dbt_valid_from,
                    NULL AS dbt_valid_to,
                    1 AS is_current
                FROM tmp_customer_scd_changes
                """
            )

        conn.execute("DROP TABLE IF EXISTS tmp_customer_scd_changes")
        return int(new_versions)

    def _apply_products_scd2(self, conn: sqlite3.Connection) -> int:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS snap_products_scd2 (
                product_sk TEXT PRIMARY KEY,
                product_id TEXT NOT NULL,
                product_name TEXT,
                category TEXT,
                unit_price REAL,
                currency TEXT,
                is_active INTEGER,
                updated_at TEXT,
                dbt_valid_from TEXT NOT NULL,
                dbt_valid_to TEXT,
                is_current INTEGER NOT NULL
            )
            """
        )
        conn.execute("DROP TABLE IF EXISTS tmp_product_scd_changes")
        conn.execute(
            """
            CREATE TEMP TABLE tmp_product_scd_changes AS
            SELECT
                s.product_id,
                s.product_name,
                s.category,
                s.unit_price,
                s.currency,
                s.is_active,
                s.updated_at,
                CASE
                    WHEN cur.product_id IS NULL THEN
                        COALESCE(s.updated_at, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                    WHEN s.updated_at IS NOT NULL AND s.updated_at > cur.dbt_valid_from THEN
                        s.updated_at
                    ELSE
                        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                END AS effective_from,
                (
                    SELECT COUNT(*)
                    FROM snap_products_scd2 h
                    WHERE h.product_id = s.product_id
                ) + 1 AS version_num,
                CASE WHEN cur.product_id IS NULL THEN 0 ELSE 1 END AS has_existing_current
            FROM stg_products s
            LEFT JOIN snap_products_scd2 cur
                ON s.product_id = cur.product_id
               AND cur.is_current = 1
            WHERE cur.product_id IS NULL
               OR s.product_name IS NOT cur.product_name
               OR s.category IS NOT cur.category
               OR s.unit_price IS NOT cur.unit_price
               OR s.currency IS NOT cur.currency
               OR s.is_active IS NOT cur.is_active
            """
        )

        new_versions = conn.execute(
            "SELECT COUNT(*) FROM tmp_product_scd_changes"
        ).fetchone()[0]

        if new_versions > 0:
            conn.execute(
                """
                UPDATE snap_products_scd2
                SET dbt_valid_to = (
                        SELECT c.effective_from
                        FROM tmp_product_scd_changes c
                        WHERE c.product_id = snap_products_scd2.product_id
                    ),
                    is_current = 0
                WHERE is_current = 1
                  AND product_id IN (
                      SELECT product_id
                      FROM tmp_product_scd_changes
                      WHERE has_existing_current = 1
                  )
                """
            )
            conn.execute(
                """
                INSERT INTO snap_products_scd2 (
                    product_sk, product_id, product_name, category, unit_price,
                    currency, is_active, updated_at, dbt_valid_from, dbt_valid_to, is_current
                )
                SELECT
                    product_id || '_v' || version_num AS product_sk,
                    product_id,
                    product_name,
                    category,
                    unit_price,
                    currency,
                    is_active,
                    updated_at,
                    effective_from AS dbt_valid_from,
                    NULL AS dbt_valid_to,
                    1 AS is_current
                FROM tmp_product_scd_changes
                """
            )

        conn.execute("DROP TABLE IF EXISTS tmp_product_scd_changes")
        return int(new_versions)

    def _build_dimensions(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            DROP TABLE IF EXISTS dim_customers;
            CREATE TABLE dim_customers AS
            SELECT
                customer_sk,
                customer_id,
                customer_name,
                email,
                segment,
                plan_tier,
                status,
                country,
                created_at,
                updated_at,
                dbt_valid_from,
                dbt_valid_to,
                is_current
            FROM snap_customers_scd2
            WHERE is_current = 1;

            DROP TABLE IF EXISTS dim_products;
            CREATE TABLE dim_products AS
            SELECT
                product_sk,
                product_id,
                product_name,
                category,
                unit_price,
                currency,
                is_active,
                updated_at,
                dbt_valid_from,
                dbt_valid_to,
                is_current
            FROM snap_products_scd2
            WHERE is_current = 1;
            """
        )

    def _table_exists(self, conn: sqlite3.Connection, table_name: str) -> bool:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table_name,),
        ).fetchone()
        return row is not None

    def _build_facts(
        self, conn: sqlite3.Connection, incremental_since: str | None = None
    ) -> None:
        orders_select_sql = """
            WITH payments_agg AS (
                SELECT
                    order_id,
                    COUNT(*) AS payment_count,
                    ROUND(SUM(CASE WHEN payment_status IN ('succeeded', 'captured') THEN amount_paid ELSE 0 END), 2) AS total_amount_paid,
                    ROUND(SUM(CASE WHEN payment_status IN ('succeeded', 'captured') THEN gateway_fee ELSE 0 END), 2) AS total_gateway_fee,
                    MAX(payment_method) AS payment_method,
                    MAX(payment_status) AS payment_status,
                    MAX(paid_at) AS paid_at,
                    MAX(updated_at) AS last_payment_updated_at
                FROM stg_payments
                GROUP BY order_id
            ),
            joined AS (
                SELECT
                    o.order_id,
                    c.customer_sk,
                    o.customer_id,
                    p.product_sk,
                    o.product_id,
                    o.order_date,
                    o.quantity,
                    o.gross_amount,
                    o.discount_amount,
                    o.tax_amount,
                    COALESCE(pay.total_gateway_fee, 0.0) AS gateway_fee,
                    COALESCE(pay.total_amount_paid, 0.0) AS amount_paid,
                    ROUND(
                        COALESCE(o.gross_amount, 0.0)
                        - COALESCE(o.discount_amount, 0.0)
                        - COALESCE(o.tax_amount, 0.0)
                        - COALESCE(pay.total_gateway_fee, 0.0),
                        2
                    ) AS net_revenue,
                    o.order_status,
                    o.currency,
                    COALESCE(pay.payment_count, 0) AS payment_count,
                    COALESCE(pay.payment_method, 'unknown') AS payment_method,
                    COALESCE(pay.payment_status, 'unpaid') AS payment_status,
                    pay.paid_at,
                    CASE
                        WHEN pay.last_payment_updated_at IS NOT NULL
                             AND pay.last_payment_updated_at > o.updated_at
                        THEN pay.last_payment_updated_at
                        ELSE o.updated_at
                    END AS updated_at
                FROM stg_orders o
                LEFT JOIN payments_agg pay ON o.order_id = pay.order_id
                LEFT JOIN dim_customers c ON o.customer_id = c.customer_id
                LEFT JOIN dim_products p ON o.product_id = p.product_id
            )
            SELECT * FROM joined
        """

        if incremental_since and self._table_exists(conn, "fct_orders"):
            conn.execute("DROP TABLE IF EXISTS tmp_fct_orders_delta")
            conn.execute(
                f"CREATE TEMP TABLE tmp_fct_orders_delta AS {orders_select_sql} WHERE updated_at >= ?",
                (incremental_since,),
            )
            conn.execute(
                "DELETE FROM fct_orders WHERE order_id IN (SELECT order_id FROM tmp_fct_orders_delta)"
            )
            conn.execute("INSERT INTO fct_orders SELECT * FROM tmp_fct_orders_delta")
            conn.execute("DROP TABLE IF EXISTS tmp_fct_orders_delta")
        else:
            conn.execute("DROP TABLE IF EXISTS fct_orders")
            conn.execute(f"CREATE TABLE fct_orders AS {orders_select_sql}")

        payments_select_sql = """
            SELECT
                pay.payment_id,
                pay.order_id,
                o.customer_id,
                c.customer_sk,
                o.product_id,
                p.product_sk,
                o.order_date,
                pay.payment_method,
                pay.payment_status,
                pay.amount_paid,
                pay.gateway_fee,
                ROUND(pay.amount_paid - pay.gateway_fee, 2) AS net_settled_amount,
                o.currency,
                pay.paid_at,
                pay.updated_at
            FROM stg_payments pay
            LEFT JOIN stg_orders o ON pay.order_id = o.order_id
            LEFT JOIN dim_customers c ON o.customer_id = c.customer_id
            LEFT JOIN dim_products p ON o.product_id = p.product_id
        """

        if incremental_since and self._table_exists(conn, "fct_payments"):
            conn.execute("DROP TABLE IF EXISTS tmp_fct_payments_delta")
            conn.execute(
                f"CREATE TEMP TABLE tmp_fct_payments_delta AS {payments_select_sql} WHERE pay.updated_at >= ?",
                (incremental_since,),
            )
            conn.execute(
                "DELETE FROM fct_payments WHERE payment_id IN (SELECT payment_id FROM tmp_fct_payments_delta)"
            )
            conn.execute("INSERT INTO fct_payments SELECT * FROM tmp_fct_payments_delta")
            conn.execute("DROP TABLE IF EXISTS tmp_fct_payments_delta")
        else:
            conn.execute("DROP TABLE IF EXISTS fct_payments")
            conn.execute(f"CREATE TABLE fct_payments AS {payments_select_sql}")

    def _build_mart_revenue_analytics(
        self, conn: sqlite3.Connection, incremental_since: str | None = None
    ) -> int:
        mart_select_sql = """
            SELECT
                o.order_id,
                o.order_date,
                SUBSTR(o.order_date, 1, 7) || '-01' AS order_month,
                o.order_status,
                o.currency,
                o.customer_sk,
                o.customer_id,
                c.customer_name,
                c.email AS customer_email,
                c.segment AS customer_segment,
                c.plan_tier,
                c.status AS customer_status,
                c.country AS country,
                c.country AS customer_country,
                o.product_sk,
                o.product_id,
                p.product_name,
                p.category AS category,
                p.category AS product_category,
                p.unit_price,
                o.quantity,
                o.gross_amount,
                o.gross_amount AS gross_revenue,
                o.discount_amount,
                o.tax_amount,
                o.gateway_fee,
                o.amount_paid,
                ROUND(
                    COALESCE(o.gross_amount, 0.0)
                    - COALESCE(o.discount_amount, 0.0)
                    - COALESCE(o.tax_amount, 0.0)
                    - COALESCE(o.gateway_fee, 0.0),
                    2
                ) AS net_revenue,
                o.payment_method,
                o.payment_status,
                o.paid_at,
                o.updated_at
            FROM fct_orders o
            LEFT JOIN dim_customers c ON o.customer_id = c.customer_id
            LEFT JOIN dim_products p ON o.product_id = p.product_id
        """

        if incremental_since and self._table_exists(conn, "mart_revenue_analytics"):
            conn.execute("DROP TABLE IF EXISTS tmp_mart_revenue_delta")
            conn.execute(
                f"CREATE TEMP TABLE tmp_mart_revenue_delta AS {mart_select_sql} WHERE o.updated_at >= ?",
                (incremental_since,),
            )
            upserted = conn.execute(
                "SELECT COUNT(*) FROM tmp_mart_revenue_delta"
            ).fetchone()[0]
            conn.execute(
                "DELETE FROM mart_revenue_analytics WHERE order_id IN (SELECT order_id FROM tmp_mart_revenue_delta)"
            )
            conn.execute(
                "INSERT INTO mart_revenue_analytics SELECT * FROM tmp_mart_revenue_delta"
            )
            conn.execute("DROP TABLE IF EXISTS tmp_mart_revenue_delta")
            return int(upserted)

        conn.execute("DROP TABLE IF EXISTS mart_revenue_analytics")
        conn.execute(f"CREATE TABLE mart_revenue_analytics AS {mart_select_sql}")
        total = conn.execute("SELECT COUNT(*) FROM mart_revenue_analytics").fetchone()[0]
        return int(total)

    def run_quality_tests(self) -> list[QualityTestResult]:
        """Execute automated SQLite data quality checks mirroring dbt schema and singular tests."""
        checks: list[tuple[str, str, str, str]] = [
            (
                "not_null_dim_customers_customer_id",
                "dim_customers",
                "SELECT COUNT(*) FROM dim_customers WHERE customer_id IS NULL",
                "dim_customers.customer_id must not be null",
            ),
            (
                "not_null_dim_products_product_id",
                "dim_products",
                "SELECT COUNT(*) FROM dim_products WHERE product_id IS NULL",
                "dim_products.product_id must not be null",
            ),
            (
                "not_null_fct_orders_order_id",
                "fct_orders",
                "SELECT COUNT(*) FROM fct_orders WHERE order_id IS NULL",
                "fct_orders.order_id must not be null",
            ),
            (
                "not_null_fct_payments_payment_id",
                "fct_payments",
                "SELECT COUNT(*) FROM fct_payments WHERE payment_id IS NULL",
                "fct_payments.payment_id must not be null",
            ),
            (
                "not_null_mart_revenue_analytics_order_id",
                "mart_revenue_analytics",
                "SELECT COUNT(*) FROM mart_revenue_analytics WHERE order_id IS NULL",
                "mart_revenue_analytics.order_id must not be null",
            ),
            (
                "unique_dim_customers_customer_id",
                "dim_customers",
                "SELECT COUNT(*) FROM (SELECT customer_id FROM dim_customers GROUP BY customer_id HAVING COUNT(*) > 1)",
                "dim_customers.customer_id must be unique",
            ),
            (
                "unique_dim_products_product_id",
                "dim_products",
                "SELECT COUNT(*) FROM (SELECT product_id FROM dim_products GROUP BY product_id HAVING COUNT(*) > 1)",
                "dim_products.product_id must be unique",
            ),
            (
                "unique_fct_orders_order_id",
                "fct_orders",
                "SELECT COUNT(*) FROM (SELECT order_id FROM fct_orders GROUP BY order_id HAVING COUNT(*) > 1)",
                "fct_orders.order_id must be unique",
            ),
            (
                "unique_fct_payments_payment_id",
                "fct_payments",
                "SELECT COUNT(*) FROM (SELECT payment_id FROM fct_payments GROUP BY payment_id HAVING COUNT(*) > 1)",
                "fct_payments.payment_id must be unique",
            ),
            (
                "unique_mart_revenue_analytics_order_id",
                "mart_revenue_analytics",
                "SELECT COUNT(*) FROM (SELECT order_id FROM mart_revenue_analytics GROUP BY order_id HAVING COUNT(*) > 1)",
                "mart_revenue_analytics.order_id must be unique",
            ),
            (
                "accepted_values_dim_customers_plan_tier",
                "dim_customers",
                "SELECT COUNT(*) FROM dim_customers WHERE LOWER(COALESCE(plan_tier, '')) NOT IN ('free', 'starter', 'growth', 'enterprise')",
                "dim_customers.plan_tier must be in {Free, Starter, Growth, Enterprise}",
            ),
            (
                "accepted_values_dim_customers_status",
                "dim_customers",
                "SELECT COUNT(*) FROM dim_customers WHERE LOWER(COALESCE(status, '')) NOT IN ('active', 'inactive', 'churned', 'trial')",
                "dim_customers.status must be in {active, inactive, churned, trial}",
            ),
            (
                "accepted_values_fct_orders_order_status",
                "fct_orders",
                "SELECT COUNT(*) FROM fct_orders WHERE LOWER(COALESCE(order_status, '')) NOT IN ('paid', 'completed', 'pending', 'cancelled', 'refunded')",
                "fct_orders.order_status must be in {paid, completed, pending, cancelled, refunded}",
            ),
            (
                "accepted_values_fct_payments_payment_status",
                "fct_payments",
                "SELECT COUNT(*) FROM fct_payments WHERE LOWER(COALESCE(payment_status, '')) NOT IN ('captured', 'succeeded', 'pending', 'failed', 'refunded')",
                "fct_payments.payment_status must be in {captured, succeeded, pending, failed, refunded}",
            ),
            (
                "relationships_fct_orders_customer_id",
                "fct_orders",
                """
                SELECT COUNT(*)
                FROM fct_orders o
                LEFT JOIN dim_customers c ON o.customer_id = c.customer_id
                WHERE c.customer_id IS NULL
                """,
                "fct_orders.customer_id must exist in dim_customers",
            ),
            (
                "relationships_fct_orders_product_id",
                "fct_orders",
                """
                SELECT COUNT(*)
                FROM fct_orders o
                LEFT JOIN dim_products p ON o.product_id = p.product_id
                WHERE p.product_id IS NULL
                """,
                "fct_orders.product_id must exist in dim_products",
            ),
            (
                "relationships_fct_payments_order_id",
                "fct_payments",
                """
                SELECT COUNT(*)
                FROM fct_payments pay
                LEFT JOIN fct_orders o ON pay.order_id = o.order_id
                WHERE o.order_id IS NULL
                """,
                "fct_payments.order_id must exist in fct_orders",
            ),
            (
                "scd2_single_current_row_snap_customers_scd2",
                "snap_customers_scd2",
                """
                SELECT COUNT(*) FROM (
                    SELECT customer_id
                    FROM snap_customers_scd2
                    GROUP BY customer_id
                    HAVING SUM(CASE WHEN is_current = 1 AND dbt_valid_to IS NULL THEN 1 ELSE 0 END) != 1
                )
                """,
                "Each customer in snap_customers_scd2 must have exactly one current open record",
            ),
            (
                "scd2_single_current_row_snap_products_scd2",
                "snap_products_scd2",
                """
                SELECT COUNT(*) FROM (
                    SELECT product_id
                    FROM snap_products_scd2
                    GROUP BY product_id
                    HAVING SUM(CASE WHEN is_current = 1 AND dbt_valid_to IS NULL THEN 1 ELSE 0 END) != 1
                )
                """,
                "Each product in snap_products_scd2 must have exactly one current open record",
            ),
            (
                "net_revenue_consistency_mart_revenue_analytics",
                "mart_revenue_analytics",
                """
                SELECT COUNT(*)
                FROM mart_revenue_analytics
                WHERE net_revenue < 0
                   OR ABS(
                       net_revenue - ROUND(
                           COALESCE(gross_amount, 0.0)
                           - COALESCE(discount_amount, 0.0)
                           - COALESCE(tax_amount, 0.0)
                           - COALESCE(gateway_fee, 0.0),
                           2
                       )
                   ) >= 0.01
                """,
                "mart_revenue_analytics.net_revenue must be >= 0 and equal gross - discount - tax - gateway_fee",
            ),
        ]

        results: list[QualityTestResult] = []
        with self._connect() as conn:
            for test_name, model, sql, description in checks:
                try:
                    failing_rows = int(conn.execute(sql).fetchone()[0])
                    passed = failing_rows == 0
                    details = "PASS" if passed else f"FAIL ({failing_rows} failing rows): {description}"
                except sqlite3.Error as exc:
                    failing_rows = 1
                    passed = False
                    details = f"ERROR: {exc}"

                results.append(
                    QualityTestResult(
                        test_name=test_name,
                        model=model,
                        passed=passed,
                        failing_rows=failing_rows,
                        details=details,
                    )
                )

        return results
