"""Deterministic multi-month e-commerce/SaaS dataset generator (Jan-Jun 2026)."""

from __future__ import annotations

import random
from datetime import date, datetime, timedelta, timezone
from typing import Any

_CUSTOMERS_CATALOG = [
    ("CUST-001", "Nubank Labs Ltda", "billing@nubanklabs.com.br", "Starter", "BR"),
    ("CUST-002", "Mercado Sul S.A.", "finance@mercadosul.com.br", "Growth", "BR"),
    ("CUST-003", "Apex Cloud Inc", "ap@apexcloud.io", "Enterprise", "US"),
    ("CUST-004", "Northstar Retail", "ops@northstarretail.com", "Starter", "US"),
    ("CUST-005", "Berlin FinTech GmbH", "rechnung@berlinfintech.de", "Growth", "DE"),
    ("CUST-006", "Thames Analytics Ltd", "accounts@thamesanalytics.co.uk", "Starter", "UK"),
    ("CUST-007", "Paulista Logistica", "fiscal@paulistalog.com.br", "Growth", "BR"),
    ("CUST-008", "Sierra SaaS Corp", "billing@sierrasaas.com", "Enterprise", "US"),
    ("CUST-009", "Rheinland Data AG", "finance@rheinlanddata.de", "Starter", "DE"),
    ("CUST-010", "Camden Commerce UK", "pay@camdencommerce.co.uk", "Growth", "UK"),
    ("CUST-011", "Carioca Pay Ltda", "contas@cariocapay.com.br", "Starter", "BR"),
    ("CUST-012", "Pacific AI Systems", "ap@pacificai.io", "Growth", "US"),
    ("CUST-013", "Munich Robotics GmbH", "billing@munichrobotics.de", "Enterprise", "DE"),
    ("CUST-014", "Bristol Media Group", "finance@bristolmedia.co.uk", "Starter", "UK"),
    ("CUST-015", "Curitiba Digital", "fin@curitibadigital.com.br", "Starter", "BR"),
    ("CUST-016", "Austin HealthTech", "billing@austinhealthtech.com", "Growth", "US"),
    ("CUST-017", "Hamburg Freight GmbH", "ops@hamburgfreight.de", "Growth", "DE"),
    ("CUST-018", "London LegalTech", "ap@londonlegaltech.co.uk", "Enterprise", "UK"),
    ("CUST-019", "Recife EduTech", "financeiro@recifeedutech.com.br", "Starter", "BR"),
    ("CUST-020", "Boston BioData Inc", "accounts@bostonbiodata.com", "Starter", "US"),
]

_PRODUCTS_CATALOG = [
    ("PROD-001", "SKU-CORE-STR", "Core Platform Starter", "SaaS", 99.00),
    ("PROD-002", "SKU-CORE-GRW", "Core Platform Growth", "SaaS", 299.00),
    ("PROD-003", "SKU-CORE-ENT", "Core Platform Enterprise", "SaaS", 899.00),
    ("PROD-004", "SKU-ADD-GOV", "Data Contracts Governance Pack", "Add-on", 149.00),
    ("PROD-005", "SKU-ADD-SEM", "Semantic BI Connector", "Add-on", 199.00),
    ("PROD-006", "SKU-ADD-OBS", "Real-Time Observability Suite", "Add-on", 129.00),
    ("PROD-007", "SKU-SRV-ONB", "Enterprise Onboarding Package", "Services", 1500.00),
    ("PROD-008", "SKU-SRV-AUD", "Data Architecture Audit", "Services", 2400.00),
]

_COUNTRY_CURRENCY = {
    "BR": "BRL",
    "US": "USD",
    "DE": "EUR",
    "UK": "EUR",
}

_CURRENCY_MULTIPLIER = {
    "USD": 1.0,
    "EUR": 0.92,
    "BRL": 5.10,
}

_COUNTRY_TAX_RATE = {
    "BR": 0.085,
    "US": 0.07,
    "DE": 0.19,
    "UK": 0.20,
}

_PAYMENT_METHODS_BY_COUNTRY = {
    "BR": ("pix", "credit_card", "boleto"),
    "US": ("credit_card", "wire"),
    "DE": ("wire", "credit_card"),
    "UK": ("credit_card", "wire"),
}

_GATEWAY_FEE_RATE = {
    "pix": 0.009,
    "boleto": 0.015,
    "credit_card": 0.029,
    "wire": 0.005,
}


def generate_customer_snapshots(
    seed: int = 42,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Generate two deterministic customer snapshots showing plan upgrades & churn for SCD Type 2."""
    rng = random.Random(seed)
    snapshot_1: list[dict[str, Any]] = []
    for cust_id, name, email, plan_tier, country in _CUSTOMERS_CATALOG:
        snapshot_1.append(
            {
                "customer_id": cust_id,
                "name": name,
                "email": email,
                "plan_tier": plan_tier,
                "country": country,
                "status": "active",
                "updated_at": "2026-01-05T08:00:00Z",
            }
        )

    # Second snapshot (March/April 2026): deterministic upgrades + churn events for SCD Type 2
    upgrade_map = {"Starter": "Growth", "Growth": "Enterprise"}
    upgradable_indices = [
        idx for idx, row in enumerate(snapshot_1) if row["plan_tier"] in upgrade_map
    ]
    chosen_upgrades = set(rng.sample(upgradable_indices, k=min(6, len(upgradable_indices))))
    remaining_indices = [idx for idx in range(len(snapshot_1)) if idx not in chosen_upgrades]
    chosen_churns = set(rng.sample(remaining_indices, k=2))

    snapshot_2: list[dict[str, Any]] = []
    for idx, base in enumerate(snapshot_1):
        updated = dict(base)
        if idx in chosen_upgrades:
            updated["plan_tier"] = upgrade_map[base["plan_tier"]]
            updated["updated_at"] = "2026-03-15T10:30:00Z"
            snapshot_2.append(updated)
        elif idx in chosen_churns:
            updated["status"] = "churned"
            updated["updated_at"] = "2026-04-20T16:00:00Z"
            snapshot_2.append(updated)

    return snapshot_1, snapshot_2


def generate_sample_datasets(
    seed: int = 42,
    include_corrupted: bool = False,
) -> dict[str, list[dict[str, Any]]]:
    """Generate deterministic Jan-Jun 2026 datasets (`raw_customers`, `raw_products`, `raw_orders`, `raw_payments`)."""
    rng = random.Random(seed)
    snapshot_1, snapshot_2 = generate_customer_snapshots(seed=seed)

    # Combine snapshots chronologically so both multi-snapshot SCD2 and single-pass loads work cleanly
    raw_customers: list[dict[str, Any]] = sorted(
        [*snapshot_1, *snapshot_2],
        key=lambda r: (r["updated_at"], r["customer_id"]),
    )

    raw_products: list[dict[str, Any]] = [
        {
            "product_id": pid,
            "sku": sku,
            "product_name": pname,
            "category": category,
            "unit_price": float(price),
            "updated_at": "2026-01-02T00:00:00Z",
        }
        for pid, sku, pname, category, price in _PRODUCTS_CATALOG
    ]

    # Build lookup of customer evolution so orders reflect active status/country
    customer_meta = {
        row["customer_id"]: {
            "country": row["country"],
            "churned_at": None,
        }
        for row in snapshot_1
    }
    for row in snapshot_2:
        if row["status"] == "churned":
            customer_meta[row["customer_id"]]["churned_at"] = row["updated_at"][:10]

    start_date = date(2026, 1, 5)
    end_date = date(2026, 6, 28)
    total_days = (end_date - start_date).days

    raw_orders: list[dict[str, Any]] = []
    raw_payments: list[dict[str, Any]] = []

    order_seq = 1
    payment_seq = 1

    # Ensure every month Jan..Jun 2026 has steady, realistic transaction volume (25 orders/month = 150 orders)
    for month_idx in range(6):
        month_num = month_idx + 1
        month_start = date(2026, month_num, 1)
        if month_num == 1:
            month_start = start_date
        if month_num == 6:
            month_days = 28
        else:
            next_month = date(2026, month_num + 1, 1)
            month_days = (next_month - month_start).days

        for _ in range(25):
            cust_row = rng.choice(snapshot_1)
            cust_id = cust_row["customer_id"]
            country = cust_row["country"]
            churn_date = customer_meta[cust_id]["churned_at"]

            day_offset = rng.randint(0, max(0, month_days - 1))
            ord_date = month_start + timedelta(days=day_offset)
            if churn_date and ord_date.isoformat() >= churn_date:
                # Reassign to a non-churned customer so orders remain realistic
                active_candidates = [
                    r for r in snapshot_1 if customer_meta[r["customer_id"]]["churned_at"] is None
                ]
                cust_row = rng.choice(active_candidates)
                cust_id = cust_row["customer_id"]
                country = cust_row["country"]

            prod = rng.choice(raw_products)
            quantity = 1 if prod["category"] == "Services" else rng.randint(1, 5)

            currency = _COUNTRY_CURRENCY[country]
            fx = _CURRENCY_MULTIPLIER[currency]
            unit_in_ccy = round(prod["unit_price"] * fx, 2)
            gross_amount = round(unit_in_ccy * quantity, 2)

            discount_rate = rng.choice([0.0, 0.0, 0.0, 0.10, 0.15])
            discount_amount = round(gross_amount * discount_rate, 2)
            taxable = max(0.0, gross_amount - discount_amount)
            tax_amount = round(taxable * _COUNTRY_TAX_RATE[country], 2)

            status_roll = rng.random()
            if status_roll < 0.84:
                order_status = "paid"
            elif status_roll < 0.94:
                order_status = "refunded"
            else:
                order_status = "pending"

            order_id = f"ORD-2026-{order_seq:04d}"
            order_seq += 1

            raw_orders.append(
                {
                    "order_id": order_id,
                    "customer_id": cust_id,
                    "product_id": prod["product_id"],
                    "order_date": ord_date.isoformat(),
                    "quantity": quantity,
                    "gross_amount": gross_amount,
                    "discount_amount": discount_amount,
                    "tax_amount": tax_amount,
                    "order_status": order_status,
                    "currency": currency,
                }
            )

            net_total = round(gross_amount - discount_amount + tax_amount, 2)
            method = rng.choice(_PAYMENT_METHODS_BY_COUNTRY[country])
            fee_rate = _GATEWAY_FEE_RATE[method]
            paid_dt = datetime(
                ord_date.year,
                ord_date.month,
                ord_date.day,
                rng.randint(8, 20),
                rng.randint(0, 59),
                0,
                tzinfo=timezone.utc,
            )

            if order_status == "paid":
                raw_payments.append(
                    {
                        "payment_id": f"PAY-2026-{payment_seq:04d}",
                        "order_id": order_id,
                        "payment_method": method,
                        "gateway_fee": round(net_total * fee_rate, 2),
                        "paid_amount": net_total,
                        "payment_status": "captured",
                        "paid_at": paid_dt.isoformat().replace("+00:00", "Z"),
                    }
                )
                payment_seq += 1
            elif order_status == "refunded":
                raw_payments.append(
                    {
                        "payment_id": f"PAY-2026-{payment_seq:04d}",
                        "order_id": order_id,
                        "payment_method": method,
                        "gateway_fee": round(net_total * fee_rate, 2),
                        "paid_amount": net_total,
                        "payment_status": "refunded",
                        "paid_at": (paid_dt + timedelta(days=2)).isoformat().replace("+00:00", "Z"),
                    }
                )
                payment_seq += 1
            else:
                # Pending order either has a failed initial attempt or awaits capture
                if rng.random() < 0.6:
                    raw_payments.append(
                        {
                            "payment_id": f"PAY-2026-{payment_seq:04d}",
                            "order_id": order_id,
                            "payment_method": method,
                            "gateway_fee": 0.0,
                            "paid_amount": 0.0,
                            "payment_status": "failed",
                            "paid_at": paid_dt.isoformat().replace("+00:00", "Z"),
                        }
                    )
                    payment_seq += 1

    # Keep chronological ordering by order_date
    raw_orders.sort(key=lambda o: (o["order_date"], o["order_id"]))
    _ = total_days  # kept for clarity of date window

    if include_corrupted:
        raw_customers.extend(
            [
                {
                    "customer_id": None,
                    "name": "Broken Account",
                    "email": "invalid-email-without-at",
                    "plan_tier": "FreeTier",
                    "country": "FR",
                    "status": "active",
                    "updated_at": "2026-05-01T00:00:00Z",
                }
            ]
        )
        raw_products.extend(
            [
                {
                    "product_id": "PROD-BAD",
                    "sku": "SKU-BAD",
                    "product_name": "Negative Price Product",
                    "category": "Hardware",
                    "unit_price": -49.99,
                    "updated_at": "2026-05-01T00:00:00Z",
                }
            ]
        )
        raw_orders.extend(
            [
                {
                    "order_id": "ORD-BAD-001",
                    "customer_id": "CUST-001",
                    "product_id": "PROD-001",
                    "order_date": "2026/99/99",
                    "quantity": 0,
                    "gross_amount": -120.0,
                    "discount_amount": 0.0,
                    "tax_amount": 0.0,
                    "order_status": "cancelled",
                    "currency": "JPY",
                }
            ]
        )
        raw_payments.extend(
            [
                {
                    "payment_id": "PAY-BAD-001",
                    "order_id": None,
                    "payment_method": "crypto",
                    "gateway_fee": -2.5,
                    "paid_amount": -100.0,
                    "payment_status": "unknown",
                    "paid_at": "2026-05-01T12:00:00Z",
                }
            ]
        )

    return {
        "raw_customers": raw_customers,
        "raw_products": raw_products,
        "raw_orders": raw_orders,
        "raw_payments": raw_payments,
    }
