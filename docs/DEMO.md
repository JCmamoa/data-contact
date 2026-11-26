# Live Pipeline Execution Proof (`scripts/generate_showcase.py`)

- **Correlation ID:** `f3639af7c4f447afad9d3f9429262084`
- **Warehouse Quality Tests Passed:** `20 / 20`
- **SCD Type 2 New Customer Versions (Snapshot 2):** `8`
- **Incremental Mart Upserted Rows:** `100`

## 1. Shift-Left Data Contract Block & Quarantine Proof
```json
{
  "dataset": "raw_orders",
  "contract_version": "1.0.0",
  "correlation_id": "f3639af7c4f447afad9d3f9429262084_corrupted_orders",
  "total_rows": 151,
  "valid_rows_count": 150,
  "invalid_rows_count": 1,
  "blocked": true,
  "violations": [
    {
      "row_index": 150,
      "field_name": "order_date",
      "rule": "pattern",
      "severity": "CRITICAL",
      "message": "Column 'order_date' value '2026/99/99' failed pattern '^\\\\d{4}-\\\\d{2}-\\\\d{2}$'.",
      "raw_value": "2026/99/99"
    },
    {
      "row_index": 150,
      "field_name": "quantity",
      "rule": "min_value",
      "severity": "CRITICAL",
      "message": "Column 'quantity' value 0 < min 1.",
      "raw_value": 0
    },
    {
      "row_index": 150,
      "field_name": "gross_amount",
      "rule": "min_value",
      "severity": "CRITICAL",
      "message": "Column 'gross_amount' value -120.0 < min 0.0.",
      "raw_value": -120.0
    },
    {
      "row_index": 150,
      "field_name": "order_status",
      "rule": "allowed_values",
      "severity": "CRITICAL",
      "message": "Column 'order_status' value 'cancelled' not in ['paid', 'refunded', 'pending'].",
      "raw_value": "cancelled"
    },
    {
      "row_index": 150,
      "field_name": "currency",
      "rule": "allowed_values",
      "severity": "CRITICAL",
      "message": "Column 'currency' value 'JPY' not in ['BRL', 'USD', 'EUR'].",
      "raw_value": "JPY"
    }
  ],
  "quarantine_file": "/tmp/tmpbw5pgnng/quarantine/raw_orders_f3639af7c4f447afad9d3f9429262084_corrupted_orders.json",
  "alert_dispatched": true
}
```

## 2. Centralized Semantic Layer Metrics
```json
{
  "net_revenue": {
    "total_value": 263753.6,
    "unit": "currency",
    "sql": "SELECT SUM(CASE WHEN order_status = 'paid' THEN net_revenue ELSE 0 END) AS value FROM mart_revenue_analytics"
  },
  "gross_revenue": {
    "total_value": 400906.33999999997,
    "unit": "currency",
    "sql": "SELECT SUM(gross_revenue) AS value FROM mart_revenue_analytics"
  },
  "average_order_value": {
    "total_value": 2076.8,
    "unit": "currency",
    "sql": "SELECT ROUND(AVG(CASE WHEN order_status = 'paid' THEN net_revenue END), 2) AS value FROM mart_revenue_analytics"
  },
  "take_rate_pct": {
    "total_value": 1.55,
    "unit": "percent",
    "sql": "SELECT ROUND(100.0 * SUM(gateway_fee) / NULLIF(SUM(gross_revenue), 0), 2) AS value FROM mart_revenue_analytics"
  },
  "retention_rate": {
    "total_value": 90.0,
    "unit": "percent",
    "sql": "SELECT ROUND(100.0 * SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS value FROM dim_customers"
  },
  "churn_rate": {
    "total_value": 10.0,
    "unit": "percent",
    "sql": "SELECT ROUND(100.0 * SUM(CASE WHEN status = 'churned' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS value FROM dim_customers"
  }
}
```

## 3. Net Revenue by Country
```json
[
  {
    "country": "BR",
    "value": 192240.1
  },
  {
    "country": "DE",
    "value": 17500.34
  },
  {
    "country": "UK",
    "value": 20634.99
  },
  {
    "country": "US",
    "value": 33378.17
  }
]
```
