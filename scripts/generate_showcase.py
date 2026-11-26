"""Generate portfolio showcase artifacts from real end-to-end pipeline outputs.

Produces:
- docs/assets/net_revenue_by_country.svg
- docs/assets/net_revenue_by_month.svg
- docs/DEMO.md
- docs/index.html
"""

from __future__ import annotations

import argparse
import html
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pipeline.runner import run_full_pipeline
from shared_models import PlatformConfig


def render_country_svg(rows: list[dict[str, Any]]) -> str:
    width, height = 740, 310
    left_pad, right_pad, top_pad = 120, 120, 68
    bar_w_max = width - left_pad - right_pad
    max_val = max((float(r.get("value", 0.0)) for r in rows), default=1.0) or 1.0
    colors = ["#38bdf8", "#34d399", "#818cf8", "#fbbf24"]

    bars = []
    for idx, row in enumerate(rows[:6]):
        label = html.escape(str(row.get("country", "")))
        val = float(row.get("value", 0.0))
        y = top_pad + idx * 52
        bar_w = max(10.0, (val / max_val) * bar_w_max)
        color = colors[idx % len(colors)]
        bars.append(
            f'<text x="{left_pad - 12}" y="{y + 20}" fill="#e2e8f0" font-size="13" '
            f'font-family="Inter,system-ui,sans-serif" text-anchor="end">{label}</text>'
            f'<rect x="{left_pad}" y="{y + 4}" width="{bar_w:.1f}" height="24" rx="6" fill="{color}" opacity="0.92"/>'
            f'<text x="{left_pad + bar_w + 10:.1f}" y="{y + 21}" fill="#94a3b8" font-size="12" '
            f'font-family="ui-monospace,monospace">${val:,.2f}</text>'
        )

    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" width="100%">\n'
        f'  <rect width="{width}" height="{height}" rx="14" fill="#0f172a"/>\n'
        f'  <rect x="1" y="1" width="{width - 2}" height="{height - 2}" rx="13" fill="none" stroke="#1e293b"/>\n'
        f'  <text x="24" y="34" fill="#f8fafc" font-size="16" font-weight="700" font-family="Inter,system-ui,sans-serif">'
        f'Receita Líquida por País (Camada Semântica Centralizada)</text>\n'
        f'  <text x="24" y="52" fill="#64748b" font-size="11.5" font-family="ui-monospace,monospace">'
        f'metric=net_revenue | dimensions=[country] | mart_revenue_analytics</text>\n'
        f'  {"".join(bars)}\n'
        f'</svg>\n'
    )


def render_monthly_svg(rows: list[dict[str, Any]]) -> str:
    width, height = 740, 310
    left_pad, right_pad, top_pad, bottom_pad = 84, 34, 72, 48
    plot_w = width - left_pad - right_pad
    plot_h = height - top_pad - bottom_pad
    vals = [float(r.get("value", 0.0)) for r in rows]
    min_v = min(vals, default=0.0) * 0.9
    max_v = max(vals, default=1.0) * 1.05
    span = (max_v - min_v) or 1.0
    n = max(1, len(rows) - 1)

    pts = []
    for i, r in enumerate(rows):
        x = left_pad + (i / n) * plot_w
        y = top_pad + plot_h - ((float(r["value"]) - min_v) / span) * plot_h
        pts.append((x, y, str(r.get("order_month", ""))[:7], float(r["value"])))

    poly = " ".join(f"{x:.1f},{y:.1f}" for x, y, _, _ in pts)
    grid = []
    for step in range(4):
        frac = step / 3
        gy = top_pad + frac * plot_h
        gval = max_v - frac * span
        grid.append(
            f'<line x1="{left_pad}" y1="{gy:.1f}" x2="{width - right_pad}" y2="{gy:.1f}" stroke="#1e293b" stroke-dasharray="4 4"/>'
            f'<text x="{left_pad - 10}" y="{gy + 4:.1f}" fill="#64748b" font-size="11" font-family="ui-monospace,monospace" text-anchor="end">${gval:,.0f}</text>'
        )

    dots = [
        f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" fill="#34d399"/>'
        f'<text x="{x:.1f}" y="{height - 18}" fill="#94a3b8" font-size="11" font-family="ui-monospace,monospace" text-anchor="middle">{html.escape(m)}</text>'
        for x, y, m, _ in pts
    ]

    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" width="100%">\n'
        f'  <rect width="{width}" height="{height}" rx="14" fill="#0f172a"/>\n'
        f'  <rect x="1" y="1" width="{width - 2}" height="{height - 2}" rx="13" fill="none" stroke="#1e293b"/>\n'
        f'  <text x="24" y="34" fill="#f8fafc" font-size="16" font-weight="700" font-family="Inter,system-ui,sans-serif">'
        f'Tendência Mensal de Receita Líquida (Jan–Jun 2026)</text>\n'
        f'  <text x="24" y="52" fill="#64748b" font-size="11.5" font-family="ui-monospace,monospace">'
        f'metric=net_revenue | dimensions=[order_month] | Incremental OBT</text>\n'
        f'  {"".join(grid)}\n'
        f'  <polyline fill="none" stroke="#34d399" stroke-width="3" points="{poly}"/>\n'
        f'  {"".join(dots)}\n'
        f'</svg>\n'
    )


def render_demo_md(summary: dict[str, Any]) -> str:
    metrics_overview = {
        name: {
            "total_value": payload["total_value"],
            "unit": payload["unit"],
            "sql": payload["sql"],
        }
        for name, payload in summary["semantic_metrics"].items()
    }
    return "\n".join(
        [
            "# Live Pipeline Execution Proof (`scripts/generate_showcase.py`)",
            "",
            f"- **Correlation ID:** `{summary['correlation_id']}`",
            f"- **Warehouse Quality Tests Passed:** `{summary['quality_passed_count']} / {summary['quality_total_count']}`",
            f"- **SCD Type 2 New Customer Versions (Snapshot 2):** `{summary['warehouse_incremental_run']['scd2_new_customer_versions']}`",
            f"- **Incremental Mart Upserted Rows:** `{summary['warehouse_incremental_run']['mart_upserted_rows']}`",
            "",
            "## 1. Shift-Left Data Contract Block & Quarantine Proof",
            "```json",
            json.dumps(summary["blocked_contract_demo"], indent=2, ensure_ascii=False),
            "```",
            "",
            "## 2. Centralized Semantic Layer Metrics",
            "```json",
            json.dumps(metrics_overview, indent=2, ensure_ascii=False),
            "```",
            "",
            "## 3. Net Revenue by Country",
            "```json",
            json.dumps(summary["net_revenue_by_country"]["rows"], indent=2, ensure_ascii=False),
            "```",
            "",
        ]
    )


def render_html(summary: dict[str, Any], country_svg: str, monthly_svg: str) -> str:
    embedded = json.dumps(
        {
            "correlation_id": summary["correlation_id"],
            "quality_passed": summary["quality_passed_count"],
            "quality_total": summary["quality_total_count"],
            "scd2_versions": summary["warehouse_incremental_run"]["scd2_new_customer_versions"],
            "tables": summary["warehouse_incremental_run"]["tables"],
            "blocked_demo": summary["blocked_contract_demo"],
            "semantic_metrics": summary["semantic_metrics"],
            "by_country": summary["net_revenue_by_country"]["rows"],
        },
        ensure_ascii=False,
    ).replace("</", "<\\/")

    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Data Contracts & Semantic Layer — Portfolio Showcase</title>
  <style>
    :root {{ --bg:#020617; --surf:#0f172a; --border:#334155; --text:#f8fafc; --muted:#94a3b8; --accent:#38bdf8; --green:#34d399; --red:#f87171; }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; font-family:Inter,system-ui,sans-serif; background:var(--bg); color:var(--text); line-height:1.5; }}
    .wrap {{ max-width:1160px; margin:0 auto; padding:28px 20px 60px; }}
    h1 {{ margin:0; font-size:1.8rem; }}
    .sub {{ color:var(--muted); margin:6px 0 22px; }}
    .grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(210px,1fr)); gap:14px; margin-bottom:22px; }}
    .charts {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(340px,1fr)); gap:16px; margin-bottom:22px; }}
    .card {{ background:var(--surf); border:1px solid var(--border); border-radius:12px; padding:16px; }}
    .lbl {{ font-size:.78rem; color:var(--muted); text-transform:uppercase; }}
    .val {{ font-size:1.45rem; font-weight:700; color:var(--green); margin-top:4px; }}
    pre {{ background:#020617; border:1px solid var(--border); border-radius:10px; padding:12px; overflow:auto; font-size:.82rem; color:#cbd5e1; }}
    .badge {{ display:inline-block; padding:3px 9px; border-radius:999px; font-size:.75rem; font-weight:600; background:#1e293b; border:1px solid var(--border); }}
    .badge.block {{ color:var(--red); border-color:rgba(248,113,113,.4); }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>🏛️ Plataforma Analítica com Contratos de Dados e Camada Semântica</h1>
    <div class="sub">Saídas reais geradas por <code>pipeline/runner.py</code> (Contratos YAML + dbt/SCD2 + OBT Incremental + Camada Semântica).</div>
    <div class="grid" id="kpis"></div>
    <div class="charts">
      <div class="card">{country_svg}</div>
      <div class="card">{monthly_svg}</div>
    </div>
    <div class="charts">
      <div class="card">
        <h3 style="margin-top:0">🛡️ Bloqueio por Quebra de Contrato (Quarantine + Alerta)</h3>
        <span class="badge block">BLOCKED AT INGESTION</span>
        <pre id="contract-json"></pre>
      </div>
      <div class="card">
        <h3 style="margin-top:0">📐 Métricas Governadas (Camada Semântica)</h3>
        <pre id="metrics-json"></pre>
      </div>
    </div>
  </div>
  <script>
    const D = {embedded};
    const kpis = [
      ["Qualidade (dbt/SQL)", D.quality_passed + " / " + D.quality_total, "Testes automatizados OK"],
      ["Receita Líquida Total", "$" + Number(D.semantic_metrics.net_revenue.total_value).toLocaleString(), "Métrica única governada"],
      ["Retenção / Churn", D.semantic_metrics.retention_rate.total_value + "% / " + D.semantic_metrics.churn_rate.total_value + "%", "Soma exata 100.00%"],
      ["SCD Tipo 2 (Snapshot 2)", "+" + D.scd2_versions + " versões", D.tables.snap_customers_scd2 + " linhas históricas"]
    ];
    document.getElementById("kpis").innerHTML = kpis.map(([l,v,s]) =>
      `<div class="card"><div class="lbl">${{l}}</div><div class="val">${{v}}</div><div class="lbl" style="text-transform:none;margin-top:4px">${{s}}</div></div>`
    ).join("");
    document.getElementById("contract-json").textContent = JSON.stringify(D.blocked_demo, null, 2);
    const simpleMetrics = Object.fromEntries(Object.entries(D.semantic_metrics).map(([k,v]) => [k, {{total_value: v.total_value, unit: v.unit, sql: v.sql}}]));
    document.getElementById("metrics-json").textContent = JSON.stringify(simpleMetrics, null, 2);
  </script>
</body>
</html>
"""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as tmp_dir:
        cfg = PlatformConfig(
            warehouse_sqlite_path=os.path.join(tmp_dir, "showcase.db"),
            quarantine_dir=os.path.join(tmp_dir, "quarantine"),
            contracts_dir="contracts",
            metrics_yaml_path="semantic_layer/metrics.yml",
        )
        summary = run_full_pipeline(cfg, seed=42, test_corrupted_contract=True)

    country_svg = render_country_svg(summary["net_revenue_by_country"]["rows"])
    monthly_svg = render_monthly_svg(summary["net_revenue_by_month"]["rows"])
    demo_md = render_demo_md(summary)
    index_html = render_html(summary, country_svg, monthly_svg)

    outputs = {
        ROOT_DIR / "docs" / "assets" / "net_revenue_by_country.svg": country_svg,
        ROOT_DIR / "docs" / "assets" / "net_revenue_by_month.svg": monthly_svg,
        ROOT_DIR / "docs" / "DEMO.md": demo_md,
        ROOT_DIR / "docs" / "index.html": index_html,
    }
    if args.check:
        for p in outputs:
            if not p.exists() or p.stat().st_size == 0:
                print(f"Missing showcase file: {p}")
                return 1
        print("All showcase artifacts verified.")
        return 0

    for p, content in outputs.items():
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")
        print(f"Generated {p.relative_to(ROOT_DIR)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
