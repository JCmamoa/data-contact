"""Centralized Semantic Layer query engine with allowlist validation, caching, and telemetry."""

from __future__ import annotations

import logging
import re
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import yaml  # type: ignore[import-not-found]
except ImportError:
    yaml = None

from observability.telemetry import (
    DEFAULT_METRICS_REGISTRY,
    DEFAULT_TRACER,
    MetricsRegistry,
    SpanTracer,
    get_correlation_id,
    get_logger,
    log_event,
)
from shared_models import SemanticMetricResult

_SAFE_IDENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


@dataclass(frozen=True)
class MetricDefinition:
    """Single metric definition parsed from semantic_layer/metrics.yml."""

    name: str
    label_pt: str
    label_en: str
    description: str
    unit: str
    model: str
    expression: str
    allowed_dimensions: tuple[str, ...]

    def to_metadata_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "label_pt": self.label_pt,
            "label_en": self.label_en,
            "description": self.description,
            "unit": self.unit,
            "model": self.model,
            "expression": self.expression,
            "allowed_dimensions": list(self.allowed_dimensions),
        }


def _strip_yaml_comment(line: str) -> str:
    """Strip inline YAML comments outside single or double quotes."""
    in_single = False
    in_double = False
    for idx, ch in enumerate(line):
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif ch == "#" and not in_single and not in_double:
            if idx == 0 or line[idx - 1].isspace():
                return line[:idx].rstrip()
    return line.rstrip()


def _parse_yaml_scalar(raw: str) -> Any:
    """Parse a scalar or inline list value from the built-in YAML subset parser."""
    value = raw.strip()
    if not value or value in ("null", "Null", "NULL", "~"):
        return None
    if (value.startswith('"') and value.endswith('"')) or (
        value.startswith("'") and value.endswith("'")
    ):
        return value[1:-1]
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1].strip()
        if not inner:
            return []
        return [_parse_yaml_scalar(part) for part in inner.split(",")]
    if value in ("true", "True"):
        return True
    if value in ("false", "False"):
        return False
    if re.fullmatch(r"-?\d+", value):
        return int(value)
    if re.fullmatch(r"-?\d+\.\d+", value):
        return float(value)
    return value


def _split_yaml_key_value(content: str) -> tuple[str, str]:
    """Split a YAML `key: value` line on the first unquoted colon."""
    in_single = False
    in_double = False
    for idx, ch in enumerate(content):
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif ch == ":" and not in_single and not in_double:
            if idx + 1 == len(content) or content[idx + 1].isspace():
                return content[:idx].strip(), content[idx + 1 :].strip()
    raise ValueError(f"Invalid YAML mapping line: {content!r}")


def parse_yaml_subset(text: str) -> dict[str, Any]:
    """Parse a clean YAML subset (mappings, lists, scalars) with zero external deps."""
    tokens: list[tuple[int, str]] = []
    for raw_line in text.splitlines():
        cleaned = _strip_yaml_comment(raw_line)
        if not cleaned.strip():
            continue
        indent = len(cleaned) - len(cleaned.lstrip(" "))
        tokens.append((indent, cleaned.lstrip(" ")))

    if not tokens:
        return {}

    def parse_block(index: int, min_indent: int) -> tuple[Any, int]:
        if tokens[index][1].startswith("- "):
            return parse_list(index, tokens[index][0])
        return parse_dict(index, tokens[index][0])

    def parse_list(index: int, list_indent: int) -> tuple[list[Any], int]:
        items: list[Any] = []
        while index < len(tokens):
            indent, content = tokens[index]
            if indent < list_indent or not content.startswith("- "):
                break
            if indent > list_indent:
                break
            rest = content[2:].strip()
            if not rest:
                if index + 1 < len(tokens) and tokens[index + 1][0] > indent:
                    nested, index = parse_block(index + 1, tokens[index + 1][0])
                    items.append(nested)
                else:
                    items.append(None)
                    index += 1
            elif ":" in rest and not (
                rest.startswith('"') or rest.startswith("'")
            ):
                k, v = _split_yaml_key_value(rest)
                obj: dict[str, Any] = {k: _parse_yaml_scalar(v) if v else None}
                index += 1
                if index < len(tokens) and tokens[index][0] > indent:
                    extra, index = parse_dict(index, tokens[index][0])
                    obj.update(extra)
                items.append(obj)
            else:
                items.append(_parse_yaml_scalar(rest))
                index += 1
        return items, index

    def parse_dict(index: int, dict_indent: int) -> tuple[dict[str, Any], int]:
        mapping: dict[str, Any] = {}
        while index < len(tokens):
            indent, content = tokens[index]
            if indent < dict_indent or content.startswith("- "):
                break
            if indent > dict_indent:
                break
            key, rest = _split_yaml_key_value(content)
            key = str(_parse_yaml_scalar(key)) if (key.startswith('"') or key.startswith("'")) else key
            index += 1
            if rest:
                mapping[key] = _parse_yaml_scalar(rest)
            elif index < len(tokens) and tokens[index][0] > indent:
                nested, index = parse_block(index, tokens[index][0])
                mapping[key] = nested
            elif index < len(tokens) and tokens[index][0] == indent and tokens[index][1].startswith("- "):
                nested, index = parse_list(index, indent)
                mapping[key] = nested
            else:
                mapping[key] = {}
        return mapping, index

    parsed, _ = parse_block(0, tokens[0][0])
    if not isinstance(parsed, dict):
        raise ValueError("Top-level metrics YAML document must be a mapping.")
    return parsed


def load_yaml_file(path: Path) -> dict[str, Any]:
    """Load a YAML file using PyYAML when installed, or built-in subset parser."""
    content = path.read_text(encoding="utf-8")
    if yaml is not None:
        loaded = yaml.safe_load(content)
        if not isinstance(loaded, dict):
            raise ValueError(f"Expected mapping in {path}, got {type(loaded).__name__}")
        return loaded
    return parse_yaml_subset(content)


def _resolve_metrics_path(metrics_yaml_path: str) -> Path:
    candidate = Path(metrics_yaml_path)
    if candidate.exists():
        return candidate
    repo_relative = Path(__file__).resolve().parent.parent / metrics_yaml_path
    if repo_relative.exists():
        return repo_relative
    raise FileNotFoundError(f"Metrics catalog YAML not found: {metrics_yaml_path}")


class SemanticLayerEngine:
    """Centralized Semantic Layer engine enforcing metric definitions and dimension allowlists."""

    def __init__(
        self,
        db_path: str,
        metrics_yaml_path: str = "semantic_layer/metrics.yml",
        metrics_registry: MetricsRegistry | None = None,
        tracer: SpanTracer | None = None,
        logger: logging.Logger | None = None,
        postgres_dsn: str = "",
    ) -> None:
        self.db_path = db_path
        self.postgres_dsn = postgres_dsn
        self.metrics_yaml_path = str(_resolve_metrics_path(metrics_yaml_path))
        self.metrics_registry = metrics_registry or DEFAULT_METRICS_REGISTRY
        self.tracer = tracer or DEFAULT_TRACER
        self.logger = logger or get_logger("semantic_layer.engine")

        self._lock = threading.Lock()
        self._cache: dict[tuple[Any, ...], SemanticMetricResult] = {}

        catalog = load_yaml_file(Path(self.metrics_yaml_path))
        raw_global_dims = catalog.get("allowed_dimensions") or []
        self.allowed_dimensions: tuple[str, ...] = tuple(str(d) for d in raw_global_dims)
        self._global_dimension_set = frozenset(self.allowed_dimensions)

        for dim in self.allowed_dimensions:
            if not _SAFE_IDENT_RE.match(dim):
                raise ValueError(f"Unsafe global dimension identifier in catalog: {dim!r}")

        raw_metrics = catalog.get("metrics") or {}
        if not isinstance(raw_metrics, dict) or not raw_metrics:
            raise ValueError("metrics.yml must define a non-empty 'metrics' mapping.")

        self._metrics: dict[str, MetricDefinition] = {}
        for name, spec in raw_metrics.items():
            if not isinstance(spec, dict):
                raise ValueError(f"Invalid metric definition for {name!r}")
            model = str(spec["model"])
            if not _SAFE_IDENT_RE.match(model):
                raise ValueError(f"Unsafe model identifier {model!r} for metric {name!r}")
            metric_dims = tuple(
                str(d) for d in (spec.get("allowed_dimensions") or self.allowed_dimensions)
            )
            for dim in metric_dims:
                if dim not in self._global_dimension_set or not _SAFE_IDENT_RE.match(dim):
                    raise ValueError(
                        f"Metric {name!r} references disallowed dimension {dim!r}"
                    )
            self._metrics[str(name)] = MetricDefinition(
                name=str(name),
                label_pt=str(spec.get("label_pt", name)),
                label_en=str(spec.get("label_en", name)),
                description=str(spec.get("description", "")),
                unit=str(spec.get("unit", "currency")),
                model=model,
                expression=str(spec["expression"]),
                allowed_dimensions=metric_dims,
            )

    def list_metrics(self) -> list[dict[str, Any]]:
        """Return metadata and allowed dimensions for all registered metrics."""
        return [metric.to_metadata_dict() for metric in self._metrics.values()]

    def clear_cache(self) -> None:
        """Clear the in-memory deterministic query result cache."""
        with self._lock:
            self._cache.clear()

    def _validate_request(
        self,
        metric_name: str,
        dimensions: list[str] | None,
        filters: dict[str, Any] | None,
    ) -> tuple[MetricDefinition, tuple[str, ...], dict[str, Any]]:
        metric = self._metrics.get(metric_name)
        if metric is None:
            raise ValueError(
                f"Unknown metric {metric_name!r}. Available metrics: {sorted(self._metrics)}"
            )

        metric_dim_set = frozenset(metric.allowed_dimensions)
        validated_dims: list[str] = []
        if dimensions is not None:
            if not isinstance(dimensions, (list, tuple)):
                raise ValueError("dimensions must be a list of dimension names.")
            seen: set[str] = set()
            for dim in dimensions:
                if not isinstance(dim, str) or not _SAFE_IDENT_RE.match(dim):
                    raise ValueError(f"Invalid dimension name: {dim!r}")
                if dim not in self._global_dimension_set or dim not in metric_dim_set:
                    raise ValueError(
                        f"Disallowed dimension {dim!r} for metric {metric_name!r}. "
                        f"Allowed: {list(metric.allowed_dimensions)}"
                    )
                if dim not in seen:
                    seen.add(dim)
                    validated_dims.append(dim)

        validated_filters: dict[str, Any] = {}
        if filters is not None:
            if not isinstance(filters, dict):
                raise ValueError("filters must be a mapping of dimension -> value.")
            for key, value in filters.items():
                if not isinstance(key, str) or not _SAFE_IDENT_RE.match(key):
                    raise ValueError(f"Invalid filter dimension name: {key!r}")
                if key not in self._global_dimension_set or key not in metric_dim_set:
                    raise ValueError(
                        f"Disallowed filter dimension {key!r} for metric {metric_name!r}. "
                        f"Allowed: {list(metric.allowed_dimensions)}"
                    )
                if isinstance(value, (list, tuple)):
                    for item in value:
                        if not isinstance(item, (str, int, float, bool)):
                            raise ValueError(f"Unsupported filter list value for {key!r}: {item!r}")
                    validated_filters[key] = tuple(value)
                elif isinstance(value, (str, int, float, bool)):
                    validated_filters[key] = value
                else:
                    raise ValueError(f"Unsupported filter value for {key!r}: {value!r}")

        return metric, tuple(validated_dims), validated_filters

    def _build_where_clause(
        self, filters: dict[str, Any], placeholder: str
    ) -> tuple[str, list[Any]]:
        if not filters:
            return "", []
        clauses: list[str] = []
        params: list[Any] = []
        for key in sorted(filters):
            value = filters[key]
            if isinstance(value, tuple):
                if not value:
                    clauses.append("1 = 0")
                else:
                    marks = ", ".join(placeholder for _ in value)
                    clauses.append(f"{key} IN ({marks})")
                    params.extend(value)
            else:
                clauses.append(f"{key} = {placeholder}")
                params.append(value)
        return " WHERE " + " AND ".join(clauses), params

    def _build_sql(
        self,
        metric: MetricDefinition,
        dimensions: tuple[str, ...],
        filters: dict[str, Any],
    ) -> tuple[str, str, list[Any]]:
        placeholder = "%s" if self.postgres_dsn else "?"
        where_sql, params = self._build_where_clause(filters, placeholder)
        total_sql = f"SELECT {metric.expression} AS value FROM {metric.model}{where_sql}"
        if not dimensions:
            return total_sql, total_sql, params
        dim_cols = ", ".join(dimensions)
        order_cols = ", ".join(f"{dim} ASC" for dim in dimensions)
        grouped_sql = (
            f"SELECT {dim_cols}, {metric.expression} AS value "
            f"FROM {metric.model}{where_sql} "
            f"GROUP BY {dim_cols} "
            f"ORDER BY {order_cols}"
        )
        return grouped_sql, total_sql, params

    def _execute_query(self, sql: str, params: list[Any]) -> list[dict[str, Any]]:
        if self.postgres_dsn:
            return self._execute_postgres(sql, params)
        return self._execute_sqlite(sql, params)

    def _execute_sqlite(self, sql: str, params: list[Any]) -> list[dict[str, Any]]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.execute(sql, params)
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def _execute_postgres(self, sql: str, params: list[Any]) -> list[dict[str, Any]]:
        pg_mod = None
        for mod_name in ("psycopg", "psycopg2"):
            try:
                pg_mod = __import__(mod_name)
                break
            except ImportError:
                continue
        if pg_mod is None:
            raise RuntimeError(
                "postgres_dsn configured but neither 'psycopg' nor 'psycopg2' is installed."
            )
        conn = pg_mod.connect(self.postgres_dsn)
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                col_names = [desc[0] for desc in cur.description]
                return [dict(zip(col_names, row)) for row in cur.fetchall()]
        finally:
            conn.close()

    @staticmethod
    def _coerce_numeric(val: Any) -> float | None:
        if val is None:
            return None
        return float(val)

    @staticmethod
    def _copy_result(result: SemanticMetricResult, cached: bool) -> SemanticMetricResult:
        return SemanticMetricResult(
            metric_name=result.metric_name,
            label_pt=result.label_pt,
            label_en=result.label_en,
            sql=result.sql,
            dimensions=list(result.dimensions),
            rows=[dict(row) for row in result.rows],
            total_value=result.total_value,
            unit=result.unit,
            cached=cached,
        )

    def query_metric(
        self,
        metric_name: str,
        dimensions: list[str] | None = None,
        filters: dict[str, Any] | None = None,
        correlation_id: str | None = None,
    ) -> SemanticMetricResult:
        """Validate, build deterministic SQL, execute, cache, and observe a metric query."""
        cid = correlation_id or get_correlation_id()
        start_perf = time.perf_counter()
        cached_hit = False

        with self.tracer.start_span(
            "semantic_layer.query_metric",
            correlation_id=cid or None,
            metric=metric_name,
        ) as span:
            try:
                metric, dims, clean_filters = self._validate_request(
                    metric_name, dimensions, filters
                )
                span.attributes["dimensions"] = ",".join(dims)
                span.attributes["filter_keys"] = ",".join(sorted(clean_filters))

                cache_key = (
                    metric.name,
                    dims,
                    tuple(sorted(clean_filters.items())),
                )
                with self._lock:
                    cached_entry = self._cache.get(cache_key)

                if cached_entry is not None:
                    cached_hit = True
                    span.attributes["cached"] = True
                    result = self._copy_result(cached_entry, cached=True)
                else:
                    span.attributes["cached"] = False
                    grouped_sql, total_sql, params = self._build_sql(
                        metric, dims, clean_filters
                    )
                    raw_rows = self._execute_query(grouped_sql, params)
                    normalized_rows: list[dict[str, Any]] = []
                    for row in raw_rows:
                        item = dict(row)
                        item["value"] = self._coerce_numeric(item.get("value"))
                        normalized_rows.append(item)

                    if not dims:
                        total_value = (
                            normalized_rows[0]["value"] if normalized_rows else None
                        )
                    else:
                        total_rows = self._execute_query(total_sql, params)
                        total_value = (
                            self._coerce_numeric(total_rows[0].get("value"))
                            if total_rows
                            else None
                        )

                    fresh_result = SemanticMetricResult(
                        metric_name=metric.name,
                        label_pt=metric.label_pt,
                        label_en=metric.label_en,
                        sql=grouped_sql,
                        dimensions=list(dims),
                        rows=normalized_rows,
                        total_value=total_value,
                        unit=metric.unit,
                        cached=False,
                    )
                    with self._lock:
                        self._cache[cache_key] = self._copy_result(fresh_result, cached=False)
                    result = fresh_result

                elapsed_sec = time.perf_counter() - start_perf
                self.metrics_registry.inc_counter(
                    "semantic_queries_total",
                    labels={
                        "metric": metric_name,
                        "status": "ok",
                        "cached": "true" if cached_hit else "false",
                    },
                )
                self.metrics_registry.observe_histogram(
                    "semantic_query_duration_seconds",
                    elapsed_sec,
                    labels={"metric": metric_name},
                )
                log_event(
                    self.logger,
                    "INFO",
                    "semantic_query_executed",
                    correlation_id=span.correlation_id,
                    metric=metric_name,
                    dimensions=list(dims),
                    cached=cached_hit,
                    row_count=len(result.rows),
                    duration_ms=round(elapsed_sec * 1000.0, 4),
                )
                return result
            except Exception as exc:
                elapsed_sec = time.perf_counter() - start_perf
                self.metrics_registry.inc_counter(
                    "semantic_queries_total",
                    labels={
                        "metric": metric_name,
                        "status": "error",
                        "cached": "false",
                    },
                )
                self.metrics_registry.observe_histogram(
                    "semantic_query_duration_seconds",
                    elapsed_sec,
                    labels={"metric": metric_name},
                )
                log_event(
                    self.logger,
                    "ERROR",
                    "semantic_query_failed",
                    correlation_id=span.correlation_id,
                    metric=metric_name,
                    error=str(exc),
                )
                raise
