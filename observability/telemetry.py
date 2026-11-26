"""Structured JSON logging, lightweight OpenTelemetry-compatible tracing, and Prometheus metrics."""

from __future__ import annotations

import contextvars
import json
import logging
import sys
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterator

try:
    from opentelemetry import trace as otel_trace  # type: ignore[import-not-found]
except ImportError:
    otel_trace = None

try:
    import prometheus_client as prom_client  # type: ignore[import-not-found]
except ImportError:
    prom_client = None


_CORRELATION_ID: contextvars.ContextVar[str] = contextvars.ContextVar("correlation_id", default="")

_STANDARD_LOG_ATTRS = frozenset(
    {
        "args",
        "asctime",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "module",
        "msecs",
        "message",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "thread",
        "threadName",
        "taskName",
        "correlation_id",
        "fields",
    }
)

DEFAULT_HISTOGRAM_BUCKETS: tuple[float, ...] = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
)


def get_correlation_id() -> str:
    """Return the active correlation ID from contextvars, or empty string if unset."""
    return _CORRELATION_ID.get()


def set_correlation_id(correlation_id: str) -> contextvars.Token[str]:
    """Set the active correlation ID in contextvars and return the reset token."""
    return _CORRELATION_ID.set(correlation_id)


@contextmanager
def correlation_id_context(correlation_id: str | None = None) -> Iterator[str]:
    """Context manager scoping a correlation ID (generating a UUID4 hex if omitted)."""
    cid = correlation_id or uuid.uuid4().hex
    token = _CORRELATION_ID.set(cid)
    try:
        yield cid
    finally:
        _CORRELATION_ID.reset(token)


class JsonFormatter(logging.Formatter):
    """Logging formatter emitting single-line structured JSON with correlation_id."""

    def format(self, record: logging.LogRecord) -> str:
        cid = getattr(record, "correlation_id", "") or get_correlation_id()
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "correlation_id": cid,
        }
        fields = getattr(record, "fields", None)
        if isinstance(fields, dict):
            payload.update(fields)
        for key, value in record.__dict__.items():
            if key not in _STANDARD_LOG_ATTRS and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, ensure_ascii=False)


def get_logger(
    name: str,
    level: str = "INFO",
    stream: Any | None = None,
) -> logging.Logger:
    """Return a configured logger using JsonFormatter without duplicating handlers."""
    logger = logging.getLogger(name)
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(numeric_level)
    logger.propagate = False

    target_stream = stream if stream is not None else sys.stdout
    if stream is not None:
        logger.handlers.clear()

    if not logger.handlers:
        handler = logging.StreamHandler(target_stream)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
    else:
        for handler in logger.handlers:
            if not isinstance(handler.formatter, JsonFormatter):
                handler.setFormatter(JsonFormatter())
    return logger


def log_event(
    logger: logging.Logger,
    level: str,
    message: str,
    correlation_id: str | None = None,
    **fields: Any,
) -> None:
    """Emit a structured log entry with explicit correlation_id and arbitrary fields."""
    log_method = getattr(logger, level.lower(), logger.info)
    cid = correlation_id if correlation_id is not None else get_correlation_id()
    log_method(message, extra={"correlation_id": cid, "fields": fields})


@dataclass
class SpanRecord:
    """Recorded execution span compatible with OpenTelemetry trace semantics."""

    name: str
    correlation_id: str
    trace_id: str
    span_id: str
    parent_span_id: str | None = None
    start_time: float = 0.0
    end_time: float = 0.0
    duration_ms: float = 0.0
    status: str = "OK"
    attributes: dict[str, Any] = field(default_factory=dict)
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "correlation_id": self.correlation_id,
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_span_id": self.parent_span_id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_ms": self.duration_ms,
            "status": self.status,
            "attributes": dict(self.attributes),
            "error": self.error,
        }


class SpanTracer:
    """Lightweight OpenTelemetry-compatible span tracer with optional OTel bridge."""

    def __init__(
        self,
        service_name: str = "data-contracts-semantic-platform",
        logger: logging.Logger | None = None,
    ) -> None:
        self.service_name = service_name
        self._logger = logger
        self._lock = threading.Lock()
        self._spans: list[SpanRecord] = []
        self._active_span: contextvars.ContextVar[SpanRecord | None] = contextvars.ContextVar(
            f"active_span_{id(self)}", default=None
        )
        self._otel_tracer = (
            otel_trace.get_tracer(service_name) if otel_trace is not None else None
        )

    @contextmanager
    def start_span(
        self,
        name: str,
        correlation_id: str | None = None,
        **attrs: Any,
    ) -> Iterator[SpanRecord]:
        """Record a timed span, bridging to OpenTelemetry when installed."""
        parent = self._active_span.get()
        cid = correlation_id or (parent.correlation_id if parent else "") or get_correlation_id() or uuid.uuid4().hex
        trace_id = parent.trace_id if parent else uuid.uuid4().hex
        span = SpanRecord(
            name=name,
            correlation_id=cid,
            trace_id=trace_id,
            span_id=uuid.uuid4().hex[:16],
            parent_span_id=parent.span_id if parent else None,
            start_time=time.time(),
            attributes={"service.name": self.service_name, **attrs},
        )
        start_perf = time.perf_counter()
        span_token = self._active_span.set(span)

        otel_cm = (
            self._otel_tracer.start_as_current_span(name)
            if self._otel_tracer is not None
            else None
        )
        otel_span = otel_cm.__enter__() if otel_cm is not None else None
        if otel_span is not None:
            for k, v in span.attributes.items():
                otel_span.set_attribute(k, str(v))
            otel_span.set_attribute("correlation_id", cid)

        exc_info: tuple[Any, Any, Any] = (None, None, None)
        try:
            yield span
        except BaseException as exc:
            span.status = "ERROR"
            span.error = f"{type(exc).__name__}: {exc}"
            if otel_span is not None:
                otel_span.record_exception(exc)
            exc_info = sys.exc_info()
            raise
        finally:
            span.end_time = time.time()
            span.duration_ms = round((time.perf_counter() - start_perf) * 1000.0, 4)
            with self._lock:
                self._spans.append(span)
            self._active_span.reset(span_token)
            if otel_cm is not None:
                otel_cm.__exit__(*exc_info)
            if self._logger is not None:
                log_event(
                    self._logger,
                    "ERROR" if span.status == "ERROR" else "INFO",
                    f"span:{span.name}",
                    correlation_id=span.correlation_id,
                    trace_id=span.trace_id,
                    span_id=span.span_id,
                    duration_ms=span.duration_ms,
                    status=span.status,
                    **span.attributes,
                )

    def get_spans(self) -> list[SpanRecord]:
        """Return a snapshot list of recorded spans."""
        with self._lock:
            return list(self._spans)

    def clear(self) -> None:
        """Clear all recorded spans."""
        with self._lock:
            self._spans.clear()


def _normalize_labels(labels: dict[str, Any] | None) -> tuple[tuple[str, str], ...]:
    if not labels:
        return ()
    return tuple(sorted((str(k), str(v)) for k, v in labels.items()))


def _format_labels(label_tuple: tuple[tuple[str, str], ...], extra: tuple[str, str] | None = None) -> str:
    items = list(label_tuple)
    if extra is not None:
        items.append(extra)
    if not items:
        return ""
    parts: list[str] = []
    for k, v in items:
        escaped = v.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')
        parts.append(f'{k}="{escaped}"')
    return "{" + ",".join(parts) + "}"


def _format_number(value: float | int) -> str:
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    if float(value).is_integer():
        return f"{float(value):.1f}" if value == 0 else str(value)
    return f"{float(value):.6g}"


@dataclass
class _CounterMetric:
    name: str
    help_text: str
    values: dict[tuple[tuple[str, str], ...], float] = field(default_factory=dict)


@dataclass
class _GaugeMetric:
    name: str
    help_text: str
    values: dict[tuple[tuple[str, str], ...], float] = field(default_factory=dict)


@dataclass
class _HistogramSample:
    bucket_counts: list[int]
    total_count: int = 0
    total_sum: float = 0.0


@dataclass
class _HistogramMetric:
    name: str
    help_text: str
    buckets: tuple[float, ...]
    series: dict[tuple[tuple[str, str], ...], _HistogramSample] = field(default_factory=dict)


class MetricsRegistry:
    """Thread-safe Prometheus-compatible metrics registry with zero mandatory dependencies."""

    def __init__(self, enable_prometheus_client: bool = False) -> None:
        self._lock = threading.Lock()
        self._counters: dict[str, _CounterMetric] = {}
        self._gauges: dict[str, _GaugeMetric] = {}
        self._histograms: dict[str, _HistogramMetric] = {}
        self._prom_counters: dict[str, Any] = {}
        self._prom_histograms: dict[str, Any] = {}
        self._use_prom_client = bool(enable_prometheus_client and prom_client is not None)

        self.register_counter(
            "pipeline_runs_total",
            "Total number of data pipeline runs partitioned by dataset and status.",
        )
        self.register_counter(
            "contract_violations_total",
            "Total number of data contract violations partitioned by dataset, severity, and rule.",
        )
        self.register_counter(
            "semantic_queries_total",
            "Total number of semantic layer metric queries executed.",
        )
        self.register_histogram(
            "semantic_query_duration_seconds",
            "Duration of semantic layer metric queries in seconds.",
            buckets=DEFAULT_HISTOGRAM_BUCKETS,
        )

    def register_counter(self, name: str, help_text: str) -> None:
        """Register a counter metric if not already present."""
        with self._lock:
            if name not in self._counters:
                self._counters[name] = _CounterMetric(name=name, help_text=help_text)

    def register_gauge(self, name: str, help_text: str) -> None:
        """Register a gauge metric if not already present."""
        with self._lock:
            if name not in self._gauges:
                self._gauges[name] = _GaugeMetric(name=name, help_text=help_text)

    def register_histogram(
        self,
        name: str,
        help_text: str,
        buckets: tuple[float, ...] = DEFAULT_HISTOGRAM_BUCKETS,
    ) -> None:
        """Register a histogram metric if not already present."""
        sorted_buckets = tuple(sorted(set(float(b) for b in buckets)))
        with self._lock:
            if name not in self._histograms:
                self._histograms[name] = _HistogramMetric(
                    name=name,
                    help_text=help_text,
                    buckets=sorted_buckets,
                )

    def inc_counter(
        self,
        name: str,
        value: float = 1.0,
        labels: dict[str, Any] | None = None,
    ) -> float:
        """Increment a counter by value (must be non-negative) and return the new total."""
        if value < 0:
            raise ValueError("Counter increment value must be non-negative.")
        key = _normalize_labels(labels)
        with self._lock:
            metric = self._counters.get(name)
            if metric is None:
                metric = _CounterMetric(name=name, help_text=f"Counter {name}")
                self._counters[name] = metric
            metric.values[key] = metric.values.get(key, 0.0) + float(value)
            new_val = metric.values[key]
        if self._use_prom_client and prom_client is not None:
            self._bridge_prom_counter(name, metric.help_text, key, float(value))
        return new_val

    def set_gauge(
        self,
        name: str,
        value: float,
        labels: dict[str, Any] | None = None,
    ) -> float:
        """Set a gauge to the specified value."""
        key = _normalize_labels(labels)
        with self._lock:
            metric = self._gauges.get(name)
            if metric is None:
                metric = _GaugeMetric(name=name, help_text=f"Gauge {name}")
                self._gauges[name] = metric
            metric.values[key] = float(value)
            return metric.values[key]

    def observe_histogram(
        self,
        name: str,
        value: float,
        labels: dict[str, Any] | None = None,
    ) -> None:
        """Observe a value in the specified histogram."""
        key = _normalize_labels(labels)
        val = float(value)
        with self._lock:
            metric = self._histograms.get(name)
            if metric is None:
                metric = _HistogramMetric(
                    name=name,
                    help_text=f"Histogram {name}",
                    buckets=DEFAULT_HISTOGRAM_BUCKETS,
                )
                self._histograms[name] = metric
            sample = metric.series.get(key)
            if sample is None:
                sample = _HistogramSample(bucket_counts=[0] * len(metric.buckets))
                metric.series[key] = sample
            for idx, bound in enumerate(metric.buckets):
                if val <= bound:
                    sample.bucket_counts[idx] += 1
            sample.total_count += 1
            sample.total_sum += val
        if self._use_prom_client and prom_client is not None:
            self._bridge_prom_histogram(name, metric.help_text, metric.buckets, key, val)

    def _bridge_prom_counter(
        self,
        name: str,
        help_text: str,
        key: tuple[tuple[str, str], ...],
        value: float,
    ) -> None:
        label_names = [k for k, _ in key]
        if name not in self._prom_counters:
            try:
                self._prom_counters[name] = prom_client.Counter(name, help_text, label_names)
            except ValueError:
                return
        c = self._prom_counters[name]
        (c.labels(**dict(key)) if key else c).inc(value)

    def _bridge_prom_histogram(
        self,
        name: str,
        help_text: str,
        buckets: tuple[float, ...],
        key: tuple[tuple[str, str], ...],
        value: float,
    ) -> None:
        label_names = [k for k, _ in key]
        if name not in self._prom_histograms:
            try:
                self._prom_histograms[name] = prom_client.Histogram(
                    name, help_text, label_names, buckets=buckets
                )
            except ValueError:
                return
        h = self._prom_histograms[name]
        (h.labels(**dict(key)) if key else h).observe(value)

    def get_counter_value(self, name: str, labels: dict[str, Any] | None = None) -> float:
        """Read the current value of a counter for the given label set."""
        key = _normalize_labels(labels)
        with self._lock:
            metric = self._counters.get(name)
            if metric is None:
                return 0.0
            return metric.values.get(key, 0.0)

    def get_histogram_count(self, name: str, labels: dict[str, Any] | None = None) -> int:
        """Read the observation count of a histogram for the given label set."""
        key = _normalize_labels(labels)
        with self._lock:
            metric = self._histograms.get(name)
            if metric is None:
                return 0
            sample = metric.series.get(key)
            return sample.total_count if sample is not None else 0

    def render_prometheus(self) -> str:
        """Render all registered metrics in standard Prometheus text exposition format."""
        lines: list[str] = []
        with self._lock:
            for name in sorted(self._counters):
                metric = self._counters[name]
                lines.append(f"# HELP {metric.name} {metric.help_text}")
                lines.append(f"# TYPE {metric.name} counter")
                for key in sorted(metric.values):
                    lines.append(f"{metric.name}{_format_labels(key)} {_format_number(metric.values[key])}")

            for name in sorted(self._gauges):
                metric = self._gauges[name]
                lines.append(f"# HELP {metric.name} {metric.help_text}")
                lines.append(f"# TYPE {metric.name} gauge")
                for key in sorted(metric.values):
                    lines.append(f"{metric.name}{_format_labels(key)} {_format_number(metric.values[key])}")

            for name in sorted(self._histograms):
                metric = self._histograms[name]
                lines.append(f"# HELP {metric.name} {metric.help_text}")
                lines.append(f"# TYPE {metric.name} histogram")
                for key in sorted(metric.series):
                    sample = metric.series[key]
                    for idx, bound in enumerate(metric.buckets):
                        label_str = _format_labels(key, extra=("le", _format_number(bound)))
                        lines.append(f"{metric.name}_bucket{label_str} {sample.bucket_counts[idx]}")
                    inf_str = _format_labels(key, extra=("le", "+Inf"))
                    lines.append(f"{metric.name}_bucket{inf_str} {sample.total_count}")
                    base_labels = _format_labels(key)
                    lines.append(f"{metric.name}_sum{base_labels} {_format_number(sample.total_sum)}")
                    lines.append(f"{metric.name}_count{base_labels} {sample.total_count}")

        return "\n".join(lines) + ("\n" if lines else "")

    def reset(self) -> None:
        """Reset all recorded metric samples while keeping registrations."""
        with self._lock:
            for counter in self._counters.values():
                counter.values.clear()
            for gauge in self._gauges.values():
                gauge.values.clear()
            for histogram in self._histograms.values():
                histogram.series.clear()


DEFAULT_METRICS_REGISTRY = MetricsRegistry()
DEFAULT_TRACER = SpanTracer()
