"""Observability stack: structured JSON logging, span tracing, and Prometheus metrics."""

from observability.telemetry import (
    DEFAULT_METRICS_REGISTRY,
    DEFAULT_TRACER,
    JsonFormatter,
    MetricsRegistry,
    SpanRecord,
    SpanTracer,
    correlation_id_context,
    get_correlation_id,
    get_logger,
    log_event,
    set_correlation_id,
)

__all__ = [
    "DEFAULT_METRICS_REGISTRY",
    "DEFAULT_TRACER",
    "JsonFormatter",
    "MetricsRegistry",
    "SpanRecord",
    "SpanTracer",
    "correlation_id_context",
    "get_correlation_id",
    "get_logger",
    "log_event",
    "set_correlation_id",
]
