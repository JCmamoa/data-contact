"""Semantic Layer HTTP API exposing FastAPI app (optional) and stdlib HTTP handler."""

from __future__ import annotations

import json
import uuid
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

try:
    from fastapi import FastAPI, HTTPException, Request, Response  # type: ignore[import-not-found]
    from pydantic import BaseModel, Field  # type: ignore[import-not-found]
except ImportError:
    FastAPI = None  # type: ignore[assignment]
    HTTPException = None  # type: ignore[assignment]
    Request = None  # type: ignore[assignment]
    Response = None  # type: ignore[assignment]
    BaseModel = object  # type: ignore[assignment,misc]
    Field = None  # type: ignore[assignment]

from observability.telemetry import correlation_id_context
from semantic_layer.engine import SemanticLayerEngine
from shared_models import PlatformConfig

PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"


def _build_default_engine(config: PlatformConfig | None = None) -> SemanticLayerEngine:
    cfg = config or PlatformConfig.from_env()
    return SemanticLayerEngine(
        db_path=cfg.warehouse_sqlite_path,
        metrics_yaml_path=cfg.metrics_yaml_path,
        postgres_dsn=cfg.postgres_dsn,
    )


def dispatch_request(
    engine: SemanticLayerEngine,
    method: str,
    path: str,
    body: bytes = b"",
    headers: dict[str, str] | None = None,
) -> tuple[int, dict[str, str], bytes]:
    """Route and execute a Semantic Layer HTTP request without external framework deps."""
    clean_path = path.split("?", 1)[0]
    req_headers = {k.lower(): v for k, v in (headers or {}).items()}
    corr_id = req_headers.get("x-correlation-id") or uuid.uuid4().hex
    base_headers = {"X-Correlation-ID": corr_id}

    with correlation_id_context(corr_id):
        if method == "GET" and clean_path == "/health":
            payload = {
                "status": "ok",
                "service": "semantic-layer",
                "metrics_count": len(engine.list_metrics()),
            }
            return (
                200,
                {**base_headers, "Content-Type": "application/json"},
                json.dumps(payload).encode("utf-8"),
            )

        if method == "GET" and clean_path == "/metrics":
            prom_text = engine.metrics_registry.render_prometheus()
            return (
                200,
                {**base_headers, "Content-Type": PROMETHEUS_CONTENT_TYPE},
                prom_text.encode("utf-8"),
            )

        if method == "GET" and clean_path == "/api/v1/semantic/metrics":
            payload = {"metrics": engine.list_metrics()}
            return (
                200,
                {**base_headers, "Content-Type": "application/json"},
                json.dumps(payload).encode("utf-8"),
            )

        if method == "POST" and clean_path == "/api/v1/semantic/query":
            try:
                raw_json = body.decode("utf-8") if body else "{}"
                data = json.loads(raw_json)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                return (
                    400,
                    {**base_headers, "Content-Type": "application/json"},
                    json.dumps({"detail": f"Invalid JSON payload: {exc}"}).encode("utf-8"),
                )

            if not isinstance(data, dict) or not data.get("metric"):
                return (
                    400,
                    {**base_headers, "Content-Type": "application/json"},
                    json.dumps({"detail": "Field 'metric' is required."}).encode("utf-8"),
                )

            try:
                result = engine.query_metric(
                    metric_name=str(data["metric"]),
                    dimensions=data.get("dimensions"),
                    filters=data.get("filters"),
                    correlation_id=corr_id,
                )
            except ValueError as exc:
                return (
                    400,
                    {**base_headers, "Content-Type": "application/json"},
                    json.dumps({"detail": str(exc)}).encode("utf-8"),
                )
            except Exception as exc:
                return (
                    500,
                    {**base_headers, "Content-Type": "application/json"},
                    json.dumps({"detail": f"Query execution failed: {exc}"}).encode("utf-8"),
                )

            return (
                200,
                {**base_headers, "Content-Type": "application/json"},
                json.dumps(result.to_dict()).encode("utf-8"),
            )

        return (
            404,
            {**base_headers, "Content-Type": "application/json"},
            json.dumps({"detail": f"Route not found: {method} {clean_path}"}).encode("utf-8"),
        )


class SemanticAPIHandler(BaseHTTPRequestHandler):
    """Stdlib HTTP request handler exposing health, Prometheus metrics, and semantic endpoints."""

    engine: SemanticLayerEngine | None = None

    def _resolve_engine(self) -> SemanticLayerEngine:
        if self.engine is not None:
            return self.engine
        server_engine = getattr(self.server, "engine", None)
        if server_engine is not None:
            return server_engine
        return _build_default_engine()

    def _handle(self, method: str) -> None:
        content_length = int(self.headers.get("Content-Length", "0") or 0)
        body = self.rfile.read(content_length) if content_length > 0 else b""
        headers = {k: v for k, v in self.headers.items()}
        status, resp_headers, resp_body = dispatch_request(
            self._resolve_engine(),
            method=method,
            path=self.path,
            body=body,
            headers=headers,
        )
        self.send_response(status)
        for key, value in resp_headers.items():
            self.send_header(key, value)
        self.send_header("Content-Length", str(len(resp_body)))
        self.end_headers()
        self.wfile.write(resp_body)

    def do_GET(self) -> None:  # noqa: N802
        self._handle("GET")

    def do_POST(self) -> None:  # noqa: N802
        self._handle("POST")

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        # Suppress default stderr HTTP access logging; engine emits structured JSON logs.
        return


def create_http_server(
    host: str = "127.0.0.1",
    port: int = 8000,
    engine: SemanticLayerEngine | None = None,
    config: PlatformConfig | None = None,
) -> ThreadingHTTPServer:
    """Create a stdlib ThreadingHTTPServer bound to SemanticAPIHandler."""
    resolved_engine = engine or _build_default_engine(config)

    class _BoundHandler(SemanticAPIHandler):
        pass

    _BoundHandler.engine = resolved_engine
    server = ThreadingHTTPServer((host, port), _BoundHandler)
    return server


def create_app(
    engine: SemanticLayerEngine | None = None,
    config: PlatformConfig | None = None,
) -> Any:
    """Create a FastAPI application for the Semantic Layer when fastapi is installed."""
    if FastAPI is None:
        raise RuntimeError(
            "fastapi is not installed; use SemanticAPIHandler or create_http_server() instead."
        )

    resolved_engine = engine or _build_default_engine(config)
    app = FastAPI(title="Centralized Semantic Layer API", version="1.0.0")

    class SemanticQueryPayload(BaseModel):  # type: ignore[valid-type]
        metric: str
        dimensions: list[str] | None = None
        filters: dict[str, Any] | None = None

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "service": "semantic-layer",
            "metrics_count": len(resolved_engine.list_metrics()),
        }

    @app.get("/metrics")
    def prometheus_metrics() -> Any:
        return Response(
            content=resolved_engine.metrics_registry.render_prometheus(),
            media_type=PROMETHEUS_CONTENT_TYPE,
        )

    @app.get("/api/v1/semantic/metrics")
    def list_semantic_metrics() -> dict[str, Any]:
        return {"metrics": resolved_engine.list_metrics()}

    @app.post("/api/v1/semantic/query")
    def query_semantic_metric(payload: SemanticQueryPayload, request: Request) -> dict[str, Any]:
        corr_id = request.headers.get("x-correlation-id") or uuid.uuid4().hex
        try:
            with correlation_id_context(corr_id):
                result = resolved_engine.query_metric(
                    metric_name=payload.metric,
                    dimensions=payload.dimensions,
                    filters=payload.filters,
                    correlation_id=corr_id,
                )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return result.to_dict()

    return app


def make_handler(engine: SemanticLayerEngine) -> type[SemanticAPIHandler]:
    """Return a SemanticAPIHandler subclass bound to a specific engine instance."""
    return partial(_BoundSemanticHandler, engine)  # type: ignore[return-value]


class _BoundSemanticHandler(SemanticAPIHandler):
    def __init__(self, bound_engine: SemanticLayerEngine, *args: Any, **kwargs: Any) -> None:
        self.engine = bound_engine
        super().__init__(*args, **kwargs)
