"""Shared configuration and dataclasses across ingestion, contracts, warehouse, semantic layer, and observability."""

from __future__ import annotations

import os
from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class PlatformConfig:
    """Runtime settings for local and containerized execution."""

    project_name: str = "Data Contracts & Semantic Platform"
    environment: str = "development"
    warehouse_sqlite_path: str = "data/warehouse.db"
    quarantine_dir: str = "data/quarantine"
    contracts_dir: str = "contracts"
    metrics_yaml_path: str = "semantic_layer/metrics.yml"
    postgres_dsn: str = ""
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "PlatformConfig":
        return cls(
            project_name=os.getenv("PROJECT_NAME", "Data Contracts & Semantic Platform"),
            environment=os.getenv("ENVIRONMENT", "development"),
            warehouse_sqlite_path=os.getenv("WAREHOUSE_SQLITE_PATH", "data/warehouse.db"),
            quarantine_dir=os.getenv("QUARANTINE_DIR", "data/quarantine"),
            contracts_dir=os.getenv("CONTRACTS_DIR", "contracts"),
            metrics_yaml_path=os.getenv("METRICS_YAML_PATH", "semantic_layer/metrics.yml"),
            postgres_dsn=os.getenv("POSTGRES_DSN", ""),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )


@dataclass
class ContractViolation:
    """Single data contract violation for an incoming record."""

    row_index: int
    field_name: str
    rule: str
    severity: str  # "CRITICAL" | "WARNING"
    message: str
    raw_value: Any = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ContractValidationReport:
    """Outcome of validating a batch against a YAML data contract."""

    dataset: str
    contract_version: str
    correlation_id: str
    total_rows: int
    valid_rows_count: int
    invalid_rows_count: int
    blocked: bool
    violations: list[ContractViolation] = field(default_factory=list)
    quarantine_file: str = ""
    alert_dispatched: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "contract_version": self.contract_version,
            "correlation_id": self.correlation_id,
            "total_rows": self.total_rows,
            "valid_rows_count": self.valid_rows_count,
            "invalid_rows_count": self.invalid_rows_count,
            "blocked": self.blocked,
            "violations": [v.to_dict() for v in self.violations],
            "quarantine_file": self.quarantine_file,
            "alert_dispatched": self.alert_dispatched,
        }


@dataclass
class QualityTestResult:
    """Outcome of a single warehouse/dbt data quality check."""

    test_name: str
    model: str
    passed: bool
    failing_rows: int
    details: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class SemanticMetricResult:
    """Standardized response returned by the centralized Semantic Layer."""

    metric_name: str
    label_pt: str
    label_en: str
    sql: str
    dimensions: list[str]
    rows: list[dict[str, Any]]
    total_value: float | None = None
    unit: str = "currency"
    cached: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
