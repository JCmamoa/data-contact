"""PySpark Data Contract ingestion mirror for Databricks / AWS EMR environments.

This module mirrors `ingestion.contracts.DataContractValidator` using native
PySpark `StructType` schemas and Column expressions (`F.col`, `F.isin`, `F.rlike`).
It guards PySpark imports so the module compiles and imports cleanly in pure-Python
local environments without Spark installed.
"""

from __future__ import annotations

import argparse
import uuid
from functools import reduce
from operator import and_, or_
from pathlib import Path
from typing import Any

from ingestion.contracts import AlertDispatcher, load_contract
from shared_models import ContractValidationReport, ContractViolation, PlatformConfig

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    PYSPARK_AVAILABLE = True
except ImportError:  # pragma: no cover - local pure-Python execution
    PYSPARK_AVAILABLE = False
    DataFrame = Any  # type: ignore[misc,assignment]
    SparkSession = Any  # type: ignore[misc,assignment]
    F = None  # type: ignore[assignment]
    BooleanType = DoubleType = IntegerType = StringType = StructField = StructType = None  # type: ignore[assignment]


def _spark_type_for(type_name: str) -> Any:
    """Map YAML contract column type to PySpark DataType."""
    if not PYSPARK_AVAILABLE:
        raise RuntimeError("PySpark is not installed in this environment.")
    norm = type_name.lower().strip()
    if norm in {"int", "integer"}:
        return IntegerType()
    if norm in {"float", "double", "number", "decimal"}:
        return DoubleType()
    if norm in {"bool", "boolean"}:
        return BooleanType()
    return StringType()


def build_spark_schema(contract: dict[str, Any]) -> Any:
    """Build a PySpark `StructType` directly from a loaded YAML data contract."""
    if not PYSPARK_AVAILABLE:
        raise RuntimeError("PySpark is not installed in this environment.")
    fields: list[Any] = []
    for col_name, col_spec in contract.get("columns", {}).items():
        spark_dtype = _spark_type_for(str(col_spec.get("type", "str")))
        nullable = bool(col_spec.get("nullable", True))
        fields.append(StructField(col_name, spark_dtype, nullable=nullable))
    return StructType(fields)


def get_contract_spark_schemas(contracts_dir: str | Path = "contracts") -> dict[str, Any]:
    """Build PySpark `StructType` schemas for all contracts in `contracts_dir`."""
    schemas: dict[str, Any] = {}
    for path in sorted(Path(contracts_dir).glob("*.y*ml")):
        contract = load_contract(path)
        dataset = str(contract.get("dataset", path.stem))
        schemas[dataset] = build_spark_schema(contract)
    return schemas


class SparkDataContractIngestor:
    """Distributed PySpark mirror of `DataContractValidator` for Databricks/EMR."""

    def __init__(
        self,
        spark: SparkSession,
        contracts_dir: str | Path = "contracts",
        quarantine_uri: str = "data/quarantine",
        alerts_dir: str | Path = "data/alerts",
        alert_dispatcher: AlertDispatcher | None = None,
    ) -> None:
        if not PYSPARK_AVAILABLE:
            raise RuntimeError("PySpark is required to instantiate SparkDataContractIngestor.")
        self.spark = spark
        self.contracts_dir = Path(contracts_dir)
        self.quarantine_uri = quarantine_uri.rstrip("/")
        self.alert_dispatcher = alert_dispatcher or AlertDispatcher(alerts_dir=alerts_dir)

    def _load_contract(self, dataset: str) -> dict[str, Any]:
        for ext in (".yaml", ".yml", ".json"):
            candidate = self.contracts_dir / f"{dataset}{ext}"
            if candidate.is_file():
                return load_contract(candidate)
        raise FileNotFoundError(f"Contract not found for {dataset!r} in {self.contracts_dir}")

    def validate_and_ingest(
        self,
        dataset: str,
        df: DataFrame,
        target_table_or_path: str | None = None,
        output_format: str = "delta",
        correlation_id: str | None = None,
    ) -> tuple[DataFrame | None, ContractValidationReport]:
        """Validate a Spark DataFrame against its YAML contract, quarantine bad rows, and write Silver."""
        contract = self._load_contract(dataset)
        contract_version = str(contract.get("version", "1.0.0"))
        block_on_critical = bool(contract.get("block_on_critical", True))
        columns_spec: dict[str, dict[str, Any]] = contract.get("columns", {})
        cid = correlation_id or uuid.uuid4().hex[:12]

        total_rows = df.count()
        violations: list[ContractViolation] = []

        # 1. Schema-level missing column check
        missing_cols = [
            col_name
            for col_name, spec in columns_spec.items()
            if bool(spec.get("required", True)) and col_name not in df.columns
        ]
        if missing_cols:
            for col_name in missing_cols:
                violations.append(
                    ContractViolation(
                        row_index=-1,
                        field_name=col_name,
                        rule="missing_column",
                        severity="CRITICAL",
                        message=f"Required column '{col_name}' missing from Spark DataFrame.",
                    )
                )
            quarantine_dest = f"{self.quarantine_uri}/{dataset}_{cid}"
            df.write.mode("overwrite").json(quarantine_dest)
            report = ContractValidationReport(
                dataset=dataset,
                contract_version=contract_version,
                correlation_id=cid,
                total_rows=total_rows,
                valid_rows_count=0,
                invalid_rows_count=total_rows,
                blocked=block_on_critical,
                violations=violations,
                quarantine_file=quarantine_dest,
                alert_dispatched=True,
            )
            self.alert_dispatcher.dispatch(report, contract=contract)
            return None, report

        # 2. Row-level Spark Column predicates mirroring DataContractValidator
        row_invalid_conditions: list[Any] = []
        working_df = df

        for col_name, spec in columns_spec.items():
            spark_dtype = _spark_type_for(str(spec.get("type", "str")))
            cast_col = F.col(col_name).cast(spark_dtype)
            col_checks: list[Any] = []

            if not bool(spec.get("nullable", True)):
                col_checks.append(F.col(col_name).isNull())
                if isinstance(spark_dtype, StringType):
                    col_checks.append(F.trim(F.col(col_name).cast("string")) == "")

            # Type cast failure when original value is non-null
            col_checks.append(F.col(col_name).isNotNull() & cast_col.isNull())

            if spec.get("min") is not None:
                col_checks.append(cast_col < F.lit(spec["min"]))
            if spec.get("exclusive_min") is not None:
                col_checks.append(cast_col <= F.lit(spec["exclusive_min"]))
            if spec.get("max") is not None:
                col_checks.append(cast_col > F.lit(spec["max"]))
            if spec.get("exclusive_max") is not None:
                col_checks.append(cast_col >= F.lit(spec["exclusive_max"]))
            if spec.get("allowed_values") is not None:
                col_checks.append(~cast_col.isin(spec["allowed_values"]))
            if spec.get("pattern") is not None:
                col_checks.append(~cast_col.cast("string").rlike(str(spec["pattern"])))

            if col_checks:
                col_failed = reduce(or_, col_checks)
                working_df = working_df.withColumn(f"__err_{col_name}", col_failed)
                row_invalid_conditions.append(F.col(f"__err_{col_name}"))

        any_invalid_expr = (
            reduce(or_, row_invalid_conditions) if row_invalid_conditions else F.lit(False)
        )
        flagged_df = working_df.withColumn("__contract_invalid", any_invalid_expr)

        bad_df = flagged_df.filter(F.col("__contract_invalid"))
        good_df = flagged_df.filter(~F.col("__contract_invalid"))

        err_cols = [c for c in flagged_df.columns if c.startswith("__err_") or c == "__contract_invalid"]
        invalid_count = bad_df.count()
        valid_count = total_rows - invalid_count

        blocked = bool(block_on_critical and invalid_count > 0)
        quarantine_dest = ""
        alert_dispatched = False

        if invalid_count > 0:
            quarantine_dest = f"{self.quarantine_uri}/{dataset}_{cid}"
            bad_df.write.mode("overwrite").json(quarantine_dest)
            # Sample up to 25 failing rows for structured alert report
            for idx, row in enumerate(bad_df.limit(25).collect()):
                for col_name in columns_spec:
                    err_flag = row.asDict().get(f"__err_{col_name}", False)
                    if err_flag:
                        violations.append(
                            ContractViolation(
                                row_index=idx,
                                field_name=col_name,
                                rule="spark_contract_check",
                                severity="CRITICAL",
                                message=f"Column '{col_name}' violated contract rules in Spark batch.",
                                raw_value=row.asDict().get(col_name),
                            )
                        )

        report = ContractValidationReport(
            dataset=dataset,
            contract_version=contract_version,
            correlation_id=cid,
            total_rows=total_rows,
            valid_rows_count=valid_count,
            invalid_rows_count=invalid_count,
            blocked=blocked,
            violations=violations,
            quarantine_file=quarantine_dest,
        )

        if blocked:
            self.alert_dispatcher.dispatch(report, contract=contract)
            report.alert_dispatched = True
            return None, report

        clean_df = good_df.drop(*err_cols)
        if target_table_or_path:
            if output_format == "delta":
                clean_df.write.format("delta").mode("append").saveAsTable(target_table_or_path)
            else:
                clean_df.write.mode("append").parquet(target_table_or_path)

        _ = and_  # preserve import symmetry
        return clean_df, report


def main() -> None:  # pragma: no cover - Databricks/EMR spark-submit entrypoint
    """CLI entrypoint for running contract-guarded Spark ingestion on Databricks/EMR."""
    parser = argparse.ArgumentParser(description="Spark Data Contract Ingestion Job")
    parser.add_argument("--dataset", required=True, help="Contract dataset name (e.g. raw_orders)")
    parser.add_argument("--source-path", required=True, help="Input JSON/Parquet path")
    parser.add_argument("--source-format", default="json", choices=["json", "parquet", "csv"])
    parser.add_argument("--target", default="", help="Target Delta table or Parquet path")
    parser.add_argument("--output-format", default="delta", choices=["delta", "parquet"])
    args = parser.parse_args()

    if not PYSPARK_AVAILABLE:
        raise SystemExit("PySpark is not installed. Run on Databricks / EMR or use Python validator.")

    cfg = PlatformConfig.from_env()
    spark = SparkSession.builder.appName(f"ContractIngest-{args.dataset}").getOrCreate()
    ingestor = SparkDataContractIngestor(
        spark=spark,
        contracts_dir=cfg.contracts_dir,
        quarantine_uri=cfg.quarantine_dir,
    )
    reader = spark.read.format(args.source_format)
    if args.source_format == "csv":
        reader = reader.option("header", "true").option("inferSchema", "false")
    raw_df = reader.load(args.source_path)

    _, report = ingestor.validate_and_ingest(
        dataset=args.dataset,
        df=raw_df,
        target_table_or_path=args.target or None,
        output_format=args.output_format,
    )
    if report.blocked:
        raise SystemExit(
            f"Pipeline blocked by data contract for {args.dataset}; quarantined at {report.quarantine_file}"
        )


if __name__ == "__main__":  # pragma: no cover
    main()
