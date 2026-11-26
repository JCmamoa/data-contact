"""YAML Data Contract loader, pure-Python validator, quarantine writer, and alert dispatcher."""

from __future__ import annotations

import json
import logging
import math
import os
import re
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from shared_models import (
    ContractValidationReport,
    ContractViolation,
    PlatformConfig,
)

try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover - exercised when pyyaml is absent
    yaml = None

logger = logging.getLogger(__name__)

_CORRELATION_RE = re.compile(r"[^A-Za-z0-9_.-]+")
_INT_STR_RE = re.compile(r"^[+-]?\d+$")


# ---------------------------------------------------------------------------
# Pure-stdlib YAML + JSON parser fallback
# ---------------------------------------------------------------------------


def _strip_inline_comment(line: str) -> str:
    """Remove trailing `# comment` outside single or double quotes."""
    in_single = False
    in_double = False
    escaped = False
    for idx, ch in enumerate(line):
        if escaped:
            escaped = False
            continue
        if ch == "\\" and in_double:
            escaped = True
            continue
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif ch == "#" and not in_single and not in_double:
            if idx == 0 or line[idx - 1].isspace():
                return line[:idx].rstrip()
    return line.rstrip()


def _split_top_level(text: str, delimiter: str = ",") -> list[str]:
    """Split flow collections by delimiter while respecting quotes and nested brackets."""
    parts: list[str] = []
    buf: list[str] = []
    in_single = False
    in_double = False
    escaped = False
    depth = 0
    for ch in text:
        if escaped:
            buf.append(ch)
            escaped = False
            continue
        if ch == "\\" and in_double:
            buf.append(ch)
            escaped = True
            continue
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif not in_single and not in_double:
            if ch in "[{":
                depth += 1
            elif ch in "]}":
                depth = max(0, depth - 1)
            elif ch == delimiter and depth == 0:
                parts.append("".join(buf).strip())
                buf = []
                continue
        buf.append(ch)
    tail = "".join(buf).strip()
    if tail:
        parts.append(tail)
    return parts


def _find_mapping_colon(text: str) -> int:
    """Return index of key/value `:` outside quotes/brackets, or -1."""
    in_single = False
    in_double = False
    escaped = False
    depth = 0
    for idx, ch in enumerate(text):
        if escaped:
            escaped = False
            continue
        if ch == "\\" and in_double:
            escaped = True
            continue
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif not in_single and not in_double:
            if ch in "[{":
                depth += 1
            elif ch in "]}":
                depth = max(0, depth - 1)
            elif ch == ":" and depth == 0:
                if idx + 1 == len(text) or text[idx + 1].isspace():
                    return idx
    return -1


def _parse_scalar(raw: str) -> Any:
    """Parse a YAML scalar or inline flow list/dict into a Python object."""
    val = raw.strip()
    if not val or val in {"null", "Null", "NULL", "~"}:
        return None
    if val in {"true", "True", "TRUE"}:
        return True
    if val in {"false", "False", "FALSE"}:
        return False

    if len(val) >= 2 and val[0] == "'" and val[-1] == "'":
        return val[1:-1].replace("''", "'")

    if len(val) >= 2 and val[0] == '"' and val[-1] == '"':
        inner = val[1:-1]
        try:
            return json.loads(f'"{inner}"')
        except json.JSONDecodeError:
            return (
                inner.replace('\\"', '"')
                .replace("\\\\", "\\")
                .replace("\\n", "\n")
                .replace("\\t", "\t")
            )

    if val.startswith("[") and val.endswith("]"):
        inner = val[1:-1].strip()
        if not inner:
            return []
        return [_parse_scalar(item) for item in _split_top_level(inner)]

    if val.startswith("{") and val.endswith("}"):
        inner = val[1:-1].strip()
        if not inner:
            return {}
        result: dict[str, Any] = {}
        for item in _split_top_level(inner):
            colon_idx = _find_mapping_colon(item)
            if colon_idx == -1:
                raise ValueError(f"Invalid inline mapping entry: {item!r}")
            k = str(_parse_scalar(item[:colon_idx]))
            v = _parse_scalar(item[colon_idx + 1 :])
            result[k] = v
        return result

    if _INT_STR_RE.match(val):
        return int(val)
    try:
        if any(c in val for c in (".", "e", "E")):
            num = float(val)
            if not (math.isnan(num) or math.isinf(num)):
                return num
    except ValueError:
        pass

    return val


def _tokenize_yaml_lines(text: str) -> list[tuple[int, str]]:
    """Strip BOM, blanks, and comments; return list of (indent, content)."""
    clean_text = text.lstrip("\ufeff")
    tokens: list[tuple[int, str]] = []
    for raw_line in clean_text.splitlines():
        if not raw_line.strip():
            continue
        stripped_comment = _strip_inline_comment(raw_line)
        if not stripped_comment.strip():
            continue
        indent = len(stripped_comment) - len(stripped_comment.lstrip(" "))
        tokens.append((indent, stripped_comment.lstrip(" ")))
    return tokens


def _parse_yaml_block(tokens: list[tuple[int, str]], idx: int, base_indent: int) -> tuple[Any, int]:
    """Recursively parse an indented YAML mapping or sequence block."""
    if idx >= len(tokens):
        return {}, idx

    first_indent, first_content = tokens[idx]
    if first_content == "-" or first_content.startswith("- "):
        return _parse_yaml_sequence(tokens, idx, first_indent)
    return _parse_yaml_mapping(tokens, idx, first_indent)


def _parse_yaml_sequence(
    tokens: list[tuple[int, str]], idx: int, seq_indent: int
) -> tuple[list[Any], int]:
    """Parse a YAML `- item` sequence at `seq_indent`."""
    items: list[Any] = []
    n = len(tokens)
    while idx < n:
        indent, content = tokens[idx]
        if indent < seq_indent:
            break
        if indent > seq_indent:
            raise ValueError(f"Unexpected indentation in YAML sequence near: {content!r}")
        if not (content == "-" or content.startswith("- ")):
            break

        rest = content[1:].lstrip(" ") if len(content) > 1 else ""
        idx += 1

        if not rest:
            if idx < n and tokens[idx][0] > seq_indent:
                child, idx = _parse_yaml_block(tokens, idx, tokens[idx][0])
                items.append(child)
            else:
                items.append(None)
            continue

        colon_idx = _find_mapping_colon(rest)
        if colon_idx != -1:
            # Inline dict item inside sequence: `- key: val`
            key = str(_parse_scalar(rest[:colon_idx]))
            val_str = rest[colon_idx + 1 :].strip()
            item_dict: dict[str, Any] = {}
            if val_str:
                item_dict[key] = _parse_scalar(val_str)
            elif idx < n and tokens[idx][0] > seq_indent + 1:
                child, idx = _parse_yaml_block(tokens, idx, tokens[idx][0])
                item_dict[key] = child
            else:
                item_dict[key] = None

            if idx < n and tokens[idx][0] > seq_indent:
                more_dict, idx = _parse_yaml_mapping(tokens, idx, tokens[idx][0])
                item_dict.update(more_dict)
            items.append(item_dict)
        else:
            items.append(_parse_scalar(rest))
    return items, idx


def _parse_yaml_mapping(
    tokens: list[tuple[int, str]], idx: int, map_indent: int
) -> tuple[dict[str, Any], int]:
    """Parse a YAML `key: value` mapping at `map_indent`."""
    mapping: dict[str, Any] = {}
    n = len(tokens)
    while idx < n:
        indent, content = tokens[idx]
        if indent < map_indent:
            break
        if indent > map_indent:
            raise ValueError(f"Unexpected indentation in YAML mapping near: {content!r}")
        if content == "-" or content.startswith("- "):
            break

        colon_idx = _find_mapping_colon(content)
        if colon_idx == -1:
            raise ValueError(f"Expected 'key: value' mapping entry, got: {content!r}")

        key = str(_parse_scalar(content[:colon_idx]))
        val_str = content[colon_idx + 1 :].strip()
        idx += 1

        if val_str:
            mapping[key] = _parse_scalar(val_str)
        elif idx < n:
            next_indent, next_content = tokens[idx]
            if next_indent > map_indent or (
                next_indent == map_indent and (next_content == "-" or next_content.startswith("- "))
            ):
                child, idx = _parse_yaml_block(tokens, idx, next_indent)
                mapping[key] = child
            else:
                mapping[key] = None
        else:
            mapping[key] = None
    return mapping, idx


def parse_yaml_lite(text: str) -> Any:
    """Pure-stdlib parser supporting JSON and structured indentation-based YAML."""
    stripped = text.lstrip("\ufeff").strip()
    if not stripped:
        return {}
    if stripped[0] in "{[":
        return json.loads(stripped)

    tokens = _tokenize_yaml_lines(text)
    if not tokens:
        return {}
    parsed, _ = _parse_yaml_block(tokens, 0, tokens[0][0])
    return parsed


def load_contract(path: str | Path) -> dict[str, Any]:
    """Load and normalize a YAML/JSON data contract file."""
    contract_path = Path(path)
    raw_text = contract_path.read_text(encoding="utf-8")
    if yaml is not None:
        data = yaml.safe_load(raw_text)
    else:
        data = parse_yaml_lite(raw_text)

    if not isinstance(data, dict):
        raise ValueError(f"Contract {contract_path} must deserialize to a mapping.")

    columns = data.get("columns", {})
    if isinstance(columns, list):
        normalized_cols: dict[str, dict[str, Any]] = {}
        for entry in columns:
            if not isinstance(entry, dict) or "name" not in entry:
                raise ValueError(f"List-style column entry missing 'name' in {contract_path}")
            col_name = str(entry["name"])
            normalized_cols[col_name] = {k: v for k, v in entry.items() if k != "name"}
        data["columns"] = normalized_cols
    elif not isinstance(columns, dict):
        raise ValueError(f"Contract 'columns' in {contract_path} must be a mapping or list.")

    return data


# ---------------------------------------------------------------------------
# Alerting
# ---------------------------------------------------------------------------


class AlertDispatcher:
    """Writes structured JSON alerts locally and optionally POSTs to ALERT_WEBHOOK_URL."""

    def __init__(
        self,
        alerts_dir: str | Path = "data/alerts",
        webhook_url: str | None = None,
    ) -> None:
        self.alerts_dir = Path(alerts_dir)
        self.webhook_url = (
            webhook_url if webhook_url is not None else os.getenv("ALERT_WEBHOOK_URL", "").strip()
        )
        self.last_alert: dict[str, Any] | None = None

    def dispatch(
        self,
        report: ContractValidationReport,
        contract: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Persist a structured JSON alert and POST to configured webhook if present."""
        summary: dict[str, int] = {}
        for v in report.violations:
            key = f"{v.severity}:{v.rule}:{v.field_name}"
            summary[key] = summary.get(key, 0) + 1

        alert_payload: dict[str, Any] = {
            "alert_id": f"{report.dataset}_{report.correlation_id}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "severity": "CRITICAL" if report.blocked else "WARNING",
            "dataset": report.dataset,
            "contract_version": report.contract_version,
            "owner": (contract or {}).get("owner", "unknown"),
            "correlation_id": report.correlation_id,
            "blocked": report.blocked,
            "total_rows": report.total_rows,
            "valid_rows_count": report.valid_rows_count,
            "invalid_rows_count": report.invalid_rows_count,
            "quarantine_file": report.quarantine_file,
            "violation_count": len(report.violations),
            "violations_summary": summary,
            "sample_violations": [v.to_dict() for v in report.violations[:10]],
        }

        self.alerts_dir.mkdir(parents=True, exist_ok=True)
        alert_file = self.alerts_dir / f"{report.dataset}_{report.correlation_id}.json"
        alert_file.write_text(json.dumps(alert_payload, indent=2, default=str), encoding="utf-8")
        alert_payload["alert_file"] = str(alert_file)

        logger.error("DATA_CONTRACT_ALERT %s", json.dumps(alert_payload, default=str))

        if self.webhook_url:
            try:
                body = json.dumps(alert_payload, default=str).encode("utf-8")
                req = urllib.request.Request(
                    self.webhook_url,
                    data=body,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=5) as resp:
                    alert_payload["webhook_status"] = getattr(resp, "status", 200)
            except (urllib.error.URLError, OSError, ValueError) as exc:
                logger.warning("Failed to POST alert webhook to %s: %s", self.webhook_url, exc)
                alert_payload["webhook_error"] = str(exc)

        self.last_alert = alert_payload
        return alert_payload


# ---------------------------------------------------------------------------
# Data Contract Validator
# ---------------------------------------------------------------------------


def _coerce_value(value: Any, expected_type: str) -> tuple[bool, Any]:
    """Validate and coerce value to expected contract type."""
    norm_type = expected_type.lower().strip()
    if norm_type in {"str", "string"}:
        if isinstance(value, str):
            return True, value
        return False, value

    if norm_type in {"int", "integer"}:
        if isinstance(value, bool):
            return False, value
        if isinstance(value, int):
            return True, value
        if isinstance(value, float):
            if math.isnan(value) or math.isinf(value) or not value.is_integer():
                return False, value
            return True, int(value)
        if isinstance(value, str):
            stripped = value.strip()
            if _INT_STR_RE.match(stripped):
                return True, int(stripped)
        return False, value

    if norm_type in {"float", "double", "number", "decimal"}:
        if isinstance(value, bool):
            return False, value
        if isinstance(value, (int, float)):
            num = float(value)
            if math.isnan(num) or math.isinf(num):
                return False, value
            return True, num
        if isinstance(value, str):
            try:
                num = float(value.strip())
                if math.isnan(num) or math.isinf(num):
                    return False, value
                return True, num
            except ValueError:
                return False, value
        return False, value

    if norm_type in {"bool", "boolean"}:
        if isinstance(value, bool):
            return True, value
        if isinstance(value, str):
            low = value.strip().lower()
            if low == "true":
                return True, True
            if low == "false":
                return True, False
        return False, value

    return True, value


class DataContractValidator:
    """Validates incoming batches against YAML contracts, quarantining and alerting on failure."""

    def __init__(
        self,
        contracts_dir: str | Path = "contracts",
        quarantine_dir: str | Path = "data/quarantine",
        alerts_dir: str | Path | None = None,
        alert_dispatcher: AlertDispatcher | None = None,
    ) -> None:
        self.contracts_dir = Path(contracts_dir)
        self.quarantine_dir = Path(quarantine_dir)
        resolved_alerts_dir = (
            Path(alerts_dir) if alerts_dir is not None else self.quarantine_dir.parent / "alerts"
        )
        self.alert_dispatcher = alert_dispatcher or AlertDispatcher(alerts_dir=resolved_alerts_dir)

    @classmethod
    def from_config(cls, config: PlatformConfig) -> "DataContractValidator":
        """Construct validator using PlatformConfig paths."""
        return cls(
            contracts_dir=config.contracts_dir,
            quarantine_dir=config.quarantine_dir,
        )

    def resolve_contract_path(self, dataset_or_path: str | Path) -> Path:
        """Resolve a dataset name or explicit file path to an existing contract file."""
        candidate = Path(dataset_or_path)
        if candidate.is_file():
            return candidate
        for ext in (".yaml", ".yml", ".json"):
            by_name = self.contracts_dir / f"{dataset_or_path}{ext}"
            if by_name.is_file():
                return by_name
        raise FileNotFoundError(
            f"Data contract not found for {dataset_or_path!r} in {self.contracts_dir}"
        )

    def load_contract(self, dataset_or_path: str | Path) -> dict[str, Any]:
        """Load contract dictionary by dataset name or file path."""
        return load_contract(self.resolve_contract_path(dataset_or_path))

    def validate(
        self,
        dataset: str,
        records: list[dict[str, Any]],
        correlation_id: str | None = None,
        contract: dict[str, Any] | None = None,
    ) -> ContractValidationReport:
        """Validate a batch and return the ContractValidationReport."""
        _, report = self.validate_batch(
            dataset=dataset,
            records=records,
            correlation_id=correlation_id,
            contract=contract,
        )
        return report

    def validate_batch(
        self,
        dataset: str,
        records: list[dict[str, Any]],
        correlation_id: str | None = None,
        contract: dict[str, Any] | None = None,
    ) -> tuple[list[dict[str, Any]], ContractValidationReport]:
        """Validate a batch, returning `(valid_records, report)` with quarantine & alerts on failure."""
        loaded_contract = contract if contract is not None else self.load_contract(dataset)
        contract_version = str(loaded_contract.get("version", "1.0.0"))
        block_on_critical = bool(loaded_contract.get("block_on_critical", True))
        columns: dict[str, dict[str, Any]] = loaded_contract.get("columns", {})

        raw_cid = correlation_id or uuid.uuid4().hex[:12]
        safe_cid = _CORRELATION_RE.sub("_", raw_cid)

        total_rows = len(records)
        if total_rows == 0:
            report = ContractValidationReport(
                dataset=dataset,
                contract_version=contract_version,
                correlation_id=safe_cid,
                total_rows=0,
                valid_rows_count=0,
                invalid_rows_count=0,
                blocked=False,
                violations=[],
            )
            return [], report

        violations: list[ContractViolation] = []
        observed_columns = {key for row in records for key in row.keys()}
        missing_batch_cols: set[str] = set()

        for col_name, col_spec in columns.items():
            is_required = bool(col_spec.get("required", True))
            if is_required and col_name not in observed_columns:
                missing_batch_cols.add(col_name)
                violations.append(
                    ContractViolation(
                        row_index=-1,
                        field_name=col_name,
                        rule="missing_column",
                        severity="CRITICAL",
                        message=f"Required column '{col_name}' is missing from incoming batch schema.",
                        raw_value=None,
                    )
                )

        valid_records: list[dict[str, Any]] = []
        bad_records_payload: list[dict[str, Any]] = []
        invalid_row_indices: set[int] = set()

        for row_idx, row in enumerate(records):
            row_violations: list[ContractViolation] = []
            coerced_row = dict(row)

            for col_name, col_spec in columns.items():
                if col_name in missing_batch_cols:
                    continue

                severity = str(col_spec.get("severity", "CRITICAL")).upper()
                nullable = bool(col_spec.get("nullable", True))
                is_required = bool(col_spec.get("required", True))

                if col_name not in row:
                    if is_required or not nullable:
                        row_violations.append(
                            ContractViolation(
                                row_index=row_idx,
                                field_name=col_name,
                                rule="missing_column",
                                severity="CRITICAL",
                                message=f"Row {row_idx} is missing required column '{col_name}'.",
                                raw_value=None,
                            )
                        )
                    continue

                raw_val = row[col_name]
                is_null = (
                    raw_val is None
                    or (isinstance(raw_val, float) and math.isnan(raw_val))
                    or (isinstance(raw_val, str) and raw_val.strip() == "")
                )
                if is_null:
                    if not nullable:
                        row_violations.append(
                            ContractViolation(
                                row_index=row_idx,
                                field_name=col_name,
                                rule="not_null",
                                severity=severity,
                                message=f"Column '{col_name}' cannot be null or empty.",
                                raw_value=raw_val,
                            )
                        )
                    continue

                expected_type = str(col_spec.get("type", "str"))
                ok_type, coerced_val = _coerce_value(raw_val, expected_type)
                if not ok_type:
                    row_violations.append(
                        ContractViolation(
                            row_index=row_idx,
                            field_name=col_name,
                            rule="type_check",
                            severity=severity,
                            message=f"Column '{col_name}' expected type '{expected_type}', got {raw_val!r}.",
                            raw_value=raw_val,
                        )
                    )
                    continue

                coerced_row[col_name] = coerced_val

                min_val = col_spec.get("min")
                if min_val is not None and isinstance(coerced_val, (int, float)) and coerced_val < min_val:
                    row_violations.append(
                        ContractViolation(
                            row_index=row_idx,
                            field_name=col_name,
                            rule="min_value",
                            severity=severity,
                            message=f"Column '{col_name}' value {coerced_val} < min {min_val}.",
                            raw_value=raw_val,
                        )
                    )

                excl_min = col_spec.get("exclusive_min")
                if (
                    excl_min is not None
                    and isinstance(coerced_val, (int, float))
                    and coerced_val <= excl_min
                ):
                    row_violations.append(
                        ContractViolation(
                            row_index=row_idx,
                            field_name=col_name,
                            rule="exclusive_min_value",
                            severity=severity,
                            message=f"Column '{col_name}' value {coerced_val} <= exclusive_min {excl_min}.",
                            raw_value=raw_val,
                        )
                    )

                max_val = col_spec.get("max")
                if max_val is not None and isinstance(coerced_val, (int, float)) and coerced_val > max_val:
                    row_violations.append(
                        ContractViolation(
                            row_index=row_idx,
                            field_name=col_name,
                            rule="max_value",
                            severity=severity,
                            message=f"Column '{col_name}' value {coerced_val} > max {max_val}.",
                            raw_value=raw_val,
                        )
                    )

                excl_max = col_spec.get("exclusive_max")
                if (
                    excl_max is not None
                    and isinstance(coerced_val, (int, float))
                    and coerced_val >= excl_max
                ):
                    row_violations.append(
                        ContractViolation(
                            row_index=row_idx,
                            field_name=col_name,
                            rule="exclusive_max_value",
                            severity=severity,
                            message=f"Column '{col_name}' value {coerced_val} >= exclusive_max {excl_max}.",
                            raw_value=raw_val,
                        )
                    )

                allowed_values = col_spec.get("allowed_values")
                if allowed_values is not None and coerced_val not in allowed_values:
                    row_violations.append(
                        ContractViolation(
                            row_index=row_idx,
                            field_name=col_name,
                            rule="allowed_values",
                            severity=severity,
                            message=f"Column '{col_name}' value {coerced_val!r} not in {allowed_values}.",
                            raw_value=raw_val,
                        )
                    )

                pattern = col_spec.get("pattern")
                if pattern is not None and re.search(str(pattern), str(coerced_val)) is None:
                    row_violations.append(
                        ContractViolation(
                            row_index=row_idx,
                            field_name=col_name,
                            rule="pattern",
                            severity=severity,
                            message=f"Column '{col_name}' value {coerced_val!r} failed pattern {pattern!r}.",
                            raw_value=raw_val,
                        )
                    )

            if row_violations:
                violations.extend(row_violations)
                invalid_row_indices.add(row_idx)
                bad_records_payload.append(
                    {
                        "row_index": row_idx,
                        "record": row,
                        "violations": [v.to_dict() for v in row_violations],
                    }
                )
            elif not missing_batch_cols:
                valid_records.append(coerced_row)

        if missing_batch_cols:
            invalid_rows_count = total_rows
            valid_rows_count = 0
            bad_records_payload = [
                {"row_index": idx, "record": row, "violations": ["schema_missing_column"]}
                for idx, row in enumerate(records)
            ]
        else:
            invalid_rows_count = len(invalid_row_indices)
            valid_rows_count = total_rows - invalid_rows_count

        has_critical = any(v.severity == "CRITICAL" for v in violations)
        blocked = bool(block_on_critical and has_critical)

        report = ContractValidationReport(
            dataset=dataset,
            contract_version=contract_version,
            correlation_id=safe_cid,
            total_rows=total_rows,
            valid_rows_count=valid_rows_count,
            invalid_rows_count=invalid_rows_count,
            blocked=blocked,
            violations=violations,
        )

        if violations:
            self.quarantine_dir.mkdir(parents=True, exist_ok=True)
            quarantine_path = self.quarantine_dir / f"{dataset}_{safe_cid}.json"
            report.quarantine_file = str(quarantine_path)
            quarantine_doc = {
                "quarantined_at": datetime.now(timezone.utc).isoformat(),
                "report": report.to_dict(),
                "bad_records": bad_records_payload,
            }
            quarantine_path.write_text(
                json.dumps(quarantine_doc, indent=2, default=str),
                encoding="utf-8",
            )

        if blocked or has_critical:
            self.alert_dispatcher.dispatch(report, contract=loaded_contract)
            report.alert_dispatched = True

        return ([] if blocked else valid_records), report
