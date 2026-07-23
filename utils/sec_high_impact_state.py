from __future__ import annotations

import csv
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

WORKFLOW_VERSION = "sec_identity_first_v1"
FINAL_STATUSES = {"completed", "automation_exhausted"}


def input_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def initialize_state(input_path: Path, output_root: Path, *, force: bool = False) -> dict[str, Any]:
    digest = input_digest(input_path)
    state_path = output_root / "run_state.json"
    existing = read_json(state_path)
    if existing and not force:
        validate_resume(existing, digest)
        return existing
    output_root.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(input_path, output_root / "input_snapshot.csv")
    (output_root / "input_sha256.txt").write_text(digest + "\n", encoding="utf-8")
    state = {
        "workflow_version": WORKFLOW_VERSION,
        "input_path": str(input_path),
        "input_sha256": digest,
        "created_at": now(),
        "updated_at": now(),
        "rows": {},
    }
    write_state(output_root, state)
    return state


def validate_resume(state: dict[str, Any], digest: str) -> None:
    if state.get("input_sha256") != digest:
        raise ValueError("input SHA-256 changed; choose a new output root or use --force-reprocess")
    if state.get("workflow_version") != WORKFLOW_VERSION:
        raise ValueError(
            "workflow version changed; choose a new output root or use --force-reprocess"
        )


def record_attempt(state: dict[str, Any], symbol_era_id: str) -> int:
    entry = state["rows"].setdefault(symbol_era_id, {})
    attempt = int(entry.get("attempt_count") or 0) + 1
    entry.update({"attempt_count": attempt, "status": "in_progress", "updated_at": now()})
    return attempt


def record_result(
    state: dict[str, Any],
    symbol_era_id: str,
    result: dict[str, Any],
    event_rows: list[dict[str, Any]],
) -> None:
    entry = state["rows"].setdefault(symbol_era_id, {})
    status = "completed" if result.get("importable") is True else "automation_exhausted"
    entry.update(
        {
            "status": status,
            "bucket": result["resolution_bucket"],
            "result": result,
            "event_rows": event_rows,
            "updated_at": now(),
        }
    )


def record_transport_error(
    state: dict[str, Any], symbol_era_id: str, error: Exception, max_attempts: int
) -> None:
    entry = state["rows"].setdefault(symbol_era_id, {})
    exhausted = int(entry.get("attempt_count") or 0) >= max_attempts
    entry.update(
        {
            "status": "automation_exhausted" if exhausted else "retryable",
            "bucket": "fetch_error_hold" if exhausted else "",
            "error": repr(error),
            "updated_at": now(),
        }
    )


def write_state(output_root: Path, state: dict[str, Any]) -> None:
    state["updated_at"] = now()
    path = output_root / "run_state.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"state must be a JSON object: {path}")
    return payload


def read_csv(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        fields = list(reader.fieldnames or [])
    if "symbol_era_id" not in fields:
        raise ValueError("input CSV missing symbol_era_id")
    ids = [row.get("symbol_era_id", "") for row in rows]
    if not all(ids) or len(ids) != len(set(ids)):
        raise ValueError("input symbol_era_id values must be non-empty and unique")
    return rows, fields


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows([{key: csv_value(row.get(key)) for key in fields} for row in rows])


def csv_value(value: Any) -> Any:
    if isinstance(value, (dict, list, tuple, set)):
        return json.dumps(value, sort_keys=True)
    if value is None:
        return ""
    return value


def now() -> str:
    return datetime.now(timezone.utc).isoformat()
