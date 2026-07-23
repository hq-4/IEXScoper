from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from utils.import_dead_ticker_manual_overrides import (
    ManualOverrideImportConfig,
    import_manual_overrides,
)
from utils.sec_high_impact_outputs import existing_override_ids, write_run_outputs
from utils.sec_high_impact_state import WORKFLOW_VERSION, read_json, write_csv
from utils.sec_identity_sources import SecEvidenceClient
from utils.sec_terminal_followup_sources import SecRequestConfig, SecSubmissionsClient


def default_raw_paths(output_root: Path) -> list[Path]:
    report_root = Path("reports/dead-ticker-review")
    return [
        path
        for path in report_root.rglob("edgar_full_text_raw.jsonl")
        if output_root not in path.parents
    ]


def load_ticker_directory(path: Path) -> set[tuple[str, str]]:
    if not path.exists():
        return set()
    payload = json.loads(path.read_text(encoding="utf-8"))
    fields, data = payload.get("fields", []), payload.get("data", [])
    if not isinstance(fields, list) or not isinstance(data, list):
        return set()
    ticker_index, cik_index = fields.index("ticker"), fields.index("cik")
    return {(str(row[ticker_index]).upper(), str(row[cik_index]).lstrip("0")) for row in data}


def new_evidence_client(config: Any) -> SecEvidenceClient:
    return SecEvidenceClient(
        config.user_agent,
        timeout_seconds=config.timeout_seconds,
        sleep_seconds=config.sleep_seconds,
        retries=config.retries,
    )


def new_submissions_client(config: Any) -> SecSubmissionsClient:
    request = SecRequestConfig(
        config.user_agent, config.timeout_seconds, config.sleep_seconds, config.retries
    )
    return SecSubmissionsClient(request)


def total_requests(evidence_client: Any, submissions_client: Any) -> int:
    return int(getattr(getattr(evidence_client, "stats", None), "request_count", 0)) + int(
        getattr(submissions_client, "request_count", 0)
    )


def write_outputs(
    config: Any,
    rows: list[dict[str, str]],
    state: dict[str, Any],
    evidence_client: Any,
    submissions_client: Any,
) -> dict[str, Any]:
    stats = {
        "sec_request_count": total_requests(evidence_client, submissions_client),
        "retry_count": int(getattr(getattr(evidence_client, "stats", None), "retry_count", 0))
        + int(getattr(submissions_client, "retry_count", 0)),
        "fetch_error_count": int(
            getattr(getattr(evidence_client, "stats", None), "fetch_error_count", 0)
        ),
    }
    return write_run_outputs(
        config.output_root, rows, state, existing_override_ids(config.overrides_path), stats
    )


def finalize_import(config: Any, summary: dict[str, Any]) -> int:
    if summary["unattempted_count"] or summary["retryable_count"]:
        return 0
    summary_path = config.output_root / (
        "high_impact_import_summary.json"
        if config.apply_import
        else "high_impact_import_dry_run_summary.json"
    )
    result = import_manual_overrides(
        ManualOverrideImportConfig(
            template_path=config.output_root / "high_impact_import_candidates.csv",
            overrides_path=config.overrides_path,
            summary_path=summary_path,
            dry_run=not config.apply_import,
        )
    )
    if config.apply_import:
        write_automation_ledger(config)
        regenerate_reporting()
        return int(result["summary"]["verified_row_count"])
    return 0


def update_import_summary(
    config: Any, summary: dict[str, Any], imported_count: int
) -> dict[str, Any]:
    summary["imported_count"] = imported_count
    path = config.output_root / "high_impact_resolution_summary.json"
    path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return summary


def write_automation_ledger(config: Any) -> None:
    state = read_json(config.output_root / "run_state.json")
    rows = [
        {
            "symbol_era_id": symbol_era_id,
            "automation_status": entry.get("status", "unattempted"),
            "automation_workflow_version": WORKFLOW_VERSION,
            "automation_attempt_count": entry.get("attempt_count", 0),
            "automation_last_bucket": entry.get("bucket", ""),
        }
        for symbol_era_id, entry in state.get("rows", {}).items()
    ]
    path = Path("reports/dead-ticker-review/resolution-workplan/identity_resolution_automation.csv")
    fields = [
        "symbol_era_id",
        "automation_status",
        "automation_workflow_version",
        "automation_attempt_count",
        "automation_last_bucket",
    ]
    write_csv(path, rows, fields)


def regenerate_reporting() -> None:
    commands = (
        ("utils/build_dead_ticker_review_queue.py",),
        ("utils/build_dead_ticker_priority_queue.py",),
        (
            "utils/build_dead_ticker_resolution_lanes.py",
            "--output-root",
            "reports/dead-ticker-review/resolution-lanes",
        ),
        ("utils/build_dead_ticker_resolution_workplan.py",),
    )
    root = Path(__file__).resolve().parents[1]
    for command in commands:
        subprocess.run([sys.executable, *command], cwd=root, check=True)
