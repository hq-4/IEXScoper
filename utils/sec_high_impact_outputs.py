from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from utils.sec_high_impact_state import WORKFLOW_VERSION, write_csv

IDENTITY_FIELDS = [
    "identity_bucket",
    "identity_score",
    "identity_reason",
    "identity_flags",
    "identity_cik",
    "identity_issuer_name",
    "identity_issuer_aliases",
    "identity_tickers",
    "identity_evidence_date",
    "identity_accession_no",
    "identity_document_url",
    "identity_source",
    "identity_candidate_cik_count",
]
EVENT_FIELDS = [
    "resolution_bucket",
    "event_type",
    "event_date",
    "event_successor",
    "event_flags",
    "event_snippet",
    "event_document_url",
    "event_form",
    "event_accession_no",
    "event_date_distance_days",
    "importable",
]
IMPORT_FIELDS = [
    "symbol",
    "symbol_era_id",
    "research_status",
    "proposed_historical_identity_status",
    "proposed_historical_issuer_name",
    "proposed_historical_event_type",
    "proposed_historical_event_date",
    "proposed_historical_successor",
    "primary_source_url",
    "secondary_source_url",
    "research_note",
]


def write_run_outputs(
    root: Path,
    input_rows: list[dict[str, str]],
    state: dict[str, Any],
    existing_ids: set[str],
    request_stats: dict[str, int],
) -> dict[str, Any]:
    completed = completed_results(input_rows, state)
    identity_fields = merged_fields(input_rows, IDENTITY_FIELDS)
    write_csv(root / "identity_evidence_review.csv", completed, identity_fields)
    verified = [row for row in completed if row.get("identity_bucket") == "identity_verified_ready"]
    write_csv(root / "identity_verified.csv", verified, identity_fields)
    event_rows = completed_event_rows(input_rows, state)
    write_csv(
        root / "event_evidence_review.csv", event_rows, merged_fields(input_rows, EVENT_FIELDS)
    )
    review_fields = merged_fields(
        input_rows, [*IDENTITY_FIELDS, *EVENT_FIELDS, "automation_status"]
    )
    write_csv(root / "high_impact_resolution_review.csv", completed, review_fields)
    auto = [row for row in completed if row.get("importable") is True]
    write_csv(root / "high_impact_auto_verified.csv", auto, review_fields)
    candidates, already = import_candidates(auto, existing_ids)
    write_csv(root / "high_impact_import_candidates.csv", candidates, IMPORT_FIELDS)
    summary = build_summary(input_rows, state, completed, candidates, already, request_stats)
    (root / "high_impact_resolution_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return summary


def completed_results(
    input_rows: list[dict[str, str]], state: dict[str, Any]
) -> list[dict[str, Any]]:
    by_id = {row["symbol_era_id"]: row for row in input_rows}
    output = []
    for symbol_era_id, entry in state["rows"].items():
        base = by_id.get(symbol_era_id)
        result = entry.get("result")
        if base and isinstance(result, dict):
            output.append({**base, **result, "automation_status": entry["status"]})
        elif base and entry.get("status") == "automation_exhausted":
            output.append(
                {
                    **base,
                    "identity_bucket": "identity_fetch_error",
                    "resolution_bucket": "fetch_error_hold",
                    "automation_status": entry["status"],
                    "importable": False,
                }
            )
    return output


def completed_event_rows(
    input_rows: list[dict[str, str]], state: dict[str, Any]
) -> list[dict[str, Any]]:
    by_id = {row["symbol_era_id"]: row for row in input_rows}
    return [
        {**by_id[symbol_era_id], **event}
        for symbol_era_id, entry in state["rows"].items()
        if symbol_era_id in by_id
        for event in entry.get("event_rows", [])
    ]


def import_candidates(
    rows: list[dict[str, Any]], existing_ids: set[str]
) -> tuple[list[dict[str, Any]], list[str]]:
    already = sorted(row["symbol_era_id"] for row in rows if row["symbol_era_id"] in existing_ids)
    candidates = [to_import_row(row) for row in rows if row["symbol_era_id"] not in existing_ids]
    return candidates, already


def to_import_row(row: dict[str, Any]) -> dict[str, Any]:
    note = (
        f"workflow={WORKFLOW_VERSION}; identity_cik={row.get('identity_cik', '')}; "
        f"identity_flags={row.get('identity_flags', '')}; event_flags={row.get('event_flags', '')}; "
        f"form={row.get('event_form', '')}; accession={row.get('event_accession_no', '')}; "
        f"date_distance={row.get('event_date_distance_days', '')}"
    )
    return {
        "symbol": row["symbol"],
        "symbol_era_id": row["symbol_era_id"],
        "research_status": "verified",
        "proposed_historical_identity_status": "manual_verified_historical_identity",
        "proposed_historical_issuer_name": row["identity_issuer_name"],
        "proposed_historical_event_type": row["event_type"],
        "proposed_historical_event_date": row["event_date"],
        "proposed_historical_successor": row.get("event_successor", ""),
        "primary_source_url": row["event_document_url"],
        "secondary_source_url": "",
        "research_note": note,
    }


def build_summary(
    inputs: list[dict[str, str]],
    state: dict[str, Any],
    completed: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
    already: list[str],
    stats: dict[str, int],
) -> dict[str, Any]:
    identity_counts, resolution_counts = (
        count_and_trade(completed, "identity_bucket"),
        count_and_trade(completed, "resolution_bucket"),
    )
    statuses = Counter(entry.get("status", "unattempted") for entry in state["rows"].values())
    total_trade = sum(int(row.get("trade_rows") or 0) for row in inputs)
    resolved_trade = sum(
        int(row.get("trade_rows") or 0) for row in completed if row.get("importable") is True
    )
    return {
        "workflow_version": WORKFLOW_VERSION,
        "input_row_count": len(inputs),
        "identity_buckets": identity_counts,
        "resolution_buckets": resolution_counts,
        "local_cache_identity_count": sum(
            row.get("identity_source") == "local_cache" for row in completed
        ),
        "network_identity_count": sum(row.get("identity_source") == "network" for row in completed),
        "unique_cik_count": len(
            {row.get("identity_cik") for row in completed if row.get("identity_cik")}
        ),
        "import_candidate_count": len(candidates),
        "already_imported_count": len(already),
        "already_imported_symbol_era_ids": already,
        "resolved_trade_rows": resolved_trade,
        "high_impact_trade_rows": total_trade,
        "resolved_trade_rows_pct": round(resolved_trade / total_trade * 100, 6)
        if total_trade
        else 0,
        "unattempted_count": len(inputs) - len(state["rows"]),
        "retryable_count": statuses["retryable"],
        "completed_count": statuses["completed"],
        "automation_exhausted_count": statuses["automation_exhausted"],
        **stats,
    }


def count_and_trade(rows: list[dict[str, Any]], key: str) -> dict[str, dict[str, int]]:
    output: dict[str, dict[str, int]] = defaultdict(lambda: {"count": 0, "trade_rows": 0})
    for row in rows:
        bucket = str(row.get(key) or "not_applicable")
        output[bucket]["count"] += 1
        output[bucket]["trade_rows"] += int(row.get("trade_rows") or 0)
    return dict(output)


def existing_override_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    with path.open(newline="", encoding="utf-8") as handle:
        return {row["symbol_era_id"] for row in csv.DictReader(handle) if row.get("symbol_era_id")}


def merged_fields(rows: list[dict[str, Any]], added: list[str]) -> list[str]:
    base = list(rows[0]) if rows else ["symbol", "symbol_era_id"]
    return list(dict.fromkeys([*base, *added]))
