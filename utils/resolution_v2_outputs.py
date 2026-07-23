from __future__ import annotations

import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

from utils.resolution_v2_schema import QUEUE_DIMENSIONS, VERIFIED, fingerprint

OVERRIDE_COLUMNS = (
    "symbol",
    "symbol_era_id",
    "historical_identity_status",
    "historical_issuer_name",
    "historical_event_type",
    "historical_event_date",
    "historical_successor",
    "source_url",
    "source_note",
)
QUEUE_COLUMNS = (
    "symbol",
    "symbol_era_id",
    "trade_rows",
    "identity_status",
    "instrument_status",
    "event_status",
    "observation_status",
    "research_status",
    "eligibility",
    "exclusion_reason",
    "next_resolver",
)


def snapshot_cohort(root: Path, rows: list[dict[str, Any]]) -> tuple[str, bool]:
    selected = [_cohort_row(row) for row in rows]
    digest = fingerprint(selected)
    manifest_path, cohort_path = root / "cohort_manifest.json", root / "review_cohort.jsonl"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("sha256") != digest:
            raise ValueError(
                "review population changed; the stable V2 cohort cannot change silently"
            )
        return digest, False
    root.mkdir(parents=True, exist_ok=True)
    _write_jsonl(cohort_path, selected)
    _write_json(manifest_path, {"sha256": digest, "row_count": len(selected)})
    return digest, True


def project_legacy_overrides(
    path: Path, identities: list[dict[str, Any]], events: list[dict[str, Any]]
) -> int:
    identity_by_era = _latest_verified(identities)
    event_by_era = _latest_verified(events)
    rows = []
    for era_id in sorted(identity_by_era.keys() & event_by_era.keys()):
        identity, event = identity_by_era[era_id], event_by_era[era_id]
        rows.append(_legacy_row(identity, event))
    _write_csv(path, rows, OVERRIDE_COLUMNS)
    return len(rows)


def write_dimension_queues(
    root: Path, decisions: list[dict[str, Any]], cohort: list[dict[str, Any]]
) -> dict[str, int]:
    cohort_by_era = {row["symbol_era_id"]: row for row in cohort}
    current = _latest_by_era(decisions)
    queue_rows = [_queue_row(decision, cohort_by_era) for decision in current.values()]
    predicates = {
        "identity": lambda row: row["identity_status"] != VERIFIED,
        "event": lambda row: row["event_status"] != VERIFIED,
        "instrument": lambda row: not row["instrument_status"].startswith("verified"),
        "observation": lambda row: row["observation_status"] not in {"verified", "local_observed"},
        "research_action": lambda row: row["research_status"] == "action_required",
    }
    counts = {}
    for dimension in QUEUE_DIMENSIONS:
        rows = sorted(filter(predicates[dimension], queue_rows), key=_queue_sort_key)
        _write_csv(root / f"{dimension}_gap_queue.csv", rows, QUEUE_COLUMNS)
        counts[dimension] = len(rows)
    return counts


def reconciliation_summary(
    cohort: list[dict[str, Any]],
    facts: dict[str, list[dict[str, Any]]],
    queue_counts: dict[str, int],
    metrics: dict[str, int],
    stopping_reason: str,
) -> dict[str, Any]:
    decisions = _latest_by_era(facts["decision"])
    cohort_ids = {row["symbol_era_id"] for row in cohort}
    if cohort_ids != set(decisions):
        missing = sorted(cohort_ids - set(decisions))[:10]
        extra = sorted(set(decisions) - cohort_ids)[:10]
        raise ValueError(f"decision/cohort mismatch; missing={missing}; extra={extra}")
    status_counts = {
        dimension: dict(Counter(str(row.get(f"{dimension}_status")) for row in decisions.values()))
        for dimension in ("identity", "instrument", "event", "observation", "research")
    }
    active = [row for row in decisions.values() if row["research_status"] == "action_required"]
    return {
        "cohort_rows": len(cohort),
        "cohort_trade_rows": sum(int(row.get("trade_rows") or 0) for row in cohort),
        "active_research_rows": len(active),
        "active_research_trade_rows": sum(int(row.get("trade_rows") or 0) for row in active),
        "fact_counts": {kind: len(rows) for kind, rows in facts.items()},
        "status_counts": status_counts,
        "queue_counts": queue_counts,
        "requests": metrics,
        "stopping_reason": stopping_reason,
    }


def write_summary(path: Path, summary: dict[str, Any]) -> None:
    _write_json(path, summary)


def _legacy_row(identity: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
    source = event.get("source") or identity.get("source") or ""
    note = f"Canonical V2 projection; identity={identity['fact_id']}; event={event['fact_id']}"
    return {
        "symbol": identity["symbol"],
        "symbol_era_id": identity["symbol_era_id"],
        "historical_identity_status": "manual_verified_historical_identity",
        "historical_issuer_name": identity.get("issuer", ""),
        "historical_event_type": event.get("event_type", ""),
        "historical_event_date": event.get("event_date", ""),
        "historical_successor": event.get("new_symbol", ""),
        "source_url": source,
        "source_note": note,
    }


def _queue_row(decision: dict[str, Any], cohort: dict[str, dict[str, Any]]) -> dict[str, Any]:
    source = cohort.get(decision["symbol_era_id"], {})
    return {column: decision.get(column, source.get(column, "")) for column in QUEUE_COLUMNS}


def _cohort_row(row: dict[str, Any]) -> dict[str, Any]:
    fields = (
        "symbol",
        "symbol_era_id",
        "first_day",
        "last_day",
        "trade_rows",
        "source_classification",
        "instrument_type",
    )
    return {field: row.get(field, "") for field in fields}


def _latest_verified(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        era: row
        for era, row in _latest_by_era(records).items()
        if row.get("verification_state") == VERIFIED
    }


def _latest_by_era(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for row in sorted(
        records, key=lambda item: (item.get("created_at", ""), item.get("fact_id", ""))
    ):
        latest[row["symbol_era_id"]] = row
    return latest


def _queue_sort_key(row: dict[str, Any]) -> tuple[int, str]:
    return -int(row.get("trade_rows") or 0), str(row.get("symbol_era_id") or "")


def _write_csv(path: Path, rows: Iterable[dict[str, Any]], columns: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(columns), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
