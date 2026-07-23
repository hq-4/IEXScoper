from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

RESOLVER_VERSION = "evidence_delta_v2"
DEFAULT_FACT_ROOT = Path("data/resolution")
DEFAULT_REPORT_ROOT = Path("reports/dead-ticker-review/resolution-v2")
DEFAULT_REVIEW_PATH = Path("reports/dead-ticker-review/dead_ticker_review_queue.csv")
DEFAULT_OVERRIDE_PATH = Path("data/manual_overrides/historical_ticker_identities.csv")
DEFAULT_LEDGER_PATH = Path("data/manual_overrides/ticker_era_resolution_ledger.csv")
DEFAULT_WORKPLAN_PATH = Path("reports/dead-ticker-review/resolution-workplan/workplan_all.csv")

FACT_FILES = {
    "identity": "identity_facts.jsonl",
    "event": "event_facts.jsonl",
    "observation": "observation_facts.jsonl",
    "decision": "research_decisions.jsonl",
    "attempt": "resolver_attempts.jsonl",
}
QUEUE_DIMENSIONS = ("identity", "event", "instrument", "observation", "research_action")
VERIFIED = "verified"
UNRESOLVED = "unresolved"
NOT_APPLICABLE = "not_applicable"
RESEARCH_CLOSED = "research_closed"
EVENT_CANDIDATE = "event_candidate"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def fingerprint(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def fact_id(kind: str, record: dict[str, Any]) -> str:
    stable = {
        key: value
        for key, value in record.items()
        if key not in {"fact_id", "created_at", "updated_at"}
    }
    return f"{kind}:{fingerprint(stable)[:24]}"


def prepare_fact(kind: str, record: dict[str, Any]) -> dict[str, Any]:
    validate_era(record)
    output = {**record, "record_type": kind, "resolver_version": RESOLVER_VERSION}
    output.setdefault("created_at", utc_now())
    output["fact_id"] = fact_id(kind, output)
    return output


def validate_era(record: dict[str, Any]) -> None:
    era = str(record.get("symbol_era_id") or "").strip()
    symbol = str(record.get("symbol") or "").strip().upper()
    if not era or "#" not in era:
        raise ValueError("canonical record requires symbol_era_id")
    if not symbol or not era.startswith(f"{symbol}#"):
        raise ValueError(f"symbol does not match symbol_era_id: {symbol!r}, {era!r}")


def evidence_fingerprint(row: dict[str, Any], requirements: list[str] | tuple[str, ...]) -> str:
    evidence = {
        "symbol_era_id": row.get("symbol_era_id"),
        "first_day": row.get("first_day"),
        "last_day": row.get("last_day"),
        "identity": row.get("identity_fact_id") or row.get("identity_status"),
        "instrument": row.get("instrument_type") or row.get("instrument_status"),
        "requirements": sorted(requirements),
    }
    return fingerprint(evidence)
