from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from utils.resolution_v2_events import PROSPECTIVE_TERMS, semantic_action_date
from utils.resolution_v2_schema import VERIFIED, prepare_fact
from utils.sec_identity_evidence import parse_day

DEFAULT_EVENT_REVIEW = Path(
    "reports/dead-ticker-review/sec-high-impact-identity-resolution-round3/event_evidence_review.csv"
)
TERMINAL_FLAGS = {"actual_terminal_language", "cik_identity_match", "terminal_language"}
IDENTITY_FLAGS = {"direct_ticker_evidence", "issuer_name_match"}


def salvage_cached_events(
    identities: list[dict[str, Any]],
    path: Path = DEFAULT_EVENT_REVIEW,
) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    identity_by_era = {
        row["symbol_era_id"]: row for row in identities if row.get("verification_state") == VERIFIED
    }
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    facts = []
    for row in rows:
        identity = identity_by_era.get(row.get("symbol_era_id", ""))
        fact = _salvage_row(row, identity) if identity else None
        if fact:
            facts.append(fact)
    return list({fact["fact_id"]: fact for fact in facts}.values())


def update_decisions_for_events(
    decisions: list[dict[str, Any]], events: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    verified = {row["symbol_era_id"] for row in events if row["verification_state"] == VERIFIED}
    output = []
    for decision in decisions:
        values = {
            key: value
            for key, value in decision.items()
            if key not in {"fact_id", "created_at", "record_type", "resolver_version"}
        }
        if decision["symbol_era_id"] in verified:
            values["event_status"] = VERIFIED
            if (
                values.get("identity_status") == VERIFIED
                and values.get("research_status") == "action_required"
            ):
                values["eligibility"] = "eligible"
                values["next_resolver"] = "instrument_verification_v2"
        output.append(prepare_fact("decision", values))
    return output


def _salvage_row(row: dict[str, str], identity: dict[str, Any]) -> dict[str, Any] | None:
    if row.get("event_form", "").upper() in {"25", "25-NSE"}:
        return None
    flags = {item for item in row.get("event_flags", "").split("|") if item}
    snippet = row.get("event_snippet", "")
    boundary = parse_day(row.get("last_day"))
    event_date, clause = semantic_action_date(snippet, boundary)
    near = bool(boundary and event_date and abs((boundary - event_date).days) <= 14)
    actual = TERMINAL_FLAGS <= flags and bool(IDENTITY_FLAGS & flags)
    prospective = any(term in clause.lower() for term in PROSPECTIVE_TERMS)
    if not (actual and near and not prospective):
        return None
    payload = _event_payload(row, identity, flags, event_date.isoformat(), clause)
    return prepare_fact("event", payload)


def _event_payload(
    row: dict[str, str],
    identity: dict[str, Any],
    flags: set[str],
    event_date: str,
    clause: str,
) -> dict[str, Any]:
    return {
        "symbol": row["symbol"].upper(),
        "symbol_era_id": row["symbol_era_id"],
        "event_type": _event_type(flags, clause),
        "event_date": event_date,
        "date_basis": "explicit_same_clause",
        "old_symbol": row["symbol"].upper(),
        "new_symbol": "",
        "subject_cik": identity.get("entity_id", ""),
        "filer_cik": identity.get("entity_id", ""),
        "form": row.get("event_form", ""),
        "accession": row.get("event_accession_no", ""),
        "source": row.get("event_document_url", ""),
        "flags": sorted(flags | {"cached_v2_semantic_date"}),
        "snippet": clause[:1_000],
        "verification_state": VERIFIED,
    }


def _event_type(flags: set[str], snippet: str) -> str:
    text = snippet.upper()
    if "delisting_language" in flags or "DELIST" in text or "CEASED TRADING" in text:
        return "delisting_or_registration_termination"
    if "MERGER" in text or "ACQUISITION" in text:
        return "merger_or_acquisition_terminal"
    return "operating_lifecycle_terminal"
