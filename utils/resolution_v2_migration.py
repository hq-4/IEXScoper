from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from utils.resolution_v2_schema import (
    DEFAULT_LEDGER_PATH,
    DEFAULT_OVERRIDE_PATH,
    DEFAULT_REVIEW_PATH,
    DEFAULT_WORKPLAN_PATH,
    EVENT_CANDIDATE,
    RESEARCH_CLOSED,
    UNRESOLVED,
    VERIFIED,
    prepare_fact,
)

LATEST_IDENTITY_STATE = Path(
    "reports/dead-ticker-review/sec-high-impact-identity-resolution-round3/run_state.json"
)


def build_legacy_migration(
    review_path: Path = DEFAULT_REVIEW_PATH,
    override_path: Path = DEFAULT_OVERRIDE_PATH,
    ledger_path: Path = DEFAULT_LEDGER_PATH,
    workplan_path: Path = DEFAULT_WORKPLAN_PATH,
    identity_state_path: Path = LATEST_IDENTITY_STATE,
) -> dict[str, Any]:
    cohort = _read_csv(review_path)
    by_era = {row["symbol_era_id"]: row for row in cohort}
    overrides = _read_csv(override_path)
    ledger = _read_csv(ledger_path)
    identities = [
        _override_identity(row, by_era.get(row["symbol_era_id"], {})) for row in overrides
    ]
    events = [_override_event(row) for row in overrides]
    identities.extend(_identity_holds(identity_state_path, by_era))
    observations = [_observation(row) for row in cohort]
    decisions = _decisions(cohort, identities, events, ledger)
    attempts = _lifecycle_attempts(workplan_path)
    records = (identities, events, observations, decisions, attempts)
    return _migration_payload(cohort, records, overrides, ledger)


def _migration_payload(
    cohort: list[dict[str, str]],
    records: tuple[list[dict[str, Any]], ...],
    overrides: list[dict[str, str]],
    ledger: list[dict[str, str]],
) -> dict[str, Any]:
    identities, events, observations, decisions, attempts = records
    return {
        "cohort": cohort,
        "identity": _unique_facts(identities),
        "event": _unique_facts(events),
        "observation": _unique_facts(observations),
        "decision": _unique_facts(decisions),
        "attempt": _unique_facts(attempts),
        "migration_counts": _migration_counts(
            cohort, overrides, ledger, identities, events, attempts
        ),
    }


def _migration_counts(
    cohort: list[dict[str, str]],
    overrides: list[dict[str, str]],
    ledger: list[dict[str, str]],
    identities: list[dict[str, Any]],
    events: list[dict[str, Any]],
    attempts: list[dict[str, Any]],
) -> dict[str, int]:
    return {
        "legacy_identities": len(overrides),
        "verified_events": sum(item["verification_state"] == VERIFIED for item in events),
        "event_candidates": sum(item["verification_state"] == EVENT_CANDIDATE for item in events),
        "promoted_identity_holds": len(identities) - len(overrides),
        "research_closures": len(ledger),
        "lifecycle_v1_attempts": len(attempts),
        "cohort_rows": len(cohort),
    }


def _override_identity(row: dict[str, str], era: dict[str, str]) -> dict[str, Any]:
    return prepare_fact(
        "identity",
        {
            "symbol": row["symbol"].upper(),
            "symbol_era_id": row["symbol_era_id"],
            "valid_from": era.get("first_day", ""),
            "valid_through": era.get("last_day", ""),
            "entity_id": "",
            "issuer": row["historical_issuer_name"],
            "instrument": era.get("instrument_type", ""),
            "evidence_method": "legacy_historical_override",
            "source": row["source_url"],
            "verification_state": VERIFIED,
            "flags": ["migrated_without_entity_id"],
        },
    )


def _override_event(row: dict[str, str]) -> dict[str, Any]:
    candidate = row["historical_event_type"].endswith("_lead")
    return prepare_fact(
        "event",
        {
            "symbol": row["symbol"].upper(),
            "symbol_era_id": row["symbol_era_id"],
            "event_type": row["historical_event_type"],
            "event_date": row["historical_event_date"],
            "date_basis": "legacy_asserted",
            "old_symbol": row["symbol"].upper(),
            "new_symbol": row["historical_successor"],
            "subject_cik": "",
            "filer_cik": "",
            "form": "",
            "accession": "",
            "source": row["source_url"],
            "flags": ["migrated_event_lead"] if candidate else ["migrated_verified_event"],
            "snippet": str(row.get("source_note") or "")[:1_000],
            "verification_state": EVENT_CANDIDATE if candidate else VERIFIED,
        },
    )


def _identity_holds(path: Path, by_era: dict[str, dict[str, str]]) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    state = json.loads(path.read_text(encoding="utf-8"))
    output = []
    for era_id, entry in state.get("rows", {}).items():
        if entry.get("bucket") != "identity_only_hold":
            continue
        result, era = entry.get("result", {}), by_era.get(era_id, {})
        output.append(_hold_identity(era_id, era, result))
    return output


def _hold_identity(era_id: str, era: dict[str, str], result: dict[str, Any]) -> dict[str, Any]:
    symbol = str(era.get("symbol") or era_id.split("#", 1)[0]).upper()
    return prepare_fact(
        "identity",
        {
            "symbol": symbol,
            "symbol_era_id": era_id,
            "valid_from": era.get("first_day", ""),
            "valid_through": era.get("last_day", ""),
            "entity_id": str(result.get("identity_cik") or ""),
            "issuer": str(result.get("identity_issuer_name") or ""),
            "instrument": era.get("instrument_type", ""),
            "evidence_method": "sec_date_scoped_display_names",
            "source": str(result.get("identity_document_url") or ""),
            "evidence_date": str(result.get("identity_evidence_date") or ""),
            "related_symbols": _flags(result.get("identity_tickers")),
            "verification_state": VERIFIED,
            "flags": _flags(result.get("identity_flags")),
        },
    )


def _observation(row: dict[str, str]) -> dict[str, Any]:
    return prepare_fact(
        "observation",
        {
            "symbol": row["symbol"].upper(),
            "symbol_era_id": row["symbol_era_id"],
            "boundary": "observed_interval",
            "first_day": row["first_day"],
            "last_day": row["last_day"],
            "gap_status": row["source_classification"],
            "related_eras": [],
            "evidence": "local_iex_tops_observation",
            "verification_state": "local_observed",
        },
    )


def _decisions(
    cohort: list[dict[str, str]],
    identities: list[dict[str, Any]],
    events: list[dict[str, Any]],
    ledger: list[dict[str, str]],
) -> list[dict[str, Any]]:
    identity_ids = {
        item["symbol_era_id"] for item in identities if item["verification_state"] == VERIFIED
    }
    event_states = {item["symbol_era_id"]: item["verification_state"] for item in events}
    closures = {item["symbol_era_id"]: item for item in ledger}
    return [
        _decision(row, identity_ids, event_states, closures.get(row["symbol_era_id"]))
        for row in cohort
    ]


def _decision(
    row: dict[str, str],
    identity_ids: set[str],
    event_states: dict[str, str],
    closure: dict[str, str] | None,
) -> dict[str, Any]:
    era_id = row["symbol_era_id"]
    instrument = row.get("instrument_type", "")
    event_status = event_states.get(era_id, UNRESOLVED)
    research_status = RESEARCH_CLOSED if closure else "action_required"
    reason = _closure_reason(closure) if closure else ""
    eligible = era_id in identity_ids and event_status == VERIFIED and not _non_common(instrument)
    payload = {
        "symbol": row["symbol"].upper(),
        "symbol_era_id": era_id,
        "identity_status": VERIFIED if era_id in identity_ids else UNRESOLVED,
        "instrument_status": _instrument_status(instrument),
        "event_status": event_status,
        "observation_status": "local_observed",
        "research_status": research_status,
        "eligibility": "eligible" if eligible else "not_yet_eligible",
        "exclusion_reason": reason,
        "next_resolver": "" if closure else _next_resolver(era_id, identity_ids, event_status),
        "trade_rows": int(row.get("trade_rows") or 0),
    }
    return prepare_fact("decision", payload)


def _instrument_status(instrument: str) -> str:
    return "heuristic" if instrument and instrument != "unknown_instrument" else UNRESOLVED


def _lifecycle_attempts(path: Path) -> list[dict[str, Any]]:
    rows = _read_csv(path) if path.exists() else []
    return [
        prepare_fact(
            "attempt",
            {
                "symbol": row["symbol"].upper(),
                "symbol_era_id": row["symbol_era_id"],
                "resolver": "lifecycle_v1",
                "status": "automation_exhausted",
                "evidence_fingerprint": "legacy_lifecycle_v1_harvest",
                "missing_requirements": ["new_v2_evidence_strategy"],
            },
        )
        for row in rows
        if row.get("workplan_bucket") == "operating_lifecycle_search"
    ]


def _closure_reason(row: dict[str, str] | None) -> str:
    if not row:
        return ""
    if row.get("resolution_disposition") == "low_materiality_market_data_artifact":
        return "low_materiality_data_artifact"
    return "parent_link_workflow_closure"


def _next_resolver(era: str, identities: set[str], event: str) -> str:
    if era not in identities:
        return "identity_recovery_v2"
    return "known_identity_event_salvage_v2" if event != VERIFIED else "instrument_verification_v2"


def _non_common(value: str) -> bool:
    return value not in {"", "probable_operating_company", "unknown_instrument"}


def _flags(value: Any) -> list[str]:
    return sorted(item for item in str(value or "").split("|") if item)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _unique_facts(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return list({record["fact_id"]: record for record in records}.values())
