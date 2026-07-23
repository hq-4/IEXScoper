from __future__ import annotations

import re
from datetime import date, timedelta
from typing import Any

from utils.resolution_v2_events import (
    ACTION_TERMS,
    PROSPECTIVE_TERMS,
    normalize_filing_text,
    select_stratified_filings,
    semantic_action_date,
    symbol_change_proof,
)
from utils.resolution_v2_identity import entity_selector_candidates
from utils.resolution_v2_network import CachedPrimaryClient, paged_sec_search
from utils.resolution_v2_registry import CachePolicy
from utils.resolution_v2_schema import VERIFIED, prepare_fact
from utils.sec_identity_evidence import identity_result
from utils.sec_identity_sources import evidence_from_payload


def resolve_sec_identity(
    row: dict[str, Any], client: CachedPrimaryClient, policy: CachePolicy
) -> tuple[dict[str, Any] | None, list[str]]:
    first, last = _day(row.get("first_day")), _day(row.get("last_day"))
    if not first or not last:
        return None, ["valid_era_interval"]
    evidence = []
    for payload in paged_sec_search(client, row["symbol"], first, last, policy):
        entity_selector_candidates(payload, row["symbol"])
        evidence.extend(evidence_from_payload(payload, source="sec_efts_search"))
        result = identity_result(row, evidence, days_before=0, days_after=0)
        if result["identity_bucket"] == "identity_collision_hold":
            return None, ["unique_date_scoped_entity"]
        if result["identity_bucket"] == "identity_verified_ready":
            return _identity_fact(row, result), []
    return None, ["exact_filer_ticker_or_dei", "unique_date_scoped_entity"]


def resolve_known_identity_event(
    row: dict[str, Any], identity: dict[str, Any], client: CachedPrimaryClient
) -> tuple[dict[str, Any] | None, list[str]]:
    cik = str(identity.get("entity_id") or "").lstrip("0")
    boundary = _day(row.get("last_day"))
    if not cik or not boundary:
        return None, ["identity_cik", "valid_last_day"]
    payload, _ = client.get_json(
        "sec_submissions",
        f"https://data.sec.gov/submissions/CIK{int(cik):010d}.json",
        {},
        CachePolicy(max_age=timedelta(days=30)),
    )
    filings = _submission_filings(payload, cik, boundary)
    for filing in select_stratified_filings(filings, boundary):
        fact = _review_document(row, identity, filing, client)
        if fact:
            return fact, []
    return None, ["hard_gated_endpoint_event"]


def _review_document(
    row: dict[str, Any],
    identity: dict[str, Any],
    filing: dict[str, Any],
    client: CachedPrimaryClient,
) -> dict[str, Any] | None:
    url = str(filing.get("document_url") or "")
    if not url:
        return None
    text = client.get_text("filing_document", url)
    boundary = _day(row.get("last_day"))
    event_date, snippet = semantic_action_date(text, boundary)
    client.registry.record_document(url, text, _document_metadata(filing, identity), [snippet])
    change = _symbol_change(row, identity, filing, text, boundary)
    if change:
        return change
    if not _terminal_gate(row, identity, filing, text, snippet, event_date, boundary):
        return None
    return _terminal_fact(row, identity, filing, event_date, snippet, url)


def _terminal_fact(
    row: dict[str, Any],
    identity: dict[str, Any],
    filing: dict[str, Any],
    event_date: date,
    snippet: str,
    url: str,
) -> dict[str, Any]:
    return prepare_fact(
        "event",
        {
            "symbol": row["symbol"].upper(),
            "symbol_era_id": row["symbol_era_id"],
            "event_type": _terminal_type(snippet),
            "event_date": event_date.isoformat(),
            "date_basis": "explicit_same_clause",
            "old_symbol": row["symbol"].upper(),
            "new_symbol": "",
            "subject_cik": identity["entity_id"],
            "filer_cik": filing["filer_cik"],
            "form": filing["form"],
            "accession": filing["accession_no"],
            "source": url,
            "flags": ["same_cik", "actual_event", "near_observed_boundary"],
            "snippet": snippet[:1_000],
            "verification_state": VERIFIED,
        },
    )


def _terminal_gate(
    row: dict[str, Any],
    identity: dict[str, Any],
    filing: dict[str, Any],
    text: str,
    snippet: str,
    event_date: date | None,
    boundary: date | None,
) -> bool:
    normalized = normalize_filing_text(text).upper()
    symbol = row["symbol"].upper()
    issuer = str(identity.get("issuer") or "").upper()
    anchored = _exact_token(symbol, normalized) or (issuer and issuer in normalized)
    same_cik = str(filing.get("filer_cik") or "").lstrip("0") == str(identity["entity_id"]).lstrip(
        "0"
    )
    near = bool(event_date and boundary and abs((event_date - boundary).days) <= 14)
    actual = any(term.upper() in snippet.upper() for term in ACTION_TERMS)
    prospective = any(term in snippet.lower() for term in PROSPECTIVE_TERMS)
    return bool(anchored and same_cik and near and actual and not prospective)


def _symbol_change(
    row: dict[str, Any],
    identity: dict[str, Any],
    filing: dict[str, Any],
    text: str,
    boundary: date | None,
) -> dict[str, Any] | None:
    confirmation = {
        "cik": identity.get("entity_id"),
        "tickers": identity.get("related_symbols", []),
        "filing_date": identity.get("evidence_date", ""),
    }
    result = symbol_change_proof(
        row["symbol"],
        text,
        {"subject_cik": identity["entity_id"], "boundary": boundary},
        [confirmation],
    )
    if result["verification_state"] != VERIFIED:
        return None
    return _symbol_fact(row, identity, filing, result)


def _symbol_fact(
    row: dict[str, Any],
    identity: dict[str, Any],
    filing: dict[str, Any],
    result: dict[str, Any],
) -> dict[str, Any]:
    return prepare_fact(
        "event",
        {
            "symbol": row["symbol"].upper(),
            "symbol_era_id": row["symbol_era_id"],
            **result,
            "date_basis": "explicit_plus_post_effective_confirmation",
            "subject_cik": identity["entity_id"],
            "filer_cik": filing["filer_cik"],
            "form": filing["form"],
            "accession": filing["accession_no"],
            "source": filing["document_url"],
        },
    )


def _submission_filings(payload: dict[str, Any], cik: str, boundary: date) -> list[dict[str, Any]]:
    recent = payload.get("filings", {}).get("recent", {})
    if not isinstance(recent, dict):
        return []
    columns = {key: value for key, value in recent.items() if isinstance(value, list)}
    count = min((len(value) for value in columns.values()), default=0)
    filings = []
    for index in range(count):
        filing = _filing_row(columns, index, cik)
        filed = _day(filing["filing_date"])
        if filed and boundary - timedelta(days=180) <= filed <= boundary + timedelta(days=90):
            filings.append(filing)
    return filings


def _filing_row(columns: dict[str, list[Any]], index: int, cik: str) -> dict[str, Any]:
    accession = _cell(columns, "accessionNumber", index)
    document = _cell(columns, "primaryDocument", index)
    url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}/{document}"
    return {
        "accession_no": accession,
        "filing_date": _cell(columns, "filingDate", index),
        "form": _cell(columns, "form", index),
        "document_url": url,
        "role": _cell(columns, "primaryDocDescription", index),
        "filer_cik": cik,
        "subject_cik": cik,
    }


def _identity_fact(row: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    return prepare_fact(
        "identity",
        {
            "symbol": row["symbol"].upper(),
            "symbol_era_id": row["symbol_era_id"],
            "valid_from": row["first_day"],
            "valid_through": row["last_day"],
            "entity_id": result["identity_cik"],
            "issuer": result["identity_issuer_name"],
            "instrument": row.get("instrument_type", ""),
            "evidence_method": "sec_date_scoped_display_names",
            "source": result["identity_document_url"],
            "evidence_date": result["identity_evidence_date"],
            "related_symbols": result["identity_tickers"].split("|"),
            "verification_state": VERIFIED,
            "flags": result["identity_flags"].split("|"),
        },
    )


def _document_metadata(filing: dict[str, Any], identity: dict[str, Any]) -> dict[str, Any]:
    return {
        "filing_date": filing["filing_date"],
        "filer_cik": filing["filer_cik"],
        "subject_cik": identity["entity_id"],
        "document_role": filing.get("role", "primary"),
        "source_authority": "sec_edgar",
        "form": filing["form"],
        "accession": filing["accession_no"],
    }


def _terminal_type(snippet: str) -> str:
    normalized = snippet.upper()
    return (
        "merger_or_acquisition_terminal"
        if "MERGER" in normalized
        else "delisting_or_registration_termination"
    )


def _exact_token(symbol: str, text: str) -> bool:
    return bool(re.search(rf"(?<![A-Z0-9]){re.escape(symbol)}(?![A-Z0-9])", text))


def _day(value: Any) -> date | None:
    from utils.sec_identity_evidence import parse_day

    return parse_day(value)


def _cell(columns: dict[str, list[Any]], name: str, index: int) -> str:
    values = columns.get(name, [])
    return str(values[index]) if index < len(values) else ""
