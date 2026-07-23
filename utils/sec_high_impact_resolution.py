from __future__ import annotations

import re
from datetime import date
from typing import Any

from utils.sec_identity_evidence import parse_day
from utils.sec_terminal_text_evidence import (
    PASS_BUCKET,
    extract_date,
    normalize,
    text_evidence_result,
)

SYMBOL_CHANGE_PASS = "symbol_change_verified_ready"
CHANGE_TERMS = (
    "changed its ticker symbol",
    "ticker symbol changed",
    "began trading under",
    "commenced trading under",
    "effective",
)
PROSPECTIVE_TERMS = ("will begin", "expects to", "expected to", "subject to", "intends to")
CHANGE_PATTERN = re.compile(
    r"(?:TICKER\s+)?SYMBOL\s+(?:CHANGED\s+)?FROM\s+([A-Z0-9.^+=-]+)\s+TO\s+([A-Z0-9.^+=-]+)"
)
SNIPPET_RADIUS = 400


def resolve_event_text(
    row: dict[str, Any],
    text: str,
    document_url: str,
    *,
    filing_cik: str,
    form: str = "",
    accession_no: str = "",
) -> dict[str, Any]:
    change = symbol_change_result(row, text, document_url, filing_cik=filing_cik)
    if change["symbol_change_bucket"] == SYMBOL_CHANGE_PASS:
        return resolution_from_change(change, form, accession_no)
    scoring_row = {
        **row,
        "proposed_historical_issuer_name": row.get("identity_issuer_name")
        or row.get("proposed_historical_issuer_name"),
        "entity": (
            f"{row.get('identity_issuer_name', '')} ({row.get('symbol', '')}) "
            f"(CIK {row.get('identity_cik', '')})"
        ),
    }
    terminal = text_evidence_result(
        scoring_row,
        text,
        document_url,
        identity_cik=str(row.get("identity_cik") or ""),
        filing_cik=filing_cik,
    )
    flags = set(str(terminal["terminal_text_flags"]).split("|"))
    verified = terminal["terminal_text_bucket"] == PASS_BUCKET and "cik_identity_match" in flags
    return resolution_from_terminal(terminal, verified, form, accession_no)


def symbol_change_result(
    row: dict[str, Any], text: str, document_url: str, *, filing_cik: str
) -> dict[str, Any]:
    snippet = best_change_snippet(text)
    normalized = normalize(snippet)
    match = CHANGE_PATTERN.search(normalized)
    old, successor = match.groups() if match else ("", "")
    event_date = extract_date(snippet)
    distance = date_distance(row, event_date)
    flags = change_flags(row, normalized, old, successor, event_date, distance, filing_cik)
    blocked = {"prospective_language", "ticker_collision"}.intersection(flags)
    ready = required_change_flags() <= flags and not blocked
    return {
        "symbol_change_bucket": SYMBOL_CHANGE_PASS if ready else "symbol_change_hold",
        "symbol_change_score": len(required_change_flags() & flags) * 20,
        "symbol_change_reason": f"flags={'+'.join(sorted(flags)) or 'none'}; days={distance}",
        "symbol_change_flags": "|".join(sorted(flags)),
        "symbol_change_snippet": snippet,
        "symbol_change_date": event_date.isoformat() if event_date else "",
        "symbol_change_date_distance_days": distance,
        "symbol_change_successor": successor,
        "symbol_change_document_url": document_url,
    }


def best_change_snippet(text: str) -> str:
    matches = []
    for term in CHANGE_TERMS:
        for match in re.finditer(re.escape(term), text, flags=re.IGNORECASE):
            start, end = max(0, match.start() - SNIPPET_RADIUS), match.end() + SNIPPET_RADIUS
            matches.append(text[start:end])
    return max(matches, key=change_snippet_score).strip() if matches else ""


def change_snippet_score(snippet: str) -> int:
    normalized = normalize(snippet)
    return (50 if CHANGE_PATTERN.search(normalized) else 0) + (30 if extract_date(snippet) else 0)


def change_flags(
    row: dict[str, Any],
    snippet: str,
    old: str,
    successor: str,
    event_date: date | None,
    distance: int | None,
    filing_cik: str,
) -> set[str]:
    symbol = str(row.get("symbol") or "").upper()
    flags: set[str] = set()
    if old == symbol:
        flags.add("old_ticker_match")
    if successor and successor != symbol:
        flags.add("different_successor_ticker")
    if any(normalize(term) in snippet for term in CHANGE_TERMS):
        flags.add("actual_change_language")
    if event_date:
        flags.add("event_date_extracted")
    if distance is not None and abs(distance) <= 14:
        flags.add("event_date_near_original_last")
    if clean_cik(row.get("identity_cik")) == clean_cik(filing_cik) and clean_cik(filing_cik):
        flags.add("cik_identity_match")
    if any(normalize(term) in snippet for term in PROSPECTIVE_TERMS):
        flags.add("prospective_language")
    if old and old != symbol:
        flags.add("ticker_collision")
    return flags


def required_change_flags() -> set[str]:
    return {
        "old_ticker_match",
        "different_successor_ticker",
        "actual_change_language",
        "event_date_extracted",
        "event_date_near_original_last",
        "cik_identity_match",
    }


def date_distance(row: dict[str, Any], event_date: date | None) -> int | None:
    last = parse_day(row.get("original_last_day") or row.get("last_day"))
    return (last - event_date).days if last and event_date else None


def resolution_from_change(change: dict[str, Any], form: str, accession: str) -> dict[str, Any]:
    return {
        "resolution_bucket": SYMBOL_CHANGE_PASS,
        "event_type": "symbol_change",
        "event_date": change["symbol_change_date"],
        "event_successor": change["symbol_change_successor"],
        "event_flags": change["symbol_change_flags"],
        "event_snippet": change["symbol_change_snippet"],
        "event_document_url": change["symbol_change_document_url"],
        "event_form": form,
        "event_accession_no": accession,
        "event_date_distance_days": change["symbol_change_date_distance_days"],
        "importable": True,
    }


def resolution_from_terminal(
    terminal: dict[str, Any], verified: bool, form: str, accession: str
) -> dict[str, Any]:
    snippet = str(terminal["terminal_text_snippet"])
    return {
        "resolution_bucket": "terminal_verified_ready" if verified else "identity_only_hold",
        "event_type": terminal_event_type(snippet) if verified else "",
        "event_date": terminal["terminal_text_date"] or "",
        "event_successor": "",
        "event_flags": terminal["terminal_text_flags"],
        "event_snippet": snippet,
        "event_document_url": terminal["terminal_text_document_url"],
        "event_form": form,
        "event_accession_no": accession,
        "event_date_distance_days": terminal["days_terminal_text_to_original_last"],
        "importable": verified,
    }


def terminal_event_type(snippet: str) -> str:
    value = normalize(snippet)
    if "BANKRUPT" in value:
        return "bankruptcy_delisted"
    if any(term in value for term in ("DELIST", "FORM 25", "TERMINATED REGISTRATION", "SUSPENDED")):
        return "delisting_or_registration_termination"
    if any(term in value for term in ("MERGER", "ACQUISITION")):
        return "merger_or_acquisition_terminal"
    return "operating_lifecycle_terminal"


def clean_cik(value: Any) -> str:
    return re.sub(r"\D", "", str(value or "")).lstrip("0")
