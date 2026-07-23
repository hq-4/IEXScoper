from __future__ import annotations

import re
from datetime import date
from typing import Any

from utils.sec_identity_evidence import parse_day
from utils.sec_terminal_text_evidence import extract_date, normalize

ACTION_TERMS = (
    "redemption",
    "redeemed",
    "expiration",
    "expired",
    "separation",
    "separated",
    "conversion",
    "converted",
    "delisting",
    "delisted",
    "cessation",
    "ceased trading",
)
SECURITY_CLASS_TERMS = ("warrant", "unit", "right", "preferred", "common shares", "class")


def derivative_resolution(
    row: dict[str, Any],
    parent_identity: dict[str, Any],
    snippet: str,
    *,
    filing_cik: str,
    parent_event_date: str = "",
) -> dict[str, Any]:
    symbol = str(row.get("symbol") or "").upper()
    instrument = str(row.get("instrument_type") or "")
    event_date = extract_date(snippet)
    flags = evidence_flags(row, parent_identity, snippet, filing_cik, event_date, parent_event_date)
    required = (
        share_class_requirements() if "share_class" in instrument else derivative_requirements()
    )
    ready = required <= flags
    return {
        "derivative_bucket": "derivative_verified_ready" if ready else "derivative_evidence_hold",
        "derivative_flags": "|".join(sorted(flags)),
        "parent_root_symbol": parent_root_symbol(symbol, instrument),
        "event_date": event_date.isoformat() if event_date else "",
        "event_type": derivative_event_type(instrument) if ready else "",
        "event_snippet": snippet[:800],
        "importable": ready,
        "destination": "resolution_ledger",
    }


def evidence_flags(
    row: dict[str, Any],
    parent_identity: dict[str, Any],
    snippet: str,
    filing_cik: str,
    event_date: date | None,
    parent_event_date: str,
) -> set[str]:
    normalized = normalize(snippet)
    symbol = str(row.get("symbol") or "").upper()
    flags: set[str] = set()
    if exact_symbol(symbol, snippet.upper()):
        flags.add("exact_child_ticker")
    if any(normalize(term) in normalized for term in SECURITY_CLASS_TERMS):
        flags.add("security_class_identified")
    if any(normalize(term) in normalized for term in ACTION_TERMS):
        flags.add("terminal_security_action")
    if near_last_day(row, event_date):
        flags.add("event_date_near_last_day")
    if clean_cik(parent_identity.get("identity_cik")) == clean_cik(filing_cik) and clean_cik(
        filing_cik
    ):
        flags.add("same_parent_cik")
    parent_date = parse_day(parent_event_date)
    if event_date and parent_date and abs((event_date - parent_date).days) <= 14:
        flags.add("parent_action_near_child_last_day")
    return flags


def derivative_requirements() -> set[str]:
    return {
        "exact_child_ticker",
        "security_class_identified",
        "terminal_security_action",
        "event_date_near_last_day",
        "same_parent_cik",
    }


def share_class_requirements() -> set[str]:
    return {
        "exact_child_ticker",
        "terminal_security_action",
        "event_date_near_last_day",
        "same_parent_cik",
        "parent_action_near_child_last_day",
    }


def parent_root_symbol(symbol: str, instrument_type: str = "") -> str:
    value = symbol.strip().upper()
    if re.search(r"[-.]", value):
        return re.split(r"[-.]", value, maxsplit=1)[0]
    for suffix in ("WTS", "WS", "WT", "RT"):
        if value.endswith(suffix) and len(value) > len(suffix):
            return value[: -len(suffix)]
    if any(token in instrument_type for token in ("warrant", "unit", "right")) and len(value) > 1:
        return value[:-1]
    return ""


def exact_symbol(symbol: str, normalized: str) -> bool:
    return bool(symbol and re.search(rf"(?<![A-Z0-9]){re.escape(symbol)}(?![A-Z0-9])", normalized))


def near_last_day(row: dict[str, Any], event_date: date | None) -> bool:
    last = parse_day(row.get("last_day") or row.get("original_last_day"))
    return bool(last and event_date and abs((last - event_date).days) <= 14)


def derivative_event_type(instrument: str) -> str:
    if "share_class" in instrument:
        return "share_class_terminal_action"
    if "preferred" in instrument:
        return "preferred_security_terminal_action"
    return "warrant_unit_right_terminal_action"


def clean_cik(value: Any) -> str:
    return re.sub(r"\D", "", str(value or "")).lstrip("0")
