from __future__ import annotations

import re
from datetime import timedelta
from typing import Any

from utils.resolution_v2_events import (
    POSTPONEMENT_TERMS,
    normalize_filing_text,
    semantic_action_date,
)
from utils.sec_identity_evidence import parse_day


def form25_event(
    filing: dict[str, Any], text: str, *, anchored_subject_cik: str, exact_security: bool
) -> dict[str, Any]:
    filed = parse_day(filing.get("filing_date"))
    filer, subject = _clean_cik(filing.get("filer_cik")), _clean_cik(filing.get("subject_cik"))
    blocked = any(term in normalize_filing_text(text).lower() for term in POSTPONEMENT_TERMS)
    anchored = bool(subject and subject == _clean_cik(anchored_subject_cik) and exact_security)
    verified = bool(filed and anchored and not blocked)
    return {
        "verification_state": "verified" if verified else "event_candidate",
        "event_type": "exchange_delisting",
        "event_date": (filed + timedelta(days=10)).isoformat() if verified else "",
        "date_basis": "regulatory_rule" if verified else "",
        "filer_cik": filer,
        "subject_cik": subject,
        "flags": _flags(filer, subject, anchored, blocked),
    }


def form15_event(
    filing: dict[str, Any], text: str, *, anchored_subject_cik: str, exact_security: bool
) -> dict[str, Any]:
    filer, subject = _clean_cik(filing.get("filer_cik")), _clean_cik(filing.get("subject_cik"))
    filed = parse_day(filing.get("filing_date"))
    event_date, snippet = semantic_action_date(
        text, filed, ("termination of registration became effective", "registration was terminated")
    )
    blocked = any(term in normalize_filing_text(text).lower() for term in POSTPONEMENT_TERMS)
    anchored = bool(subject == _clean_cik(anchored_subject_cik) and exact_security)
    verified = bool(event_date and anchored and not blocked)
    return {
        "verification_state": "verified" if verified else "event_candidate",
        "event_type": "registration_termination",
        "event_date": event_date.isoformat() if verified else "",
        "date_basis": "explicit_form_text" if verified else "",
        "filing_date": filed.isoformat() if filed else "",
        "filer_cik": filer,
        "subject_cik": subject,
        "flags": _flags(filer, subject, anchored, blocked),
        "snippet": snippet[:1_000],
    }


def _flags(filer: str, subject: str, anchored: bool, blocked: bool) -> list[str]:
    flags = {"exchange_filer_distinct"} if filer and filer != subject else set()
    if anchored:
        flags.add("exact_subject_security")
    if blocked:
        flags.add("withdrawal_or_postponement")
    return sorted(flags)


def _clean_cik(value: Any) -> str:
    return re.sub(r"\D", "", str(value or "")).lstrip("0")
