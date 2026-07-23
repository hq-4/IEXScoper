from __future__ import annotations

import html
import re
from datetime import date
from typing import Any, Iterable

from utils.sec_identity_evidence import parse_day

MONTHS = {
    name: number
    for number, names in enumerate(
        (
            ("JANUARY", "JAN"),
            ("FEBRUARY", "FEB"),
            ("MARCH", "MAR"),
            ("APRIL", "APR"),
            ("MAY",),
            ("JUNE", "JUN"),
            ("JULY", "JUL"),
            ("AUGUST", "AUG"),
            ("SEPTEMBER", "SEP", "SEPT"),
            ("OCTOBER", "OCT"),
            ("NOVEMBER", "NOV"),
            ("DECEMBER", "DEC"),
        ),
        start=1,
    )
    for name in names
}
MONTH_TOKEN = "|".join(sorted(MONTHS, key=len, reverse=True))
ACTION_TERMS = (
    "completed the merger",
    "consummated the merger",
    "transaction closed",
    "ceased trading",
    "suspended from trading",
    "delisted",
    "became effective",
    "changed its ticker",
    "began trading under",
    "commenced trading under",
)
POSTPONEMENT_TERMS = ("withdraw", "postpon", "rescinded", "cancelled", "canceled")
PROSPECTIVE_TERMS = ("will", "expects to", "expected to", "intends to", "subject to")
SENTENCE_SCAN_LIMIT = 2_000
CHANGE_PATTERNS = (
    re.compile(
        r"(?:TICKER\s+)?SYMBOL\s+(?:WILL\s+)?CHANGE(?:D)?\s+FROM\s+([A-Z0-9.^+=-]+)\s+TO\s+([A-Z0-9.^+=-]+)"
    ),
    re.compile(
        r"FROM\s+(?:THE\s+)?(?:TICKER\s+)?SYMBOL\s+([A-Z0-9.^+=-]+)\s+TO\s+([A-Z0-9.^+=-]+)"
    ),
    re.compile(
        r"UNDER\s+(?:THE\s+)?(?:NEW\s+)?(?:TICKER\s+)?SYMBOL\s+([A-Z0-9.^+=-]+).*?FORMERLY\s+([A-Z0-9.^+=-]+)"
    ),
)


def normalize_filing_text(value: str) -> str:
    decoded = html.unescape(str(value or ""))
    without_tags = re.sub(r"<[^>]+>", " ", decoded)
    normalized = re.sub(r"\s+", " ", without_tags).strip()
    corrupt = rf"\b({MONTH_TOKEN})\.?\s+160\s+([0-9]{{1,2}}(?:ST|ND|RD|TH)?\b)"
    return re.sub(corrupt, r"\1 \2", normalized, flags=re.IGNORECASE)


def date_candidates(value: str) -> list[dict[str, Any]]:
    text = normalize_filing_text(value)
    found: list[dict[str, Any]] = []
    month_pattern = re.compile(
        rf"\b({MONTH_TOKEN})\.?\s+([0-9]{{1,2}})(?:ST|ND|RD|TH)?(?:,)?\s+([0-9]{{4}})\b",
        re.IGNORECASE,
    )
    for match in month_pattern.finditer(text):
        parsed = _safe_date(
            int(match.group(3)), MONTHS[match.group(1).upper()], int(match.group(2))
        )
        if parsed:
            found.append(_date_row(parsed, match, text))
    patterns = (
        re.compile(r"\b([0-9]{4})[-/]([0-9]{1,2})[-/]([0-9]{1,2})\b"),
        re.compile(r"\b([0-9]{1,2})/([0-9]{1,2})/([0-9]{4})\b"),
    )
    for index, pattern in enumerate(patterns):
        for match in pattern.finditer(text):
            parts = tuple(int(item) for item in match.groups())
            parsed = _safe_date(*parts) if index == 0 else _safe_date(parts[2], parts[0], parts[1])
            if parsed:
                found.append(_date_row(parsed, match, text))
    return sorted(
        {(item["date"], item["start"]): item for item in found}.values(),
        key=lambda item: item["start"],
    )


def semantic_action_date(
    value: str, boundary: date | str | None, action_terms: Iterable[str] = ACTION_TERMS
) -> tuple[date | None, str]:
    text = normalize_filing_text(value)
    candidates = date_candidates(text)
    action_positions = [
        match.start()
        for term in action_terms
        for match in re.finditer(re.escape(term), text, re.IGNORECASE)
    ]
    if not candidates or not action_positions:
        return None, ""
    target = boundary if isinstance(boundary, date) else parse_day(boundary)
    ranked = sorted(
        candidates, key=lambda item: _candidate_rank(item, action_positions, target, text)
    )
    chosen = ranked[0]
    return chosen["date"], _bounded_clause(text, chosen["start"], action_positions)


def select_stratified_filings(
    filings: list[dict[str, Any]], boundary: date
) -> list[dict[str, Any]]:
    from utils.resolution_v2_filings import select_stratified_filings as select

    return select(filings, boundary)


def form25_event(
    filing: dict[str, Any], text: str, *, anchored_subject_cik: str, exact_security: bool
) -> dict[str, Any]:
    from utils.resolution_v2_regulatory import form25_event as parse

    return parse(
        filing, text, anchored_subject_cik=anchored_subject_cik, exact_security=exact_security
    )


def form15_event(
    filing: dict[str, Any], text: str, *, anchored_subject_cik: str, exact_security: bool
) -> dict[str, Any]:
    from utils.resolution_v2_regulatory import form15_event as parse

    return parse(
        filing, text, anchored_subject_cik=anchored_subject_cik, exact_security=exact_security
    )


def symbol_change_proof(
    symbol: str,
    announcement_text: str,
    announcement: dict[str, Any],
    confirmations: list[dict[str, Any]],
) -> dict[str, Any]:
    old, new = _parse_symbol_change(announcement_text)
    event_date, snippet = semantic_action_date(
        announcement_text,
        announcement.get("boundary"),
        (*ACTION_TERMS, "ticker symbol will change", "ticker symbol changed", "change from"),
    )
    same_cik = _clean_cik(announcement.get("subject_cik"))
    confirmed = [
        item for item in confirmations if _valid_confirmation(item, new, same_cik, event_date)
    ]
    prospective = any(term in snippet.lower() for term in PROSPECTIVE_TERMS)
    collision = bool(old and old != symbol.upper())
    ready = bool(old == symbol.upper() and new and event_date and confirmed and not collision)
    if prospective and not confirmed:
        ready = False
    return _change_output(old, new, event_date, snippet, bool(confirmed), ready)


def _change_output(
    old: str,
    new: str,
    event_date: date | None,
    snippet: str,
    confirmed: bool,
    ready: bool,
) -> dict[str, Any]:
    return {
        "verification_state": "verified" if ready else "event_candidate",
        "event_type": "symbol_change",
        "event_date": event_date.isoformat() if event_date else "",
        "old_symbol": old,
        "new_symbol": new,
        "flags": ["two_source_temporal_proof" if confirmed else "confirmation_missing"],
        "snippet": snippet[:1_000],
    }


def _candidate_rank(
    item: dict[str, Any], actions: list[int], boundary: date | None, text: str
) -> tuple[int, int, int]:
    nearest_action = min(abs(item["start"] - position) for position in actions)
    same_clause = int(
        not any(_same_sentence(text, item["start"], position) for position in actions)
    )
    boundary_distance = abs((item["date"] - boundary).days) if boundary else 0
    return same_clause, nearest_action, boundary_distance


def _same_sentence(text: str, first: int, second: int) -> bool:
    left, right = sorted((first, second))
    if right - left > SENTENCE_SCAN_LIMIT:
        return False
    return re.search(r"[.!?]\s+(?=[A-Z])", text[left:right]) is None


def _bounded_clause(text: str, date_position: int, actions: list[int]) -> str:
    action = min(actions, key=lambda position: abs(position - date_position))
    center = min(date_position, action), max(date_position, action)
    left = max(text.rfind(".", 0, center[0]) + 1, center[0] - 500)
    right_stop = text.find(".", center[1])
    right = min(len(text), right_stop + 1 if right_stop >= 0 else center[1] + 500)
    return text[left:right].strip()[:1_000]


def _parse_symbol_change(text: str) -> tuple[str, str]:
    normalized = re.sub(r"[^A-Z0-9.^+=-]+", " ", normalize_filing_text(text).upper())
    for index, pattern in enumerate(CHANGE_PATTERNS):
        if match := pattern.search(normalized):
            first, second = match.groups()
            first, second = first.strip("."), second.strip(".")
            return (second, first) if index == 2 else (first, second)
    return "", ""


def _valid_confirmation(item: dict[str, Any], new: str, cik: str, event_date: date | None) -> bool:
    filed = parse_day(item.get("filing_date") or item.get("effective_date"))
    tickers = {str(value).upper() for value in item.get("tickers", [])}
    return bool(
        new in tickers
        and _clean_cik(item.get("cik")) == cik
        and filed
        and event_date
        and filed >= event_date
    )


def _date_row(parsed: date, match: re.Match[str], text: str) -> dict[str, Any]:
    return {
        "date": parsed,
        "start": match.start(),
        "end": match.end(),
        "raw": text[match.start() : match.end()],
    }


def _safe_date(year: int, month: int, day: int) -> date | None:
    try:
        return date(year, month, day)
    except ValueError:
        return None


def _clean_cik(value: Any) -> str:
    return re.sub(r"\D", "", str(value or "")).lstrip("0")
