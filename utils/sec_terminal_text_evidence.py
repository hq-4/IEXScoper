from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any

from utils.build_strict_terminal_review import parenthetical_tickers, split_flags
from utils.verify_sec_override_candidates import issuer_matches

MAX_DATE_DISTANCE_DAYS = 14
SNIPPET_RADIUS = 320
PASS_BUCKET = "terminal_text_verified_ready"
MONTHS = {
    name.upper(): index
    for index, name in enumerate(
        (
            "January",
            "February",
            "March",
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
        ),
        start=1,
    )
}
TERMINAL_TERMS = (
    "completed the merger",
    "consummated the merger",
    "closing of the merger",
    "became effective",
    "effective time",
    "transaction closed",
    "shares ceased trading",
    "delisted",
    "suspended from trading",
    "form 25",
    "terminated registration",
)
COMPLETION_TERMS = (
    "completed the merger",
    "consummated the merger",
    "closing of the merger",
    "became effective",
    "effective time",
    "transaction closed",
)
DELISTING_TERMS = (
    "shares ceased trading",
    "delisted",
    "suspended from trading",
    "form 25",
    "terminated registration",
)
ACTUAL_TERMS = (
    "completed the merger",
    "completed its merger",
    "consummated the merger",
    "completed its acquisition",
    "transaction closed",
    "has merged",
    "announcing the closing",
    "closed the merger",
    "became effective",
    "shares ceased trading",
    "was suspended",
    "ceased being traded",
    "delisted",
    "suspended from trading",
    "form 25",
    "terminated registration",
)
PROSPECTIVE_TERMS = (
    "expected to occur",
    "expect to close",
    "expected to close",
    "subject to the satisfaction",
    "subject to satisfaction",
    "expects to complete",
    "will close",
    "will be delisted",
    "intends to delist",
    "will cease trading",
)


def text_evidence_result(
    row: dict[str, Any],
    text: str,
    document_url: str,
    max_date_distance_days: int = MAX_DATE_DISTANCE_DAYS,
    *,
    identity_cik: str = "",
    filing_cik: str = "",
) -> dict[str, Any]:
    snippet = best_snippet(text)
    terminal_date = extract_date(snippet)
    days_to_last = days_terminal_to_original_last(row, terminal_date)
    flags = terminal_flags(row, text, snippet, terminal_date, days_to_last, max_date_distance_days)
    if clean_cik(identity_cik) and clean_cik(identity_cik) == clean_cik(filing_cik):
        flags = tuple(sorted({*flags, "cik_identity_match"}))
    return {
        "terminal_text_bucket": bucket_for_flags(flags),
        "terminal_text_score": terminal_score(flags),
        "terminal_text_reason": reason(flags, days_to_last),
        "terminal_text_snippet": snippet,
        "terminal_text_date": terminal_date.isoformat() if terminal_date else None,
        "days_terminal_text_to_original_last": days_to_last,
        "terminal_text_flags": "|".join(flags),
        "ticker_match_status": ticker_match_status(row, text, snippet),
        "parenthetical_tickers": "|".join(sorted(all_parenthetical_tickers(row, snippet))),
        "terminal_text_document_url": document_url,
    }


def empty_result(
    bucket: str, score: int, reason_text: str, flags: str, document_url: str | None
) -> dict[str, Any]:
    return {
        "terminal_text_bucket": bucket,
        "terminal_text_score": score,
        "terminal_text_reason": reason_text,
        "terminal_text_snippet": "",
        "terminal_text_date": None,
        "days_terminal_text_to_original_last": None,
        "terminal_text_flags": flags,
        "ticker_match_status": "unknown",
        "parenthetical_tickers": "",
        "terminal_text_document_url": document_url,
    }


def best_snippet(text: str) -> str:
    normalized = normalize(text)
    snippets = terminal_snippets(normalized)
    return max(snippets, key=snippet_quality).strip() if snippets else ""


def terminal_snippets(normalized: str) -> list[str]:
    snippets = []
    for term in TERMINAL_TERMS:
        start = normalized.find(normalize(term))
        while start >= 0:
            snippets.append(snippet_at(normalized, start))
            start = normalized.find(normalize(term), start + 1)
    return snippets


def snippet_at(normalized: str, start: int) -> str:
    return normalized[max(0, start - SNIPPET_RADIUS) : min(len(normalized), start + SNIPPET_RADIUS)]


def snippet_quality(snippet: str) -> int:
    score = 0
    if extract_date(snippet):
        score += 40
    if has_term(snippet, COMPLETION_TERMS):
        score += 25
    if has_term(snippet, DELISTING_TERMS):
        score += 25
    if has_term(snippet, TERMINAL_TERMS):
        score += 10
    return score


def terminal_flags(
    row: dict[str, Any],
    text: str,
    snippet: str,
    terminal_date: date | None,
    days_to_last: int | None,
    max_date_distance_days: int,
) -> tuple[str, ...]:
    flags = set(split_flags(row.get("verifier_flags")))
    if issuer_matches(row.get("proposed_historical_issuer_name"), normalize(text)):
        flags.add("issuer_name_match")
    flags.update(language_flags(snippet))
    flags.update(ticker_flags(row, text, snippet))
    if terminal_date:
        flags.add("terminal_date_extracted")
    if days_to_last is not None and abs(days_to_last) <= max_date_distance_days:
        flags.add("terminal_date_near_original_last")
    add_safety_flags(flags, row, text, snippet)
    return tuple(sorted(flags))


def language_flags(snippet: str) -> set[str]:
    flags = set()
    if has_term(snippet, TERMINAL_TERMS):
        flags.add("terminal_language")
    if has_term(snippet, COMPLETION_TERMS):
        flags.add("completion_language")
    if has_term(snippet, DELISTING_TERMS):
        flags.add("delisting_language")
    if has_term(snippet, ACTUAL_TERMS):
        flags.add("actual_terminal_language")
    if has_term(snippet, PROSPECTIVE_TERMS):
        flags.add("prospective_close_language")
    return flags


def ticker_flags(row: dict[str, Any], text: str, snippet: str) -> set[str]:
    symbol = normalize_symbol(row.get("symbol"))
    if symbol in all_parenthetical_tickers(row, snippet):
        return {"direct_ticker_evidence"}
    if direct_ticker_match(symbol, text) or direct_ticker_match(symbol, snippet):
        return {"direct_ticker_evidence"}
    return set()


def add_safety_flags(flags: set[str], row: dict[str, Any], text: str, snippet: str) -> None:
    if has_parenthetical_collision(row, snippet):
        flags.add("parenthetical_ticker_collision")
    if ambiguous_issuer_without_direct_ticker(row, text, snippet):
        flags.add("ambiguous_issuer_token_match")


def bucket_for_flags(flags: tuple[str, ...]) -> str:
    flag_set = set(flags)
    if "parenthetical_ticker_collision" in flag_set:
        return "reject_symbol_collision"
    if "terminal_language" not in flag_set:
        return "no_terminal_text_review"
    if "terminal_date_extracted" not in flag_set:
        return "missing_terminal_date_review"
    if "terminal_date_near_original_last" not in flag_set:
        return "date_mismatch_review"
    if "ambiguous_issuer_token_match" in flag_set:
        return "ambiguous_identity_review"
    if "prospective_close_language" in flag_set:
        return "prospective_close_review"
    if not credible_identity(flag_set):
        return "weak_identity_review"
    if terminal_close_language(flag_set) and "actual_terminal_language" in flag_set:
        return PASS_BUCKET
    return "terminal_text_review"


def credible_identity(flags: set[str]) -> bool:
    return "issuer_name_match" in flags or "direct_ticker_evidence" in flags


def terminal_score(flags: tuple[str, ...]) -> int:
    weights = {
        "issuer_name_match": 20,
        "direct_ticker_evidence": 20,
        "terminal_language": 25,
        "completion_language": 20,
        "delisting_language": 20,
        "terminal_date_extracted": 15,
        "terminal_date_near_original_last": 25,
        "actual_terminal_language": 20,
        "prospective_close_language": -50,
        "parenthetical_ticker_collision": -100,
        "ambiguous_issuer_token_match": -30,
    }
    return sum(weights.get(flag, 0) for flag in flags)


def terminal_close_language(flags: set[str]) -> bool:
    return "completion_language" in flags or "delisting_language" in flags


def reason(flags: tuple[str, ...], days_to_last: int | None) -> str:
    return f"flags={'+'.join(flags) or 'none'}; days_terminal_text_to_original_last={days_to_last}"


def extract_date(snippet: str) -> date | None:
    pattern = r"\b([A-Z]+)\s+([0-9]{1,2}),?\s+([0-9]{4})\b"
    for match in re.finditer(pattern, normalize(snippet)):
        month = MONTHS.get(match.group(1))
        if not month:
            continue
        try:
            return date(int(match.group(3)), month, int(match.group(2)))
        except ValueError:
            continue
    numeric = re.search(r"\b([0-9]{4})[ -]([0-9]{2})[ -]([0-9]{2})\b", normalize(snippet))
    if numeric:
        try:
            return date(int(numeric.group(1)), int(numeric.group(2)), int(numeric.group(3)))
        except ValueError:
            return None
    return None


def days_terminal_to_original_last(row: dict[str, Any], terminal_date: date | None) -> int | None:
    original_last = parse_yyyymmdd(row.get("original_last_day") or row.get("last_day"))
    return (original_last - terminal_date).days if original_last and terminal_date else None


def parse_yyyymmdd(value: Any) -> date | None:
    try:
        return datetime.strptime(str(value or ""), "%Y%m%d").date()
    except ValueError:
        return None


def has_parenthetical_collision(row: dict[str, Any], snippet: str) -> bool:
    symbol = normalize_symbol(row.get("symbol"))
    tickers = all_parenthetical_tickers(row, snippet)
    return bool(tickers - {symbol})


def all_parenthetical_tickers(row: dict[str, Any], snippet: str) -> set[str]:
    return parenthetical_tickers(row.get("entity")) | parenthetical_tickers(snippet)


def ticker_match_status(row: dict[str, Any], text: str, snippet: str) -> str:
    symbol = normalize_symbol(row.get("symbol"))
    tickers = all_parenthetical_tickers(row, snippet)
    if symbol in tickers:
        return "parenthetical_match"
    if tickers:
        return "parenthetical_collision"
    if direct_ticker_match(symbol, text):
        return "direct_text_match"
    return "no_direct_ticker_evidence"


def ambiguous_issuer_without_direct_ticker(row: dict[str, Any], text: str, snippet: str) -> bool:
    symbol = normalize_symbol(row.get("symbol"))
    issuer_tokens = set(normalize(row.get("proposed_historical_issuer_name")).split())
    has_direct = symbol in all_parenthetical_tickers(row, snippet) or direct_ticker_match(
        symbol, f"{text} {snippet}"
    )
    return symbol in issuer_tokens and not has_direct


def direct_ticker_match(symbol: Any, text: str) -> bool:
    symbol_text = normalize_symbol(symbol)
    if not symbol_text or len(symbol_text) <= 1:
        return False
    patterns = (
        rf"\b(?:TICKER|SYMBOL|NASDAQ|NYSE|NYSEAMERICAN|OTC)\s+{re.escape(symbol_text)}\b",
        rf"\b{re.escape(symbol_text)}\s+(?:COMMON|ORDINARY|SHARES|STOCK)\b",
    )
    return symbol_text in parenthetical_tickers(text) or any(
        re.search(pattern, normalize(text)) for pattern in patterns
    )


def has_term(text: str, terms: tuple[str, ...]) -> bool:
    normalized = normalize(text)
    return any(normalize(term) in normalized for term in terms)


def normalize(value: Any) -> str:
    return re.sub(r"[^A-Z0-9]+", " ", str(value or "").upper()).strip()


def normalize_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def clean_cik(value: Any) -> str:
    return re.sub(r"\D", "", str(value or "")).lstrip("0")
