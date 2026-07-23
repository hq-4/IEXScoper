from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any

from utils.sec_terminal_text_evidence import (
    all_parenthetical_tickers,
    direct_ticker_match,
    empty_result,
    extract_date,
    has_parenthetical_collision,
    has_term,
    normalize,
    normalize_symbol,
)
from utils.verify_sec_override_candidates import issuer_matches

MAX_DATE_DISTANCE_DAYS = 14
SNIPPET_RADIUS = 320
PASS_BUCKET = "lifecycle_text_verified_ready"
TERMINAL_TERMS = (
    "completed the merger",
    "consummated the merger",
    "closing of the merger",
    "transaction closed",
    "shares ceased trading",
    "suspended from trading",
    "delisted",
    "form 25",
    "terminated registration",
)
FIRST_DAY_TERMS = (
    "began trading",
    "commenced trading",
    "will begin trading",
    "listed on",
    "started trading",
    "under the ticker symbol",
    "under the symbol",
    "changed its ticker",
    "ticker symbol changed",
    "changed its name",
)
PROSPECTIVE_TERMS = (
    "expected to",
    "anticipates",
    "subject to",
    "intends to",
    "will begin trading",
)


def lifecycle_text_result(
    row: dict[str, Any],
    text: str,
    document_url: str,
    max_date_distance_days: int = MAX_DATE_DISTANCE_DAYS,
) -> dict[str, Any]:
    snippet = best_snippet(row, text)
    event_date = extract_date(snippet)
    days_to_anchor = days_event_to_anchor(row, event_date)
    flags = lifecycle_flags(row, text, snippet, event_date, days_to_anchor, max_date_distance_days)
    return {
        "lifecycle_text_bucket": bucket_for_flags(row, flags),
        "lifecycle_text_score": lifecycle_score(flags),
        "lifecycle_text_reason": reason(flags, days_to_anchor),
        "lifecycle_text_snippet": snippet,
        "lifecycle_text_date": event_date.isoformat() if event_date else None,
        "days_lifecycle_text_to_anchor": days_to_anchor,
        "lifecycle_text_flags": "|".join(flags),
        "lifecycle_text_document_url": document_url,
    }


def lifecycle_empty_result(
    bucket: str, score: int, reason_text: str, flags: str, document_url: str | None
) -> dict[str, Any]:
    result = empty_result(bucket, score, reason_text, flags, document_url)
    return {
        "lifecycle_text_bucket": result["terminal_text_bucket"],
        "lifecycle_text_score": result["terminal_text_score"],
        "lifecycle_text_reason": result["terminal_text_reason"],
        "lifecycle_text_snippet": result["terminal_text_snippet"],
        "lifecycle_text_date": result["terminal_text_date"],
        "days_lifecycle_text_to_anchor": result["days_terminal_text_to_original_last"],
        "lifecycle_text_flags": result["terminal_text_flags"],
        "lifecycle_text_document_url": result["terminal_text_document_url"],
    }


def best_snippet(row: dict[str, Any], text: str) -> str:
    terms = FIRST_DAY_TERMS if row.get("lifecycle_anchor") == "first" else TERMINAL_TERMS
    snippets = snippets_for_terms(text, terms)
    return max(snippets, key=snippet_quality).strip() if snippets else ""


def snippets_for_terms(text: str, terms: tuple[str, ...]) -> list[str]:
    snippets = []
    for term in terms:
        pattern = r"\s+".join(re.escape(part) for part in term.split())
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            start = max(0, match.start() - SNIPPET_RADIUS)
            end = min(len(text), match.end() + SNIPPET_RADIUS)
            snippets.append(text[start:end])
    return snippets


def snippet_quality(snippet: str) -> int:
    score = 0
    if extract_date(snippet):
        score += 40
    if has_term(snippet, TERMINAL_TERMS):
        score += 25
    if has_term(snippet, FIRST_DAY_TERMS):
        score += 20
    return score


def lifecycle_flags(
    row: dict[str, Any],
    text: str,
    snippet: str,
    event_date: date | None,
    days_to_anchor: int | None,
    max_date_distance_days: int,
) -> tuple[str, ...]:
    flags: set[str] = set()
    if issuer_matches(row.get("proposed_historical_issuer_name"), normalize(text)):
        flags.add("issuer_name_match")
    if direct_ticker(row, text, snippet):
        flags.add("direct_ticker_evidence")
    if has_term(snippet, TERMINAL_TERMS):
        flags.add("terminal_language")
    if has_term(snippet, FIRST_DAY_TERMS):
        flags.add("first_day_lifecycle_language")
    if has_term(snippet, PROSPECTIVE_TERMS):
        flags.add("prospective_language")
    if event_date:
        flags.add("lifecycle_date_extracted")
    if days_to_anchor is not None and abs(days_to_anchor) <= max_date_distance_days:
        flags.add("lifecycle_date_near_anchor")
    add_safety_flags(flags, row, text, snippet)
    return tuple(sorted(flags))


def direct_ticker(row: dict[str, Any], text: str, snippet: str) -> bool:
    symbol = normalize_symbol(row.get("symbol"))
    return symbol in all_parenthetical_tickers(row, snippet) or direct_ticker_match(symbol, f"{text} {snippet}")


def add_safety_flags(flags: set[str], row: dict[str, Any], text: str, snippet: str) -> None:
    symbol = normalize_symbol(row.get("symbol"))
    tickers = all_parenthetical_tickers(row, snippet)
    if has_parenthetical_collision(row, snippet) or bool(tickers - {symbol}):
        flags.add("parenthetical_ticker_collision")
    issuer_tokens = set(normalize(row.get("proposed_historical_issuer_name")).split())
    if symbol in issuer_tokens and "direct_ticker_evidence" not in flags:
        flags.add("ambiguous_issuer_token_match")
    if not re.search(rf"\b{re.escape(symbol)}\b", normalize(text)) and len(symbol) <= 2:
        flags.add("short_symbol_without_direct_context")


def bucket_for_flags(row: dict[str, Any], flags: tuple[str, ...]) -> str:
    flag_set = set(flags)
    if "parenthetical_ticker_collision" in flag_set:
        return "reject_symbol_collision"
    if not credible_identity(flag_set):
        return "weak_identity_review"
    if "lifecycle_date_extracted" not in flag_set:
        return "missing_lifecycle_date_review"
    if "lifecycle_date_near_anchor" not in flag_set:
        return "date_mismatch_review"
    if "ambiguous_issuer_token_match" in flag_set:
        return "ambiguous_identity_review"
    if "prospective_language" in flag_set:
        return "prospective_language_review"
    if row.get("lifecycle_anchor") == "first":
        return first_anchor_bucket(flag_set)
    return last_anchor_bucket(flag_set)


def first_anchor_bucket(flags: set[str]) -> str:
    if "first_day_lifecycle_language" in flags and "direct_ticker_evidence" in flags:
        return PASS_BUCKET
    return "first_day_lifecycle_review"


def last_anchor_bucket(flags: set[str]) -> str:
    if "terminal_language" in flags:
        return PASS_BUCKET
    return "last_day_lifecycle_review"


def credible_identity(flags: set[str]) -> bool:
    return "issuer_name_match" in flags or "direct_ticker_evidence" in flags


def lifecycle_score(flags: tuple[str, ...]) -> int:
    weights = {
        "issuer_name_match": 20,
        "direct_ticker_evidence": 25,
        "terminal_language": 30,
        "first_day_lifecycle_language": 25,
        "lifecycle_date_extracted": 15,
        "lifecycle_date_near_anchor": 25,
        "prospective_language": -40,
        "parenthetical_ticker_collision": -100,
        "ambiguous_issuer_token_match": -30,
        "short_symbol_without_direct_context": -20,
    }
    return sum(weights.get(flag, 0) for flag in flags)


def reason(flags: tuple[str, ...], days_to_anchor: int | None) -> str:
    return f"flags={'+'.join(flags) or 'none'}; days_lifecycle_text_to_anchor={days_to_anchor}"


def days_event_to_anchor(row: dict[str, Any], event_date: date | None) -> int | None:
    anchor_key = "original_first_day" if row.get("lifecycle_anchor") == "first" else "original_last_day"
    anchor = parse_yyyymmdd(row.get(anchor_key))
    return (anchor - event_date).days if anchor and event_date else None


def parse_yyyymmdd(value: Any) -> date | None:
    try:
        return datetime.strptime(str(value or ""), "%Y%m%d").date()
    except ValueError:
        return None
