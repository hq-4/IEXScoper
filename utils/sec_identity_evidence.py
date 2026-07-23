from __future__ import annotations

import re
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from typing import Any, Iterable

IDENTITY_BEFORE_DAYS = 45
IDENTITY_AFTER_DAYS = 90
CIK_PATTERN = re.compile(r"\(\s*CIK\s+0*([0-9]+)\s*\)\s*$", re.IGNORECASE)
TICKER_PATTERN = re.compile(r"\(([^()]*)\)\s*\(\s*CIK\s+[0-9]+\s*\)\s*$", re.IGNORECASE)


@dataclass(frozen=True)
class IdentityEvidence:
    issuer_name: str
    cik: str
    tickers: tuple[str, ...]
    filing_date: str = ""
    accession_no: str = ""
    document_url: str = ""
    source: str = "local_cache"


def parse_display_name(
    value: str,
    *,
    filing_date: str = "",
    accession_no: str = "",
    document_url: str = "",
    source: str = "local_cache",
) -> IdentityEvidence:
    cik_match = CIK_PATTERN.search(value.strip())
    ticker_match = TICKER_PATTERN.search(value.strip())
    cik = cik_match.group(1) if cik_match else ""
    tickers = parse_tickers(ticker_match.group(1) if ticker_match else "")
    cutoff = (
        ticker_match.start() if ticker_match else cik_match.start() if cik_match else len(value)
    )
    issuer = re.sub(r"\s+", " ", value[:cutoff]).strip(" ,")
    return IdentityEvidence(issuer, cik, tickers, filing_date, accession_no, document_url, source)


def parse_tickers(value: str) -> tuple[str, ...]:
    items = [item.strip().upper() for item in value.split(",")]
    return tuple(dict.fromkeys(item for item in items if re.fullmatch(r"[A-Z0-9.^+=-]+", item)))


def with_metadata(evidence: IdentityEvidence, **values: str) -> IdentityEvidence:
    return replace(evidence, **values)


def build_identity_queries(symbol: str, lower: date, upper: date) -> list[dict[str, str]]:
    ticker = symbol.strip().upper()
    queries = (
        f'{ticker} AND "trading under the symbol"',
        f'{ticker} AND ("ticker symbol" OR "common stock")',
        f'{ticker} AND ("NYSE" OR "Nasdaq" OR "NYSE American")',
    )
    return [
        {
            "q": query,
            "dateRange": "custom",
            "startdt": lower.isoformat(),
            "enddt": upper.isoformat(),
            "size": "100",
        }
        for query in queries
    ]


def identity_result(
    row: dict[str, Any],
    evidence: Iterable[IdentityEvidence],
    *,
    days_before: int = IDENTITY_BEFORE_DAYS,
    days_after: int = IDENTITY_AFTER_DAYS,
) -> dict[str, Any]:
    symbol = str(row.get("symbol") or "").strip().upper()
    lower, upper = identity_window(row, days_before, days_after)
    scoped = [item for item in evidence if in_window(item.filing_date, lower, upper)]
    direct = [item for item in scoped if symbol in item.tickers and item.cik and item.issuer_name]
    ciks = sorted({item.cik for item in direct})
    bucket = identity_bucket(direct, ciks)
    chosen = max(direct, key=evidence_sort_key) if len(ciks) == 1 else None
    aliases = sorted({item.issuer_name for item in direct if chosen and item.cik == chosen.cik})
    tickers = sorted(
        {ticker for item in direct if chosen and item.cik == chosen.cik for ticker in item.tickers}
    )
    return identity_output(bucket, direct, ciks, chosen, aliases, tickers)


def identity_window(
    row: dict[str, Any], before: int, after: int
) -> tuple[date | None, date | None]:
    first = parse_day(row.get("first_day") or row.get("original_first_day"))
    last = parse_day(row.get("last_day") or row.get("original_last_day"))
    return (
        first - timedelta(days=before) if first else None,
        last + timedelta(days=after) if last else None,
    )


def in_window(value: str, lower: date | None, upper: date | None) -> bool:
    filed = parse_day(value)
    return bool(filed and lower and upper and lower <= filed <= upper)


def identity_bucket(direct: list[IdentityEvidence], ciks: list[str]) -> str:
    if len(ciks) > 1:
        return "identity_collision_hold"
    if len(ciks) == 1:
        return "identity_verified_ready"
    return "identity_no_evidence" if not direct else "identity_weak_hold"


def evidence_sort_key(item: IdentityEvidence) -> tuple[str, str, str]:
    return (item.filing_date, item.accession_no, item.issuer_name)


def identity_output(
    bucket: str,
    direct: list[IdentityEvidence],
    ciks: list[str],
    chosen: IdentityEvidence | None,
    aliases: list[str],
    tickers: list[str],
) -> dict[str, Any]:
    flags = ["exact_filer_ticker"] if direct else []
    if len(ciks) > 1:
        flags.append("multiple_date_scoped_ciks")
    if chosen:
        flags.extend(("valid_cik", "date_scoped", "single_cik"))
    return {
        "identity_bucket": bucket,
        "identity_score": 100 if chosen else -100 if len(ciks) > 1 else 0,
        "identity_reason": f"direct_candidates={len(direct)}; candidate_ciks={len(ciks)}",
        "identity_flags": "|".join(sorted(flags)),
        "identity_cik": chosen.cik if chosen else "",
        "identity_issuer_name": chosen.issuer_name if chosen else "",
        "identity_issuer_aliases": "|".join(aliases),
        "identity_tickers": "|".join(tickers),
        "identity_evidence_date": chosen.filing_date if chosen else "",
        "identity_accession_no": chosen.accession_no if chosen else "",
        "identity_document_url": chosen.document_url if chosen else "",
        "identity_source": chosen.source if chosen else "",
        "identity_candidate_cik_count": len(ciks),
    }


def parse_day(value: Any) -> date | None:
    text = str(value or "")
    for pattern in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, pattern).date()
        except ValueError:
            continue
    return None
