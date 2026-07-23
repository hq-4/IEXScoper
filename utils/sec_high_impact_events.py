from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from utils.sec_high_impact_resolution import resolve_event_text
from utils.sec_identity_evidence import parse_day

EVENT_FORMS = (
    "8-K",
    "6-K",
    "25",
    "25-NSE",
    "15-12B",
    "15-12G",
    "15-15D",
    "425",
    "DEFM14A",
    "S-4",
    "S-4/A",
    "SC 13E3",
    "SC 13E3/A",
)


def event_evidence(
    row: dict[str, Any], config: Any, evidence_client: Any, submissions_client: Any
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    target = parse_day(row.get("last_day"))
    if not target:
        return [], submissions_client.submissions(row["identity_cik"])
    lower = target - timedelta(days=config.event_days_before)
    upper = target + timedelta(days=config.event_days_after)
    filings = submissions_client.filings(
        row["identity_cik"], lower=lower.isoformat(), upper=upper.isoformat()
    )
    candidates = sorted(
        (filing for filing in filings if filing_in_window(filing, lower, upper)),
        key=lambda filing: filing_sort_key(filing, target),
    )[: config.max_docs_per_row]
    reviewed = [review_filing(row, filing, evidence_client) for filing in candidates]
    return reviewed, submissions_client.submissions(row["identity_cik"])


def filing_in_window(filing: dict[str, str], lower: date, upper: date) -> bool:
    filed = parse_day(filing.get("filing_date"))
    return bool(filing.get("form") in EVENT_FORMS and filed and lower <= filed <= upper)


def filing_sort_key(filing: dict[str, str], target: date) -> tuple[int, int]:
    form_order = EVENT_FORMS.index(filing["form"])
    filed = parse_day(filing["filing_date"])
    return (form_order, abs((filed - target).days) if filed else 10_000)


def review_filing(
    row: dict[str, Any], filing: dict[str, str], evidence_client: Any
) -> dict[str, Any]:
    text = evidence_client.document_text(filing["document_url"])
    return resolve_event_text(
        row,
        text,
        filing["document_url"],
        filing_cik=row["identity_cik"],
        form=filing["form"],
        accession_no=filing["accession_no"],
    )


def select_verified_event(events: list[dict[str, Any]]) -> dict[str, Any] | None:
    for bucket in ("symbol_change_verified_ready", "terminal_verified_ready"):
        if rows := [row for row in events if row["resolution_bucket"] == bucket]:
            return rows[0]
    return None


def active_current_evidence(
    row: dict[str, Any],
    submissions: dict[str, Any],
    directory: set[tuple[str, str]],
    client: Any,
) -> bool:
    symbol, cik = row["symbol"].upper(), str(row["identity_cik"]).lstrip("0")
    metadata_tickers = {str(value).upper() for value in submissions.get("tickers", [])}
    metadata_cik = str(submissions.get("cik") or "").lstrip("0")
    last = parse_day(row.get("last_day"))
    if not active_identity_matches(symbol, cik, metadata_tickers, metadata_cik, directory, last):
        return False
    filings = client.filings(cik, lower=last.isoformat(), upper="9999-12-31")
    return any((filed := parse_day(item["filing_date"])) and filed > last for item in filings)


def active_identity_matches(
    symbol: str,
    cik: str,
    tickers: set[str],
    metadata_cik: str,
    directory: set[tuple[str, str]],
    last: date | None,
) -> bool:
    return bool(symbol in tickers and metadata_cik == cik and (symbol, cik) in directory and last)


def empty_resolution(bucket: str) -> dict[str, Any]:
    return {
        "resolution_bucket": bucket,
        "event_type": "",
        "event_date": "",
        "event_successor": "",
        "event_flags": "",
        "event_snippet": "",
        "event_document_url": "",
        "event_form": "",
        "event_accession_no": "",
        "event_date_distance_days": "",
        "importable": False,
    }
