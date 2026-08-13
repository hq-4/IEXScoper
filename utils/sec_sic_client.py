"""Fetch SIC/`sicDescription` for a CIK from SEC's submissions endpoint.

Reuses `utils.resolution_v2_network.CachedPrimaryClient` rather than building a parallel
cached/rate-limited/retrying HTTP client: it already does exactly what a SIC fetcher
needs (cache-first via `EvidenceRegistry`, backoff+jitter on 429/5xx, required SEC
`User-Agent`), and calling it with the identical `source="sec_submissions"` +
`request={"url": url, "params": {}}` shape `utils/resolution_v2_sec.py` already uses
means any CIK the live SEC-lane resolver has already fetched while walking filings is a
free cache hit here — the submissions payload's `sic`/`sicDescription` fields are
already sitting in that cached response, just never read by that module.

`fetch_filing_activity` reads a second, previously-unused part of that same payload:
`filings.recent` (and whether `filings.files` holds older history) — the CIK's actual
SEC filing dates, used by `utils.edgar_company_search_match` to tell a genuinely
historically-operating registrant apart from a same-named dormant shell. Kept as its own
function rather than folded into `fetch_sic`'s result: `fetch_many` runs `fetch_sic` over
every resolved CIK for the main SIC pass, which never needs filing dates, and embedding
up to ~1,000 date strings per row there would be pure dead weight. Calling it right after
`fetch_sic` for the same CIK is a guaranteed cache hit (identical `SEC_SUBMISSIONS_SOURCE`
request shape), never a second real network call. [CA][REH][KBT]
"""

from __future__ import annotations

import re
from datetime import timedelta
from typing import Any

from src.framework.logging import get_logger
from utils.resolution_v2_network import CachedPrimaryClient, PrimarySourceError
from utils.resolution_v2_registry import CachePolicy

SEC_SUBMISSIONS_SOURCE = "sec_submissions"
DEFAULT_MAX_AGE_DAYS = 90
LOG_EVERY = 250

STATUS_OK = "ok"
STATUS_NO_SIC = "no_sic_on_record"
STATUS_NOT_FOUND = "cik_not_found"
STATUS_FETCH_ERROR = "fetch_error"

FILING_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def fetch_sic(
    client: CachedPrimaryClient, cik: str, *, max_age_days: int = DEFAULT_MAX_AGE_DAYS
) -> dict[str, Any]:
    """One CIK, one result row. Never raises for a 404 or a transient network failure
    after exhausted retries — those become `fetch_status` values so a batch run over
    thousands of CIKs continues past individual bad ones."""
    url = f"https://data.sec.gov/submissions/CIK{int(cik):010d}.json"
    policy = CachePolicy(max_age=timedelta(days=max_age_days))
    try:
        payload, from_cache = client.get_json(SEC_SUBMISSIONS_SOURCE, url, {}, policy)
    except PrimarySourceError as error:
        return _error_result(cik, error)
    return _payload_result(cik, payload, from_cache)


def fetch_filing_activity(
    client: CachedPrimaryClient, cik: str, *, max_age_days: int = DEFAULT_MAX_AGE_DAYS
) -> dict[str, Any]:
    """One CIK, one result row: distinct sorted filing dates from `filings.recent`, plus
    whether `filings.files` holds older history this doesn't cover. Never raises. Reuses
    the identical `SEC_SUBMISSIONS_SOURCE` request shape `fetch_sic` uses, so calling
    this right after `fetch_sic` for the same CIK is a guaranteed cache hit, not a second
    real request."""
    url = f"https://data.sec.gov/submissions/CIK{int(cik):010d}.json"
    policy = CachePolicy(max_age=timedelta(days=max_age_days))
    try:
        payload, from_cache = client.get_json(SEC_SUBMISSIONS_SOURCE, url, {}, policy)
    except PrimarySourceError as error:
        return _filing_activity_error_result(cik, error)
    return _filing_activity_payload_result(cik, payload, from_cache)


def fetch_many(
    client: CachedPrimaryClient, ciks: list[str], *, max_age_days: int = DEFAULT_MAX_AGE_DAYS
) -> list[dict[str, Any]]:
    """Best-effort batch: one bad CIK never aborts the run. Logs progress every
    `LOG_EVERY` CIKs so a multi-thousand-CIK pass has visible heartbeat."""
    logger = get_logger(__name__)
    results = []
    for index, cik in enumerate(ciks, start=1):
        results.append(fetch_sic(client, cik, max_age_days=max_age_days))
        if index % LOG_EVERY == 0 or index == len(ciks):
            logger.info(
                "SIC fetch progress",
                extra={
                    "event": "sec_sic_fetch_progress",
                    "detail": {"done": index, "total": len(ciks)},
                },
            )
    return results


def _payload_result(cik: str, payload: Any, from_cache: bool) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return _base_result(cik, STATUS_FETCH_ERROR, from_cache)
    sic = str(payload.get("sic") or "").strip()
    status = STATUS_OK if sic else STATUS_NO_SIC
    result = _base_result(cik, status, from_cache)
    result["sic"] = sic or None
    result["sic_description"] = str(payload.get("sicDescription") or "").strip() or None
    result["entity_name"] = str(payload.get("name") or "").strip() or None
    result["former_names"] = _former_names(payload)
    return result


def _former_names(payload: dict[str, Any]) -> list[str]:
    """SEC's submissions payload carries a `formerNames` array (exact historical name +
    date range) for any registrant that has renamed — sitting alongside `sic`/`name` in
    the same already-fetched response, previously unread. Returns just the name strings;
    callers that need the date range can read `formerNames` off the raw payload
    directly."""
    names = []
    for entry in payload.get("formerNames") or ():
        if isinstance(entry, dict):
            name = str(entry.get("name") or "").strip()
            if name:
                names.append(name)
    return names


def _error_result(cik: str, error: PrimarySourceError) -> dict[str, Any]:
    status_code = _http_status(error)
    status = STATUS_NOT_FOUND if status_code == 404 else STATUS_FETCH_ERROR
    return _base_result(cik, status, from_cache=False)


def _http_status(error: PrimarySourceError) -> int | None:
    response = getattr(error.__cause__, "response", None)
    return getattr(response, "status_code", None)


def _base_result(cik: str, fetch_status: str, from_cache: bool) -> dict[str, Any]:
    return {
        "cik": cik,
        "sic": None,
        "sic_description": None,
        "entity_name": None,
        "former_names": [],
        "fetch_status": fetch_status,
        "from_cache": from_cache,
    }


def _filing_activity_payload_result(cik: str, payload: Any, from_cache: bool) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return _base_filing_activity_result(cik, STATUS_FETCH_ERROR, from_cache)
    dates = _recent_filing_dates(payload)
    result = _base_filing_activity_result(cik, STATUS_OK, from_cache)
    result["filing_dates"] = dates
    result["earliest_filing_date"] = dates[0] if dates else None
    result["latest_filing_date"] = dates[-1] if dates else None
    result["has_older_shards"] = bool(payload.get("filings", {}).get("files"))
    return result


def _recent_filing_dates(payload: dict[str, Any]) -> tuple[str, ...]:
    """Sorted, distinct `YYYY-MM-DD` dates out of `filings.recent.filingDate` — SEC's
    parallel-array-of-columns shape (`form`/`filingDate`/`accessionNumber`/... all the
    same length), so only the one column this needs is read. Malformed/blank entries are
    dropped rather than raising, same "never trust the payload shape blindly" posture as
    `_former_names`."""
    recent = payload.get("filings", {}).get("recent", {})
    if not isinstance(recent, dict):
        return ()
    raw_dates = recent.get("filingDate")
    if not isinstance(raw_dates, list):
        return ()
    valid = {date for date in raw_dates if isinstance(date, str) and FILING_DATE_PATTERN.match(date)}
    return tuple(sorted(valid))


def _filing_activity_error_result(cik: str, error: PrimarySourceError) -> dict[str, Any]:
    status_code = _http_status(error)
    status = STATUS_NOT_FOUND if status_code == 404 else STATUS_FETCH_ERROR
    return _base_filing_activity_result(cik, status, from_cache=False)


def _base_filing_activity_result(cik: str, fetch_status: str, from_cache: bool) -> dict[str, Any]:
    return {
        "cik": cik,
        "filing_dates": (),
        "earliest_filing_date": None,
        "latest_filing_date": None,
        "has_older_shards": False,
        "fetch_status": fetch_status,
        "from_cache": from_cache,
    }
