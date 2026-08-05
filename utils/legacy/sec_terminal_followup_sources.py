from __future__ import annotations

import random
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

import requests

SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
SEC_SUBMISSIONS_FILE_URL = "https://data.sec.gov/submissions/{name}"
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
TERMINAL_FORM_PRIORITY = {"8-K": 0, "6-K": 1, "25": 2, "25-NSE": 2, "15-12B": 3}


@dataclass(frozen=True)
class SecRequestConfig:
    user_agent: str
    timeout_seconds: float = 10.0
    sleep_seconds: float = 0.25
    retries: int = 3


class SecSubmissionsClient:
    def __init__(self, config: SecRequestConfig) -> None:
        self.config = config
        self._json_cache: dict[str, dict[str, Any]] = {}
        self._filing_cache: dict[tuple[str, str, str], list[dict[str, str]]] = {}
        self.request_count = 0
        self.retry_count = 0

    def submissions(self, cik: str) -> dict[str, Any]:
        url = SEC_SUBMISSIONS_URL.format(cik=clean_cik(cik).zfill(10))
        return self._get_json(url)

    def filings(self, cik: str, *, lower: str, upper: str) -> list[dict[str, str]]:
        clean = clean_cik(cik)
        key = (clean, lower, upper)
        if key in self._filing_cache:
            return list(self._filing_cache[key])
        payload = self.submissions(clean)
        rows = recent_filings(payload, clean)
        for shard in overlapping_files(payload, lower, upper):
            shard_payload = self._get_json(SEC_SUBMISSIONS_FILE_URL.format(name=shard))
            rows.extend(recent_filings(shard_payload, clean))
        deduped = deduplicate_filings(rows)
        self._filing_cache[key] = deduped
        return list(deduped)

    def _get_json(self, url: str) -> dict[str, Any]:
        if url in self._json_cache:
            return self._json_cache[url]
        payload = request_json(url, self.config, self)
        self._json_cache[url] = payload
        return payload


def candidate_filings(
    row: dict[str, Any],
    cik: str,
    config: SecRequestConfig,
    *,
    forms: tuple[str, ...],
    days_before: int,
    days_after: int,
    max_docs: int,
    client: SecSubmissionsClient | None = None,
) -> list[dict[str, str]]:
    target = parse_yyyymmdd(row.get("original_last_day") or row.get("last_day"))
    return candidate_filings_for_target(
        cik,
        config,
        forms=forms,
        days_before=days_before,
        days_after=days_after,
        max_docs=max_docs,
        target=target,
        client=client,
    )


def candidate_filings_for_target(
    cik: str,
    config: SecRequestConfig,
    *,
    forms: tuple[str, ...],
    days_before: int,
    days_after: int,
    max_docs: int,
    target: date | None,
    client: SecSubmissionsClient | None = None,
) -> list[dict[str, str]]:
    if not target:
        return []
    lower, upper = target - timedelta(days=days_before), target + timedelta(days=days_after)
    source = client or SecSubmissionsClient(config)
    filings = source.filings(cik, lower=lower.isoformat(), upper=upper.isoformat())
    candidates = [
        row for row in filings if row["form"] in forms and in_date_range(row, lower, upper)
    ]
    return sorted(candidates, key=lambda item: filing_sort_key(item, target))[:max_docs]


def fetch_submissions(cik: str, config: SecRequestConfig) -> dict[str, Any]:
    return SecSubmissionsClient(config).submissions(cik)


def request_json(
    url: str, config: SecRequestConfig, client: SecSubmissionsClient
) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(1, config.retries + 1):
        client.request_count += 1
        try:
            response = requests.get(url, headers=headers(config), timeout=config.timeout_seconds)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError(f"SEC submissions response must be an object: {url}")
            time.sleep(config.sleep_seconds)
            return payload
        except requests.RequestException as exc:
            last_error = exc
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if attempt == config.retries or (
                status is not None and status not in RETRY_STATUS_CODES
            ):
                raise
            client.retry_count += 1
            time.sleep(config.sleep_seconds * (2 ** (attempt - 1)) + random.uniform(0, 0.1))
    raise RuntimeError("SEC request retry loop exhausted") from last_error


def recent_filings(payload: dict[str, Any], cik: str) -> list[dict[str, str]]:
    recent = payload.get("filings", {}).get("recent", payload)
    if not isinstance(recent, dict):
        return []
    keys = ("form", "filingDate", "accessionNumber", "primaryDocument")
    values = [recent.get(key) for key in keys]
    if not all(isinstance(value, list) for value in values):
        return []
    return [
        filing_row(cik, *(str(value) for value in items))
        for items in zip(*values, strict=False)
        if all(items)
    ]


def overlapping_files(payload: dict[str, Any], lower: str, upper: str) -> list[str]:
    files = payload.get("filings", {}).get("files", [])
    if not isinstance(files, list):
        return []
    return [
        str(item["name"])
        for item in files
        if isinstance(item, dict)
        and item.get("name")
        and str(item.get("filingFrom") or "9999-12-31") <= upper
        and str(item.get("filingTo") or "0001-01-01") >= lower
    ]


def deduplicate_filings(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    by_accession: dict[str, dict[str, str]] = {}
    for row in rows:
        by_accession.setdefault(row["accession_no"], row)
    return list(by_accession.values())


def filing_row(
    cik: str, form: str, filing_date: str, accession: str, document: str
) -> dict[str, str]:
    accession_path = accession.replace("-", "")
    return {
        "form": form,
        "filing_date": filing_date,
        "accession_no": accession,
        "primary_document": document,
        "document_url": f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession_path}/{document}",
    }


def in_date_range(item: dict[str, str], lower: date, upper: date) -> bool:
    try:
        return lower <= parse_iso_date(item["filing_date"]) <= upper
    except ValueError:
        return False


def filing_sort_key(item: dict[str, str], target: date) -> tuple[int, int]:
    return (
        TERMINAL_FORM_PRIORITY.get(item["form"], 9),
        abs((parse_iso_date(item["filing_date"]) - target).days),
    )


def cik_for_row(row: dict[str, Any]) -> str:
    for key in ("identity_cik", "cik", "sec_cik"):
        if value := clean_cik(row.get(key)):
            return value
    for key in ("entity", "primary_source_url", "verifier_document_url"):
        if value := extract_cik(str(row.get(key) or "")):
            return value
    return ""


def clean_cik(value: Any) -> str:
    return re.sub(r"\D+", "", str(value or "")).lstrip("0")


def extract_cik(value: str) -> str:
    for pattern in (r"CIK\s*0*([0-9]+)", r"/Archives/edgar/data/([0-9]+)/"):
        if match := re.search(pattern, value, flags=re.IGNORECASE):
            return clean_cik(match.group(1))
    return ""


def parse_yyyymmdd(value: Any) -> date | None:
    try:
        return datetime.strptime(str(value or ""), "%Y%m%d").date()
    except ValueError:
        return None


def parse_iso_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def headers(config: SecRequestConfig) -> dict[str, str]:
    return {"User-Agent": config.user_agent, "Accept": "application/json"}
