from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import requests

from utils.sec_identity_evidence import IdentityEvidence, parse_display_name, with_metadata

SEC_EFTS_URL = "https://efts.sec.gov/LATEST/search-index"
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}


class SecTransportError(RuntimeError):
    """Raised after bounded SEC transport retries are exhausted."""


@dataclass
class RequestStats:
    request_count: int = 0
    retry_count: int = 0
    fetch_error_count: int = 0
    search_request_count: int = 0
    document_request_count: int = 0


class SecEvidenceClient:
    def __init__(
        self,
        user_agent: str,
        *,
        timeout_seconds: float = 10.0,
        sleep_seconds: float = 0.25,
        retries: int = 3,
    ) -> None:
        self.user_agent = user_agent
        self.timeout_seconds = timeout_seconds
        self.sleep_seconds = sleep_seconds
        self.retries = retries
        self.stats = RequestStats()
        self._json_cache: dict[tuple[str, tuple[tuple[str, str], ...]], dict[str, Any]] = {}
        self._text_cache: dict[str, str] = {}

    def search(self, params: dict[str, str]) -> dict[str, Any]:
        key = (SEC_EFTS_URL, tuple(sorted(params.items())))
        if key not in self._json_cache:
            self.stats.search_request_count += 1
            response = self._request(SEC_EFTS_URL, params=params)
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError("SEC EFTS response must be a JSON object")
            self._json_cache[key] = payload
        return self._json_cache[key]

    def document_text(self, url: str) -> str:
        if url not in self._text_cache:
            self.stats.document_request_count += 1
            self._text_cache[url] = self._request(url).text
        return self._text_cache[url]

    def _request(self, url: str, *, params: dict[str, str] | None = None) -> requests.Response:
        error: Exception | None = None
        for attempt in range(1, self.retries + 1):
            self.stats.request_count += 1
            try:
                request_kwargs: dict[str, Any] = {
                    "headers": {
                        "User-Agent": self.user_agent,
                        "Accept": "application/json,text/html",
                    },
                    "timeout": self.timeout_seconds,
                }
                if params is not None:
                    request_kwargs["params"] = params
                response = requests.get(url, **request_kwargs)
                response.raise_for_status()
                time.sleep(self.sleep_seconds)
                return response
            except requests.RequestException as exc:
                error = exc
                status = getattr(getattr(exc, "response", None), "status_code", None)
                retryable = status is None or status in RETRY_STATUS_CODES
                if attempt == self.retries or not retryable:
                    self.stats.fetch_error_count += 1
                    raise SecTransportError(f"SEC request failed: {url}") from exc
                self.stats.retry_count += 1
                delay = self.sleep_seconds * (2 ** (attempt - 1)) + random.uniform(0, 0.1)
                time.sleep(delay)
        raise SecTransportError(f"SEC request failed: {url}") from error


def evidence_from_raw_paths(paths: Iterable[Path]) -> list[IdentityEvidence]:
    evidence: list[IdentityEvidence] = []
    for path in paths:
        if not path.exists():
            continue
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                evidence.extend(evidence_from_raw_line(line))
    return deduplicate_evidence(evidence)


def evidence_from_raw_line(line: str) -> list[IdentityEvidence]:
    try:
        record = json.loads(line)
    except (json.JSONDecodeError, TypeError):
        return []
    payload = record.get("payload", record) if isinstance(record, dict) else {}
    return evidence_from_payload(payload, source="local_cache")


def evidence_from_payload(payload: dict[str, Any], *, source: str) -> list[IdentityEvidence]:
    hits = payload.get("hits", {}).get("hits", [])
    if not isinstance(hits, list):
        return []
    output = []
    for hit in hits:
        output.extend(evidence_from_hit(hit, source))
    return deduplicate_evidence(output)


def evidence_from_hit(hit: Any, source: str) -> list[IdentityEvidence]:
    if not isinstance(hit, dict):
        return []
    item = hit.get("_source", hit)
    if not isinstance(item, dict):
        return []
    names = item.get("display_names") or item.get("displayNames") or []
    if isinstance(names, str):
        names = [names]
    if not isinstance(names, list):
        return []
    accession = str(item.get("adsh") or item.get("accession_no") or "")
    filing_date = str(item.get("file_date") or item.get("filing_date") or "")
    parsed = [
        parse_display_name(
            str(name), filing_date=filing_date, accession_no=accession, source=source
        )
        for name in names
    ]
    return [
        with_metadata(entry, document_url=document_url(hit, item, accession, entry.cik))
        for entry in parsed
    ]


def document_url(hit: dict[str, Any], item: dict[str, Any], accession: str, filer_cik: str) -> str:
    direct = item.get("documentUrl") or item.get("file_url") or item.get("url")
    if direct:
        return str(direct)
    identifier = str(hit.get("_id") or "")
    document = identifier.split(":", 1)[1] if ":" in identifier else ""
    ciks = item.get("ciks") or item.get("cik") or []
    source_cik = ciks[0] if isinstance(ciks, list) and ciks else ciks
    cik = str(source_cik or filer_cik).lstrip("0")
    if accession and document and cik:
        return (
            f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/"
            f"{accession.replace('-', '')}/{document}"
        )
    return ""


def deduplicate_evidence(items: Iterable[IdentityEvidence]) -> list[IdentityEvidence]:
    unique: dict[tuple[Any, ...], IdentityEvidence] = {}
    for item in items:
        key = (item.cik, item.issuer_name, item.tickers, item.filing_date, item.accession_no)
        unique.setdefault(key, item)
    return list(unique.values())
