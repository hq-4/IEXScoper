from __future__ import annotations

import secrets
import time
from dataclasses import dataclass
from datetime import date
from typing import Any, Iterator

import requests

from utils.resolution_v2_registry import CachePolicy, EvidenceRegistry

SEC_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"
RETRYABLE = {429, 500, 502, 503, 504}


class PrimarySourceError(RuntimeError):
    """A bounded public-primary request could not be completed."""


@dataclass(frozen=True)
class NetworkConfig:
    user_agent: str
    budget: int = 25_000
    delay_seconds: float = 0.25
    timeout_seconds: float = 10.0
    retries: int = 3


class CachedPrimaryClient:
    def __init__(self, config: NetworkConfig, registry: EvidenceRegistry) -> None:
        if not config.user_agent.strip():
            raise ValueError("SEC_USER_AGENT is required for network resolution")
        self.config = config
        self.registry = registry
        self.requests = 0
        self.failures = 0

    def get_json(
        self,
        source: str,
        url: str,
        params: dict[str, Any],
        policy: CachePolicy,
    ) -> tuple[Any, bool]:
        request = {"url": url, "params": params}
        cached = self.registry.get(source, request, policy)
        if cached is not None:
            return cached, True
        response = self._request(url, params=params)
        payload = response.json()
        self.registry.put(source, request, payload, negative=_negative_payload(payload))
        return payload, False

    def get_text(self, source: str, url: str) -> str:
        if source != "filing_document":
            raise ValueError("text retrieval is restricted to in-memory filing documents")
        return self._request(url).text

    def _request(self, url: str, *, params: dict[str, Any] | None = None) -> requests.Response:
        error: Exception | None = None
        for attempt in range(1, self.config.retries + 1):
            if self.requests >= self.config.budget:
                raise PrimarySourceError("network request budget exhausted")
            self.requests += 1
            self.registry._increment("network_requests")
            try:
                response = self._send(url, params)
                response.raise_for_status()
                time.sleep(self.config.delay_seconds)
                return response
            except requests.RequestException as exc:
                error = exc
                self._retry_or_raise(url, attempt, exc)
        raise PrimarySourceError(f"primary-source request failed: {url}") from error

    def _send(self, url: str, params: dict[str, Any] | None) -> requests.Response:
        headers = {"User-Agent": self.config.user_agent, "Accept": "application/json,text/html"}
        return requests.get(
            url, params=params, headers=headers, timeout=self.config.timeout_seconds
        )

    def _retry_or_raise(self, url: str, attempt: int, error: requests.RequestException) -> None:
        self.failures += 1
        status = getattr(getattr(error, "response", None), "status_code", None)
        if attempt == self.config.retries or status not in RETRYABLE | {None}:
            raise PrimarySourceError(f"primary-source request failed: {url}") from error
        jitter = secrets.randbelow(101) / 1_000
        time.sleep(self.config.delay_seconds * 2 ** (attempt - 1) + jitter)


def annual_search_slices(first_day: date, last_day: date) -> list[tuple[date, date]]:
    years = list(range(first_day.year, last_day.year + 1))
    ordered = sorted(
        years, key=lambda year: (min(abs(year - last_day.year), abs(year - first_day.year)), -year)
    )
    return [
        (max(first_day, date(year, 1, 1)), min(last_day, date(year, 12, 31))) for year in ordered
    ]


def paged_sec_search(
    client: CachedPrimaryClient,
    symbol: str,
    first_day: date,
    last_day: date,
    policy: CachePolicy,
    page_size: int = 100,
) -> Iterator[dict[str, Any]]:
    for lower, upper in annual_search_slices(first_day, last_day):
        yield from _slice_pages(client, symbol, lower, upper, policy, page_size)


def _slice_pages(
    client: CachedPrimaryClient,
    symbol: str,
    lower: date,
    upper: date,
    policy: CachePolicy,
    page_size: int,
) -> Iterator[dict[str, Any]]:
    offset = 0
    while True:
        params = {
            "q": symbol.upper(),
            "entityName": symbol.upper(),
            "dateRange": "custom",
            "startdt": lower.isoformat(),
            "enddt": upper.isoformat(),
            "from": str(offset),
            "size": str(page_size),
        }
        payload, _ = client.get_json("sec_efts_search", SEC_SEARCH_URL, params, policy)
        yield payload
        hits = payload.get("hits", {}).get("hits", []) if isinstance(payload, dict) else []
        if not isinstance(hits, list) or len(hits) < page_size:
            break
        offset += page_size


def _negative_payload(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return not payload
    hits = payload.get("hits", {})
    if isinstance(hits, dict):
        rows = hits.get("hits", [])
        return isinstance(rows, list) and not rows
    return not payload
