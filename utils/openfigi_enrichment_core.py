from __future__ import annotations

import csv
import json
import logging
import random
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

import requests

from utils.openfigi_enrichment_outputs import AUDIT_COLUMNS, OPENFIGI_COLUMNS, write_outputs

OPENFIGI_MAPPING_URL = "https://api.openfigi.com/v3/mapping"
DEFAULT_BATCH_SIZE = 10
DEFAULT_SLEEP_SECONDS = 1.0
DEFAULT_TIMEOUT_SECONDS = 10.0
DEFAULT_MAX_RETRIES = 3


class OpenFigiClient(Protocol):
    def map_jobs(self, jobs: list[dict[str, str]]) -> list[dict[str, Any]]: ...


@dataclass(frozen=True)
class OpenFigiConfig:
    input_path: Path
    output_root: Path
    api_key: str | None
    batch_size: int = DEFAULT_BATCH_SIZE
    sleep_seconds: float = DEFAULT_SLEEP_SECONDS
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    max_retries: int = DEFAULT_MAX_RETRIES
    exch_code: str = "US"
    market_sector: str = "Equity"
    symbols: frozenset[str] | None = None
    classifications: frozenset[str] | None = None
    limit_symbols: int | None = None


class RequestsOpenFigiClient:
    def __init__(self, config: OpenFigiConfig) -> None:
        self.config = config

    def map_jobs(self, jobs: list[dict[str, str]]) -> list[dict[str, Any]]:
        headers = {"Content-Type": "application/json"}
        if self.config.api_key:
            headers["X-OPENFIGI-APIKEY"] = self.config.api_key
        for attempt in range(1, self.config.max_retries + 1):
            try:
                response = requests.post(
                    OPENFIGI_MAPPING_URL,
                    headers=headers,
                    json=jobs,
                    timeout=self.config.timeout_seconds,
                )
                if response.status_code in {429, 500, 502, 503, 504}:
                    raise requests.HTTPError(f"retryable HTTP {response.status_code}")
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, list):
                    raise ValueError("OpenFIGI response must be a list")
                return payload
            except (requests.RequestException, ValueError):
                if attempt == self.config.max_retries:
                    raise
                delay = min(60.0, (2 ** (attempt - 1)) + random.uniform(0.0, 0.25))
                time.sleep(delay)
        raise RuntimeError("OpenFIGI retry loop exhausted")


def enrich_symbol_stability(
    config: OpenFigiConfig,
    client: OpenFigiClient | None = None,
) -> dict[str, Any]:
    config.output_root.mkdir(parents=True, exist_ok=True)
    audit_rows = select_audit_rows(read_audit_rows(config.input_path), config)
    cache_path = config.output_root / "openfigi_cache.jsonl"
    cache = load_cache(cache_path)
    cache_entries_before = len(cache)
    figi_client = client or RequestsOpenFigiClient(config)
    responses = resolve_openfigi(audit_rows, config, figi_client, cache, cache_path)
    rows = [
        build_enriched_row(row, responses.get(cache_key(row["symbol"], config)))
        for row in audit_rows
    ]
    summary = build_summary(config, rows, cache_entries_before)
    write_outputs(config.output_root, summary, rows)
    return {"summary": summary, "rows": rows}


def read_audit_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def select_audit_rows(rows: list[dict[str, str]], config: OpenFigiConfig) -> list[dict[str, str]]:
    selected: list[dict[str, str]] = []
    for row in rows:
        symbol = (row.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        if config.symbols and symbol not in config.symbols:
            continue
        if config.classifications and row.get("classification") not in config.classifications:
            continue
        selected.append({**row, "symbol": symbol})
        if config.limit_symbols and len(selected) >= config.limit_symbols:
            break
    return selected


def resolve_openfigi(
    rows: list[dict[str, str]],
    config: OpenFigiConfig,
    client: OpenFigiClient,
    cache: dict[str, dict[str, Any]],
    cache_path: Path,
) -> dict[str, dict[str, Any]]:
    logger = logging.getLogger(__name__)
    keys = [cache_key(row["symbol"], config) for row in rows]
    missing = [row for row, key in zip(rows, keys, strict=True) if key not in cache]
    batches = chunked(missing, config.batch_size)
    logger.info(
        "OpenFIGI cache resolved",
        extra={
            "event": "openfigi_enrichment_cache_resolved",
            "detail": {
                "selected_symbols": len(rows),
                "cache_hits": len(rows) - len(missing),
                "cache_misses": len(missing),
                "batch_count": len(batches),
            },
        },
    )
    for index, batch in enumerate(batches, start=1):
        logger.info(
            "OpenFIGI batch start",
            extra={
                "event": "openfigi_enrichment_batch_start",
                "detail": {"batch_index": index, "batch_count": len(batches), "size": len(batch)},
            },
        )
        jobs = [build_openfigi_job(row["symbol"], config) for row in batch]
        batch_responses = client.map_jobs(jobs)
        if len(batch_responses) != len(batch):
            raise ValueError("OpenFIGI response count does not match request count")
        for row, response in zip(batch, batch_responses, strict=True):
            key = cache_key(row["symbol"], config)
            cache[key] = response
            append_cache(cache_path, key, response)
        logger.info(
            "OpenFIGI batch complete",
            extra={
                "event": "openfigi_enrichment_batch_complete",
                "detail": {"batch_index": index, "batch_count": len(batches), "size": len(batch)},
            },
        )
        if config.sleep_seconds:
            time.sleep(config.sleep_seconds)
    return {key: cache[key] for key in keys if key in cache}


def build_openfigi_job(symbol: str, config: OpenFigiConfig) -> dict[str, str]:
    job = {"idType": "TICKER", "idValue": symbol, "exchCode": config.exch_code}
    if config.market_sector:
        job["marketSecDes"] = config.market_sector
    return job


def build_enriched_row(row: dict[str, str], response: dict[str, Any] | None) -> dict[str, Any]:
    status, match_count, best, error = classify_response(response)
    symbol = row["symbol"]
    risk = classify_identity_risk(symbol, row.get("classification", ""), status, match_count, best)
    enriched = {key: row.get(key) for key in AUDIT_COLUMNS}
    enriched.update(
        {
            "openfigi_status": status,
            "openfigi_match_count": match_count,
            "identity_risk": risk,
            "openfigi_error": error,
        }
    )
    for key in OPENFIGI_COLUMNS:
        if key not in enriched:
            enriched[key] = best.get(key) if best else None
    return enriched


def classify_response(
    response: dict[str, Any] | None,
) -> tuple[str, int, dict[str, Any] | None, str | None]:
    if response is None:
        return "not_requested", 0, None, None
    if error := response.get("error"):
        return "error", 0, None, str(error)
    data = response.get("data") or []
    if not data:
        return "unmatched", 0, None, None
    status = "multiple_matches" if len(data) > 1 else "matched"
    return status, len(data), data[0], None


def classify_identity_risk(
    symbol: str,
    classification: str,
    status: str,
    match_count: int,
    best: dict[str, Any] | None,
) -> str:
    if status in {"error", "unmatched", "not_requested"}:
        return "unresolved"
    if match_count > 1:
        return "multiple_openfigi_matches"
    if best and str(best.get("ticker") or "").upper() != symbol:
        return "ticker_mismatch"
    if classification == "stable_candidate":
        return "stable_candidate_with_match"
    return "needs_review"


def build_summary(
    config: OpenFigiConfig, rows: list[dict[str, Any]], cache_entries_before: int
) -> dict[str, Any]:
    status_counts = count_by(rows, "openfigi_status")
    risk_counts = count_by(rows, "identity_risk")
    return {
        "generated_at": datetime.now().astimezone().isoformat(),
        "input_path": str(config.input_path),
        "output_root": str(config.output_root),
        "symbol_count": len(rows),
        "status_counts": status_counts,
        "identity_risk_counts": risk_counts,
        "cache_entries_before_run": cache_entries_before,
        "exch_code": config.exch_code,
        "market_sector": config.market_sector,
        "limitations": [
            "OpenFIGI ticker mapping is not a historical security master.",
            "Ticker-only requests can return multiple listings or current mappings.",
            "Use licensed CUSIP/ISIN or exchange listing history for issuer-level proof.",
        ],
    }


def load_cache(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    cache: dict[str, dict[str, Any]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        item = json.loads(line)
        cache[item["key"]] = item["response"]
    return cache


def append_cache(path: Path, key: str, response: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"key": key, "response": response}, sort_keys=True) + "\n")


def cache_key(symbol: str, config: OpenFigiConfig) -> str:
    return "|".join(["TICKER", symbol.upper(), config.exch_code, config.market_sector])


def count_by(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key) or "")
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def chunked(rows: list[dict[str, str]], size: int) -> list[list[dict[str, str]]]:
    if size < 1:
        raise ValueError("batch size must be positive")
    return [rows[index : index + size] for index in range(0, len(rows), size)]
