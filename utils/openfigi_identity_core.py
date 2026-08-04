from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

import requests

from utils.openfigi_identity_outputs import append_cache, load_cache, load_eras

OPENFIGI_MAPPING_URL = "https://api.openfigi.com/v3/mapping"
DEFAULT_BATCH_SIZE = 100
DEFAULT_SLEEP_SECONDS = 0.25
DEFAULT_TIMEOUT_SECONDS = 15.0
DEFAULT_MAX_RETRIES = 5
DEFAULT_CACHE_PATH = Path("data/openfigi/identity_cache.jsonl")
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
TOP_SECURITY_TYPE_LIMIT = 20

CLASS_FUND_ETF = "fund_etf"
CLASS_EQUITY_COMMON = "equity_common"
CLASS_ADR = "adr"
CLASS_REIT = "reit"
CLASS_PREFERRED = "preferred"
CLASS_UNIT = "unit"
CLASS_RIGHT = "right"
CLASS_WARRANT = "warrant"

SECURITY_TYPE2_CLASS_MAP = {
    "ETP": CLASS_FUND_ETF,
    "Mutual Fund": CLASS_FUND_ETF,
    "Common Stock": CLASS_EQUITY_COMMON,
    "ADR": CLASS_ADR,
    "Depositary Receipt": CLASS_ADR,
    "REIT": CLASS_REIT,
    "Preferred": CLASS_PREFERRED,
    "Preference": CLASS_PREFERRED,
    "Depositary": CLASS_PREFERRED,
    "Unit": CLASS_UNIT,
    "Right": CLASS_RIGHT,
    "Warrant": CLASS_WARRANT,
}
FUND_MARKET_SECTORS = {"fund", "funds"}

MAP_COLUMNS = (
    "symbol",
    "figi",
    "composite_figi",
    "name",
    "security_type",
    "security_type2",
    "market_sector",
    "exch_code",
    "security_description",
    "match_status",
    "figi_count",
    "openfigi_class",
)


class IdentityClient(Protocol):
    def map_jobs(self, jobs: list[dict[str, str]]) -> list[dict[str, Any]]: ...


@dataclass(frozen=True)
class IdentityConfig:
    input_path: Path
    output_root: Path
    api_key: str | None
    cache_path: Path = DEFAULT_CACHE_PATH
    batch_size: int = DEFAULT_BATCH_SIZE
    sleep_seconds: float = DEFAULT_SLEEP_SECONDS
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    max_retries: int = DEFAULT_MAX_RETRIES
    exch_code: str = "US"
    market_sector: str = "Equity"
    limit_symbols: int | None = None


class RequestsIdentityClient:
    def __init__(self, config: IdentityConfig) -> None:
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
                if response.status_code in RETRYABLE_STATUS_CODES:
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


def collect_identity_data(
    config: IdentityConfig,
    client: IdentityClient | None = None,
) -> dict[str, Any]:
    eras = load_eras(config.input_path)
    symbols = unique_symbols(eras, config.limit_symbols)
    cache = load_cache(config.cache_path)
    figi_client = client or RequestsIdentityClient(config)
    responses = resolve_identities(symbols, config, figi_client, cache)
    map_rows = build_symbol_map_rows(symbols, responses)
    era_rows = build_era_rows(eras, map_rows)
    summary = build_summary(config, symbols, map_rows)
    return {
        "symbols": symbols,
        "eras": eras,
        "map_rows": map_rows,
        "era_rows": era_rows,
        "summary": summary,
    }


def unique_symbols(eras: list[dict[str, str]], limit: int | None = None) -> list[str]:
    symbols = sorted({era["symbol"] for era in eras if era["symbol"]})
    return symbols[:limit] if limit else symbols


def resolve_identities(
    symbols: list[str],
    config: IdentityConfig,
    client: IdentityClient,
    cache: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    logger = logging.getLogger(__name__)
    missing = [symbol for symbol in symbols if symbol not in cache]
    batches = chunked(missing, config.batch_size)
    logger.info(
        "OpenFIGI identity cache resolved",
        extra={
            "event": "openfigi_identity_cache_resolved",
            "detail": {
                "symbols": len(symbols),
                "cache_hits": len(symbols) - len(missing),
                "cache_misses": len(missing),
                "batch_count": len(batches),
            },
        },
    )
    for index, batch in enumerate(batches, start=1):
        jobs = [build_openfigi_job(symbol, config) for symbol in batch]
        responses = map_with_fallback(client, jobs)
        for symbol, response in zip(batch, responses, strict=True):
            cache[symbol] = response
            append_cache(config.cache_path, symbol, response)
        logger.info(
            "OpenFIGI identity batch complete",
            extra={
                "event": "openfigi_identity_batch_complete",
                "detail": {"batch_index": index, "batch_count": len(batches)},
            },
        )
        if config.sleep_seconds:
            time.sleep(config.sleep_seconds)
    return {symbol: cache[symbol] for symbol in symbols if symbol in cache}


def map_with_fallback(client: IdentityClient, jobs: list[dict[str, str]]) -> list[dict[str, Any]]:
    try:
        responses = client.map_jobs(jobs)
    except (requests.RequestException, ValueError, RuntimeError) as exc:
        if len(jobs) == 1:
            return [{"error": f"request_failed: {exc}"}]
        return [map_with_fallback(client, [job])[0] for job in jobs]
    if len(responses) != len(jobs):
        raise ValueError("OpenFIGI response count does not match request count")
    return responses


def build_openfigi_job(symbol: str, config: IdentityConfig) -> dict[str, str]:
    return {
        "idType": "TICKER",
        "idValue": symbol,
        "exchCode": config.exch_code,
        "marketSecDes": config.market_sector,
    }


def build_symbol_map_rows(
    symbols: list[str], responses: dict[str, dict[str, Any]]
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for symbol in symbols:
        rows.extend(rows_for_symbol(symbol, responses.get(symbol)))
    return rows


def rows_for_symbol(symbol: str, response: dict[str, Any] | None) -> list[dict[str, Any]]:
    data = (response or {}).get("data") or []
    if not data:
        status = "error" if (response or {}).get("error") else "unmatched"
        return [_map_row(symbol, None, status, 0, (response or {}).get("error"))]
    status = "single" if len(data) == 1 else "multi"
    return [_map_row(symbol, item, status, len(data), None) for item in data]


def _map_row(
    symbol: str,
    item: dict[str, Any] | None,
    status: str,
    figi_count: int,
    error: Any,
) -> dict[str, Any]:
    item = item or {}
    return {
        "symbol": symbol,
        "figi": item.get("figi"),
        "composite_figi": item.get("compositeFIGI"),
        "name": item.get("name"),
        "security_type": item.get("securityType"),
        "security_type2": item.get("securityType2"),
        "market_sector": item.get("marketSector"),
        "exch_code": item.get("exchCode"),
        "security_description": item.get("securityDescription"),
        "match_status": status,
        "figi_count": figi_count,
        "openfigi_class": classify_instrument(
            item.get("securityType2"), item.get("securityType"), item.get("marketSector")
        )
        if item
        else None,
        "error": str(error) if error else None,
    }


def classify_instrument(
    security_type2: Any, security_type: Any = None, market_sector: Any = None
) -> str:
    type2 = str(security_type2 or "").strip()
    sector = str(market_sector or "").strip().lower()
    if type2 in SECURITY_TYPE2_CLASS_MAP:
        return SECURITY_TYPE2_CLASS_MAP[type2]
    if sector in FUND_MARKET_SECTORS:
        return CLASS_FUND_ETF
    if type2:
        return f"other_{type2.lower()}"
    fallback = str(security_type or "").strip()
    return f"other_{fallback.lower()}" if fallback else "other"


def build_era_rows(
    eras: list[dict[str, str]], map_rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    first_match = {}
    for row in map_rows:
        first_match.setdefault(row["symbol"], row)
    era_rows: list[dict[str, Any]] = []
    for era in eras:
        match = first_match.get(era["symbol"], {})
        era_rows.append(
            {
                **era,
                "openfigi_class": match.get("openfigi_class"),
                "match_status": match.get("match_status", "unmatched"),
                "figi_count": match.get("figi_count", 0),
                "best_name": match.get("name"),
            }
        )
    return era_rows


def build_summary(
    config: IdentityConfig, symbols: list[str], map_rows: list[dict[str, Any]]
) -> dict[str, Any]:
    per_symbol: dict[str, dict[str, Any]] = {}
    for row in map_rows:
        per_symbol.setdefault(row["symbol"], row)
    status_counts = count_values(row["match_status"] for row in per_symbol.values())
    class_counts = count_values(
        row["openfigi_class"] or row["match_status"] for row in per_symbol.values()
    )
    type2_counts = count_values(
        str(row["security_type2"]) for row in map_rows if row["security_type2"]
    )
    return {
        "generated_at": datetime.now().astimezone().isoformat(),
        "input_path": str(config.input_path),
        "output_root": str(config.output_root),
        "has_api_key": bool(config.api_key),
        "total_symbols": len(symbols),
        "matched_symbols": sum(
            count for status, count in status_counts.items() if status in {"single", "multi"}
        ),
        "unmatched_symbols": status_counts.get("unmatched", 0),
        "error_symbols": status_counts.get("error", 0),
        "single_figi_symbols": status_counts.get("single", 0),
        "multi_figi_symbols": status_counts.get("multi", 0),
        "total_figi_matches": sum(row["figi_count"] for row in per_symbol.values()),
        "class_distribution": class_counts,
        "top_security_type2": dict(
            sorted(type2_counts.items(), key=lambda kv: kv[1], reverse=True)[
                :TOP_SECURITY_TYPE_LIMIT
            ]
        ),
        "limitations": [
            "OpenFIGI ticker mapping is not a historical security master.",
            "Multiple FIGIs per ticker indicate ticker reuse across eras.",
            "Era-level binding of multi-FIGI symbols is a later phase.",
        ],
    }


def count_values(values: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        key = str(value)
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def chunked(items: list[str], size: int) -> list[list[str]]:
    if size < 1:
        raise ValueError("batch size must be positive")
    return [items[index : index + size] for index in range(0, len(items), size)]
