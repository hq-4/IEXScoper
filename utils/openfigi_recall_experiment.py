from __future__ import annotations

import argparse
import json
import logging
import os
import random
import time
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.framework.logging import get_logger, setup_logging
from utils.openfigi_identity_core import (
    IdentityConfig,
    RequestsIdentityClient,
    chunked,
    map_with_fallback,
)
from utils.openfigi_recall_metrics import build_report

OPENFIGI_SEARCH_URL = "https://api.openfigi.com/v3/search"
STANDARD_CACHE_PATH = Path("data/openfigi/identity_cache.jsonl")
RECALL_CACHE_PATH = Path("data/openfigi/recall_cache.jsonl")
IDENTITY_FACTS_PATH = Path("data/resolution/identity_facts.jsonl")
DEFAULT_REPORT_PATH = Path("reports/openfigi-identity/recall_experiment.json")
DEFAULT_SAMPLE_SIZE = 220
DEFAULT_SAMPLE_SEED = 20260803
DEFAULT_SLEEP_SECONDS = 0.25
SEARCH_TIMEOUT_SECONDS = 15.0
SEARCH_MAX_RETRIES = 5
BASELINE_OVERALL_RECALL = 1961 / 13256

MAPPING_VARIANTS = ("no_market_sector", "no_exch_code", "bare_ticker", "unlisted")
ALL_VARIANTS = (*MAPPING_VARIANTS, "search_endpoint")


def build_variant_job(symbol: str, variant: str) -> dict[str, Any]:
    job: dict[str, Any] = {"idType": "TICKER", "idValue": symbol}
    if variant == "no_market_sector":
        job["exchCode"] = "US"
    elif variant == "no_exch_code":
        job["marketSecDes"] = "Equity"
    elif variant == "unlisted":
        job["includeUnlistedEquities"] = True
    elif variant != "bare_ticker":
        raise ValueError(f"no mapping job for variant {variant}")
    return job


def load_unmatched_symbols(cache_path: Path) -> list[str]:
    unmatched: list[str] = []
    for line in cache_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        item = json.loads(line)
        response = item["response"]
        if not response.get("data"):
            unmatched.append(item["symbol"])
    return sorted(unmatched)


def load_ground_truth(path: Path) -> dict[str, str]:
    truth: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        fact = json.loads(line)
        symbol = str(fact.get("symbol") or "").strip().upper()
        issuer = str(fact.get("issuer") or "").strip()
        if symbol and issuer and symbol not in truth:
            truth[symbol] = issuer
    return truth


def build_sample(
    unmatched: list[str], ground_truth: dict[str, str], size: int, seed: int
) -> list[str]:
    gt_hits = sorted(symbol for symbol in unmatched if symbol in ground_truth)
    rest = [symbol for symbol in unmatched if symbol not in ground_truth]
    rng = random.Random(seed)
    fill = rng.sample(rest, min(len(rest), max(0, size - len(gt_hits))))
    return sorted(gt_hits + fill)


def load_recall_cache(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    cache: dict[str, dict[str, Any]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            item = json.loads(line)
            cache[f"{item['symbol']}|{item['variant']}"] = item["response"]
    return cache


def append_recall_cache(path: Path, symbol: str, variant: str, response: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        record = {"symbol": symbol, "variant": variant, "response": response}
        handle.write(json.dumps(record, sort_keys=True) + "\n")


def run_mapping_variant(
    symbols: list[str],
    variant: str,
    client: RequestsIdentityClient,
    cache: dict[str, dict[str, Any]],
    cache_path: Path,
    sleep_seconds: float,
) -> None:
    logger = logging.getLogger(__name__)
    missing = [s for s in symbols if f"{s}|{variant}" not in cache]
    batches = chunked(missing, 100)
    for index, batch in enumerate(batches, start=1):
        jobs = [build_variant_job(symbol, variant) for symbol in batch]
        responses = map_with_fallback(client, jobs)
        for symbol, response in zip(batch, responses, strict=True):
            cache[f"{symbol}|{variant}"] = response
            append_recall_cache(cache_path, symbol, variant, response)
        logger.info(
            "Recall variant batch complete",
            extra={
                "event": "openfigi_recall_batch",
                "detail": {"variant": variant, "batch": index, "batches": len(batches)},
            },
        )
        if sleep_seconds:
            time.sleep(sleep_seconds)


def run_search_variant(
    symbols: list[str],
    api_key: str | None,
    cache: dict[str, dict[str, Any]],
    cache_path: Path,
    sleep_seconds: float,
) -> None:
    for symbol in symbols:
        if f"{symbol}|search_endpoint" in cache:
            continue
        response = search_symbol(symbol, api_key)
        cache[f"{symbol}|search_endpoint"] = response
        append_recall_cache(cache_path, symbol, "search_endpoint", response)
        if sleep_seconds:
            time.sleep(sleep_seconds)


def search_symbol(symbol: str, api_key: str | None) -> dict[str, Any]:
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-OPENFIGI-APIKEY"] = api_key
    for attempt in range(1, SEARCH_MAX_RETRIES + 1):
        try:
            response = requests.post(
                OPENFIGI_SEARCH_URL,
                json={"query": symbol},
                headers=headers,
                timeout=SEARCH_TIMEOUT_SECONDS,
            )
            if response.status_code in {429, 500, 502, 503, 504}:
                raise requests.HTTPError(f"retryable HTTP {response.status_code}")
            response.raise_for_status()
            return {"data": normalize_search_payload(response.json())}
        except (requests.RequestException, ValueError) as exc:
            if attempt == SEARCH_MAX_RETRIES or not is_retryable_error(exc):
                return {"error": f"request_failed: {exc}"}
            time.sleep(min(60.0, 2 ** (attempt - 1) + random.uniform(0.0, 0.25)))
    return {"error": "retry loop exhausted"}


def is_retryable_error(exc: Exception) -> bool:
    response = getattr(exc, "response", None)
    if response is None:
        return True
    return response.status_code in {429, 500, 502, 503, 504}


def normalize_search_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("data", "results", "instruments"):
            if isinstance(payload.get(key), list):
                return payload[key]
    return []


def main() -> int:
    load_dotenv()
    args = parse_args()
    api_key = os.getenv(args.api_key_env)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    setup_logging(str(args.output.parent / "openfigi_recall.jsonl"))
    logger = get_logger(__name__)
    unmatched = load_unmatched_symbols(STANDARD_CACHE_PATH)
    ground_truth = load_ground_truth(IDENTITY_FACTS_PATH)
    sample = build_sample(unmatched, ground_truth, args.sample_size, args.seed)
    cache = load_recall_cache(RECALL_CACHE_PATH)
    config = IdentityConfig(
        input_path=Path("data/resolution/observation_facts.jsonl"),
        output_root=args.output.parent,
        api_key=api_key,
    )
    client = RequestsIdentityClient(config)
    for variant in MAPPING_VARIANTS:
        run_mapping_variant(sample, variant, client, cache, RECALL_CACHE_PATH, args.sleep_seconds)
    run_search_variant(sample, api_key, cache, RECALL_CACHE_PATH, args.sleep_seconds)
    report = build_report(sample, cache, ground_truth, ALL_VARIANTS, BASELINE_OVERALL_RECALL)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    logger.info(
        "Recall experiment complete",
        extra={"event": "openfigi_recall_complete", "detail": report["recommendation"]},
    )
    return 0
    for variant in MAPPING_VARIANTS:
        run_mapping_variant(sample, variant, client, cache, RECALL_CACHE_PATH, args.sleep_seconds)
    run_search_variant(sample, api_key, cache, RECALL_CACHE_PATH, args.sleep_seconds)
    report = build_report(sample, cache, ground_truth)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    logger.info(
        "Recall experiment complete",
        extra={"event": "openfigi_recall_complete", "detail": report["recommendation"]},
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-key-env", default="OPENFIGI_API_KEY")
    parser.add_argument("--sample-size", type=int, default=DEFAULT_SAMPLE_SIZE)
    parser.add_argument("--seed", type=int, default=DEFAULT_SAMPLE_SEED)
    parser.add_argument("--sleep-seconds", type=float, default=DEFAULT_SLEEP_SECONDS)
    parser.add_argument("--output", type=Path, default=DEFAULT_REPORT_PATH)
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(main())
