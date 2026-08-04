from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.framework.logging import get_logger, setup_logging
from utils.build_openfigi_symbol_identities import (
    DEFAULT_HINTS_PATH,
    DEFAULT_INPUT,
    DEFAULT_OUTPUT_ROOT,
    regex_fund_census,
)
from utils.openfigi_identity_core import (
    IdentityConfig,
    RequestsIdentityClient,
    build_era_rows,
    build_summary,
    rows_for_symbol,
)
from utils.openfigi_identity_outputs import load_cache, load_eras, write_outputs
from utils.openfigi_recall_experiment import (
    RECALL_CACHE_PATH,
    STANDARD_CACHE_PATH,
    load_recall_cache,
    load_unmatched_symbols,
    run_mapping_variant,
)
from utils.openfigi_recall_metrics import split_us_matches

DEFAULT_SLEEP_SECONDS = 0.25


def main() -> int:
    load_dotenv()
    args = parse_args()
    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    setup_logging(str(output_root / "openfigi_full_pass.jsonl"))
    logger = get_logger(__name__)
    config = IdentityConfig(
        input_path=Path(args.input),
        output_root=output_root,
        api_key=os.getenv(args.api_key_env),
    )
    if not args.cached_only:
        run_full_pass(args.variant, config, args.sleep_seconds, logger)
    result = regenerate_outputs(args.variant, config)
    logger.info(
        "Full pass complete",
        extra={"event": "openfigi_full_pass_complete", "detail": result},
    )
    return 0


def run_full_pass(variant: str, config: IdentityConfig, sleep_seconds: float, logger: Any) -> None:
    unmatched = load_unmatched_symbols(STANDARD_CACHE_PATH)
    cache = load_recall_cache(RECALL_CACHE_PATH)
    logger.info(
        "Full pass start",
        extra={
            "event": "openfigi_full_pass_start",
            "detail": {"variant": variant, "unmatched_symbols": len(unmatched)},
        },
    )
    client = RequestsIdentityClient(config)
    run_mapping_variant(unmatched, variant, client, cache, RECALL_CACHE_PATH, sleep_seconds)


def regenerate_outputs(variant: str, config: IdentityConfig) -> dict[str, Any]:
    eras = load_eras(config.input_path)
    symbols = sorted({era["symbol"] for era in eras if era["symbol"]})
    standard = load_cache(STANDARD_CACHE_PATH)
    recall = load_recall_cache(RECALL_CACHE_PATH)
    map_rows = merged_map_rows(symbols, standard, recall, variant)
    era_rows = build_era_rows(eras, map_rows)
    summary = build_summary(config, symbols, map_rows)
    summary["full_pass_variant"] = variant
    summary["regex_classifier_comparison"] = regex_fund_census(era_rows, DEFAULT_HINTS_PATH)
    write_outputs(config.output_root, map_rows, era_rows, summary)
    return {
        "matched_symbols": summary["matched_symbols"],
        "total_symbols": summary["total_symbols"],
        "variant": variant,
    }


def merged_map_rows(
    symbols: list[str],
    standard: dict[str, dict[str, Any]],
    recall: dict[str, dict[str, Any]],
    variant: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for symbol in symbols:
        base = standard.get(symbol) or {}
        if base.get("data"):
            rows.extend(with_variant(rows_for_symbol(symbol, base), "standard"))
            continue
        us, _noise = split_us_matches(recall.get(f"{symbol}|{variant}"), variant)
        us = dedupe_by_composite(us)
        if us:
            rows.extend(with_variant(rows_for_symbol(symbol, {"data": us}), variant))
        else:
            rows.extend(with_variant(rows_for_symbol(symbol, base), "unresolved"))
    return rows


def dedupe_by_composite(matches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for match in matches:
        key = str(match.get("compositeFIGI") or match.get("figi") or "")
        if key not in seen:
            seen.add(key)
            unique.append(match)
    return unique


def with_variant(rows: list[dict[str, Any]], variant: str) -> list[dict[str, Any]]:
    return [{**row, "query_variant": variant} for row in rows]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--variant", required=True, help="Winning recall variant to apply")
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--api-key-env", default="OPENFIGI_API_KEY")
    parser.add_argument("--sleep-seconds", type=float, default=DEFAULT_SLEEP_SECONDS)
    parser.add_argument(
        "--cached-only",
        action="store_true",
        help="Skip the network pass; regenerate outputs from existing caches only",
    )
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(main())
