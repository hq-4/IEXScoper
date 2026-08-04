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
from utils.openfigi_identity_core import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_SLEEP_SECONDS,
    IdentityConfig,
    collect_identity_data,
)
from utils.openfigi_identity_outputs import write_outputs

DEFAULT_INPUT = Path("data/resolution/observation_facts.jsonl")
DEFAULT_OUTPUT_ROOT = Path("reports/openfigi-identity")
DEFAULT_HINTS_PATH = Path("reports/dead-ticker-review/instrument_heuristic_audit.csv")


def main() -> int:
    load_dotenv()
    args = parse_args()
    config = IdentityConfig(
        input_path=Path(args.input),
        output_root=Path(args.output_root),
        api_key=os.getenv(args.api_key_env),
        batch_size=args.batch_size,
        sleep_seconds=args.sleep_seconds,
        limit_symbols=args.limit_symbols,
    )
    config.output_root.mkdir(parents=True, exist_ok=True)
    setup_logging(str(config.output_root / "openfigi_identity.jsonl"))
    logger = get_logger(__name__)
    logger.info(
        "OpenFIGI identity start",
        extra={
            "event": "openfigi_identity_start",
            "detail": {
                "input_path": str(config.input_path),
                "output_root": str(config.output_root),
                "batch_size": config.batch_size,
                "has_api_key": bool(config.api_key),
                "limit_symbols": config.limit_symbols,
            },
        },
    )
    if not config.api_key:
        logger.error(
            "OpenFIGI API key missing",
            extra={"event": "openfigi_identity_no_api_key", "detail": {"env": args.api_key_env}},
        )
        return 1
    result = collect_identity_data(config)
    summary = result["summary"]
    summary["regex_classifier_comparison"] = regex_fund_census(
        result["era_rows"], Path(args.hints_path)
    )
    write_outputs(config.output_root, result["map_rows"], result["era_rows"], summary)
    logger.info(
        "OpenFIGI identity complete",
        extra={"event": "openfigi_identity_complete", "detail": summary},
    )
    return 0


def regex_fund_census(era_rows: list[dict[str, Any]], hints_path: Path) -> dict[str, Any]:
    try:
        from utils.instrument_classifier import TYPE_FUND_OR_TRUST, classify_instrument
    except ImportError:
        return {"status": "skipped", "note": "utils.instrument_classifier not importable"}
    hints = load_iex_hints(hints_path)
    total = len(era_rows)
    fund = 0
    for era in era_rows:
        hint = hints.get(era["symbol_era_id"], {})
        classification = classify_instrument(
            era["symbol"], hint.get("iex_product_hint"), hint.get("iex_latest_issuer")
        )
        if classification.instrument_type == TYPE_FUND_OR_TRUST:
            fund += 1
    openfigi_fund = sum(1 for era in era_rows if era["openfigi_class"] == "fund_etf")
    return {
        "status": "ok",
        "hints_source": str(hints_path) if hints else None,
        "total_eras": total,
        "regex_fund_or_trust_eras": fund,
        "regex_fund_or_trust_share": round(fund / total, 4) if total else 0.0,
        "openfigi_fund_etf_eras": openfigi_fund,
        "openfigi_fund_etf_share": round(openfigi_fund / total, 4) if total else 0.0,
        "note": (
            "Regex estimate uses symbol plus IEX product/issuer hints where available; "
            "without hints the regex fund share is understated. openfigi_fund_etf counts "
            "eras whose mapped class is fund_etf (securityType2 'ETP' or 'Mutual Fund')."
        ),
    }


def load_iex_hints(hints_path: Path) -> dict[str, dict[str, Any]]:
    if not hints_path.exists():
        return {}
    import polars as pl

    frame = pl.read_csv(hints_path).select(
        ["symbol_era_id", "iex_product_hint", "iex_latest_issuer"]
    )
    return {
        row["symbol_era_id"]: row for row in frame.iter_rows(named=True) if row["symbol_era_id"]
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--api-key-env", default="OPENFIGI_API_KEY")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--sleep-seconds", type=float, default=DEFAULT_SLEEP_SECONDS)
    parser.add_argument("--limit-symbols", type=int)
    parser.add_argument("--hints-path", default=str(DEFAULT_HINTS_PATH))
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(main())
