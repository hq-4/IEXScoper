from __future__ import annotations

import argparse
import os
from pathlib import Path

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.framework.logging import get_logger, setup_logging
from utils.openfigi_enrichment_core import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_MAX_RETRIES,
    DEFAULT_SLEEP_SECONDS,
    DEFAULT_TIMEOUT_SECONDS,
    OpenFigiConfig,
    enrich_symbol_stability,
)

DEFAULT_INPUT = Path("reports/symbol-stability/symbol_stability_rows.csv")
DEFAULT_OUTPUT_ROOT = Path("reports/symbol-stability-openfigi")


def main() -> int:
    args = parse_args()
    config = OpenFigiConfig(
        input_path=Path(args.input),
        output_root=Path(args.output_root),
        api_key=os.getenv(args.api_key_env),
        batch_size=args.batch_size,
        sleep_seconds=args.sleep_seconds,
        timeout_seconds=args.timeout_seconds,
        max_retries=args.max_retries,
        exch_code=args.exch_code,
        market_sector=args.market_sector,
        symbols=parse_csv_set(args.symbols),
        classifications=parse_csv_set(args.only_classifications),
        limit_symbols=args.limit_symbols,
    )
    setup_logging(str(config.output_root / "openfigi_enrichment.jsonl"))
    logger = get_logger(__name__)
    logger.info(
        "OpenFIGI enrichment start",
        extra={
            "event": "openfigi_enrichment_start",
            "detail": {
                "input_path": str(config.input_path),
                "output_root": str(config.output_root),
                "batch_size": config.batch_size,
                "has_api_key": bool(config.api_key),
                "limit_symbols": config.limit_symbols,
            },
        },
    )
    result = enrich_symbol_stability(config)
    logger.info(
        "OpenFIGI enrichment complete",
        extra={"event": "openfigi_enrichment_complete", "detail": result["summary"]},
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--api-key-env", default="OPENFIGI_API_KEY")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--sleep-seconds", type=float, default=DEFAULT_SLEEP_SECONDS)
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES)
    parser.add_argument("--exch-code", default="US")
    parser.add_argument("--market-sector", default="Equity")
    parser.add_argument("--symbols", help="Comma-separated symbols to enrich, e.g. C,SNOW,XLNX")
    parser.add_argument("--only-classifications", help="Comma-separated audit classifications")
    parser.add_argument("--limit-symbols", type=int)
    return parser.parse_args()


def parse_csv_set(value: str | None) -> frozenset[str] | None:
    if not value:
        return None
    parsed = frozenset(part.strip().upper() for part in value.split(",") if part.strip())
    return parsed or None


if __name__ == "__main__":
    raise SystemExit(main())
