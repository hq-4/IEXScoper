"""Build the session-validity quarantine manifest over the TOPS corpus.

Scans each TOPS main Parquet day, classifies the session, and writes a manifest consumed
by era construction (`build_symbol_stability_audit.py`) and daily bars
(`build_daily_trade_bars.py`) via `--quarantine-path`. [REH][PA][CDiP]
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.framework.logging import get_logger, setup_logging
from utils.iextools_backfill_core import existing_tops_days
from utils.session_validity import (
    MIN_TRADE_SHARE,
    build_validity_manifest,
    write_validity_manifest,
)

DEFAULT_PARQUET_ROOT = Path("/media/tn/pq")
DEFAULT_OUTPUT_PATH = Path("reports/symbol-stability/session_validity.json")
DEFAULT_START_DAY = "20160101"
DEFAULT_END_DAY = "20260622"


@dataclass(frozen=True)
class SessionValidityConfig:
    parquet_root: Path
    output_path: Path
    start_day: str
    end_day: str
    min_trade_share: float
    limit_days: int | None


def parse_args() -> SessionValidityConfig:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parquet-root", default=str(DEFAULT_PARQUET_ROOT))
    parser.add_argument("--output-path", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument("--start-day", default=DEFAULT_START_DAY)
    parser.add_argument("--end-day", default=DEFAULT_END_DAY)
    parser.add_argument("--min-trade-share", type=float, default=MIN_TRADE_SHARE)
    parser.add_argument("--limit-days", type=int)
    args = parser.parse_args()
    return SessionValidityConfig(
        parquet_root=Path(args.parquet_root),
        output_path=Path(args.output_path),
        start_day=args.start_day,
        end_day=args.end_day,
        min_trade_share=args.min_trade_share,
        limit_days=args.limit_days,
    )


def discover_days(config: SessionValidityConfig) -> list[str]:
    days = [
        day
        for day in sorted(existing_tops_days(config.parquet_root))
        if config.start_day <= day <= config.end_day
    ]
    if config.limit_days is not None:
        return days[: config.limit_days]
    return days


def main() -> int:
    config = parse_args()
    setup_logging(str(config.output_path.parent / "session_validity.jsonl"))
    days = discover_days(config)
    manifest = build_validity_manifest(
        config.parquet_root, days, min_trade_share=config.min_trade_share
    )
    write_validity_manifest(config.output_path, manifest)
    get_logger(__name__).info(
        "session validity manifest complete",
        extra={"event": "session_validity_complete", "detail": manifest},
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
