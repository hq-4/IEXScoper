from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import polars as pl

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.framework.logging import get_logger, setup_logging
from utils.dead_ticker_review_schema import DEFAULT_OUTPUT_ROOT

DEFAULT_INPUT_PATH = DEFAULT_OUTPUT_ROOT / "resolution-lanes" / "operating_terminal_event.csv"
DEFAULT_OUTPUT_PATH = (
    DEFAULT_OUTPUT_ROOT / "resolution-lanes" / "operating_terminal_event_terminal_window.csv"
)
DEFAULT_SUMMARY_PATH = (
    DEFAULT_OUTPUT_ROOT / "resolution-lanes" / "operating_terminal_event_terminal_window_summary.json"
)
DEFAULT_LOOKBACK_DAYS = 180
DEFAULT_LOOKAHEAD_DAYS = 60


@dataclass(frozen=True)
class TerminalEventSearchBatchConfig:
    input_path: Path
    output_path: Path
    summary_path: Path
    symbols: tuple[str, ...]
    limit: int | None
    lookback_days: int
    lookahead_days: int


def main() -> int:
    args = parse_args()
    config = TerminalEventSearchBatchConfig(
        input_path=Path(args.input_path),
        output_path=Path(args.output_path),
        summary_path=Path(args.summary_path),
        symbols=parse_symbols(args.symbols),
        limit=args.limit,
        lookback_days=args.lookback_days,
        lookahead_days=args.lookahead_days,
    )
    setup_logging(str(config.summary_path.with_suffix(".jsonl")))
    result = build_terminal_event_search_batch(config)
    get_logger(__name__).info(
        "terminal event search batch complete",
        extra={"event": "terminal_event_search_batch_complete", "detail": result["summary"]},
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", default=str(DEFAULT_INPUT_PATH))
    parser.add_argument("--output-path", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument("--summary-path", default=str(DEFAULT_SUMMARY_PATH))
    parser.add_argument("--symbols")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--lookahead-days", type=int, default=DEFAULT_LOOKAHEAD_DAYS)
    return parser.parse_args()


def parse_symbols(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    return tuple(symbol.strip().upper() for symbol in value.split(",") if symbol.strip())


def build_terminal_event_search_batch(
    config: TerminalEventSearchBatchConfig,
) -> dict[str, Any]:
    validate_config(config)
    source = pl.read_csv(config.input_path, infer_schema_length=0)
    require_columns(source)
    batch = terminal_window_rows(source, config)
    summary = build_summary(config, source, batch)
    write_outputs(config, batch, summary)
    return {"summary": summary, "rows": batch.to_dicts()}


def validate_config(config: TerminalEventSearchBatchConfig) -> None:
    if not config.input_path.exists():
        raise FileNotFoundError(f"input batch does not exist: {config.input_path}")
    if config.lookback_days < 0:
        raise ValueError("--lookback-days cannot be negative")
    if config.lookahead_days < 0:
        raise ValueError("--lookahead-days cannot be negative")
    if config.limit is not None and config.limit <= 0:
        raise ValueError("--limit must be positive")


def require_columns(frame: pl.DataFrame) -> None:
    required = ["symbol", "symbol_era_id", "first_day", "last_day", "priority_rank"]
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"input batch missing required columns: {missing}")


def terminal_window_rows(
    source: pl.DataFrame, config: TerminalEventSearchBatchConfig
) -> pl.DataFrame:
    rows = source
    if config.symbols:
        rows = rows.filter(pl.col("symbol").str.to_uppercase().is_in(config.symbols))
    rows = rows.sort(pl.col("priority_rank").cast(pl.Int64, strict=False))
    if config.limit:
        rows = rows.head(config.limit)
    return (
        rows.with_columns(
            pl.col("first_day").alias("original_first_day"),
            pl.col("last_day").alias("original_last_day"),
            pl.col("last_day")
            .map_elements(
                lambda value: shifted_day(value, -config.lookback_days),
                return_dtype=pl.String,
            )
            .alias("first_day"),
            pl.col("last_day")
            .map_elements(
                lambda value: shifted_day(value, config.lookahead_days),
                return_dtype=pl.String,
            )
            .alias("last_day"),
        )
        .with_columns(
            pl.concat_str(
                [
                    pl.lit("terminal_window_"),
                    pl.lit(str(config.lookback_days)),
                    pl.lit("d_before_"),
                    pl.lit(str(config.lookahead_days)),
                    pl.lit("d_after"),
                ]
            ).alias("search_window_reason")
        )
    )


def shifted_day(value: Any, days: int) -> str | None:
    text = str(value or "")
    if len(text) != 8 or not text.isdigit():
        return None
    parsed = datetime.strptime(text, "%Y%m%d").date()
    return (parsed + timedelta(days=days)).strftime("%Y%m%d")


def build_summary(
    config: TerminalEventSearchBatchConfig, source: pl.DataFrame, batch: pl.DataFrame
) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "terminal event SEC search window batch",
        "input_path": str(config.input_path),
        "output_path": str(config.output_path),
        "input_row_count": source.height,
        "row_count": batch.height,
        "symbol_count": batch["symbol"].n_unique() if batch.height else 0,
        "symbols": batch["symbol"].to_list() if batch.height <= 50 else [],
        "lookback_days": config.lookback_days,
        "lookahead_days": config.lookahead_days,
        "limitations": [
            "This rewrites first_day/last_day for SEC search only.",
            "Original era bounds are preserved in original_first_day/original_last_day.",
        ],
    }


def write_outputs(
    config: TerminalEventSearchBatchConfig, batch: pl.DataFrame, summary: dict[str, Any]
) -> None:
    config.output_path.parent.mkdir(parents=True, exist_ok=True)
    config.summary_path.parent.mkdir(parents=True, exist_ok=True)
    batch.write_csv(config.output_path)
    config.summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
