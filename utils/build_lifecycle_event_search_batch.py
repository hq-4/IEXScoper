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

DEFAULT_INPUT_PATH = DEFAULT_OUTPUT_ROOT / "resolution-workplan" / "workplan_operating_lifecycle_search.csv"
DEFAULT_OUTPUT_PATH = DEFAULT_OUTPUT_ROOT / "sec-lifecycle-iterations" / "lifecycle_window.csv"
DEFAULT_SUMMARY_PATH = DEFAULT_OUTPUT_ROOT / "sec-lifecycle-iterations" / "lifecycle_window_summary.json"


@dataclass(frozen=True)
class LifecycleEventSearchBatchConfig:
    input_path: Path
    output_path: Path
    summary_path: Path
    limit: int | None
    first_lookback_days: int
    first_lookahead_days: int
    last_lookback_days: int
    last_lookahead_days: int


def main() -> int:
    args = parse_args()
    config = LifecycleEventSearchBatchConfig(
        input_path=Path(args.input_path),
        output_path=Path(args.output_path),
        summary_path=Path(args.summary_path),
        limit=args.limit,
        first_lookback_days=args.first_lookback_days,
        first_lookahead_days=args.first_lookahead_days,
        last_lookback_days=args.last_lookback_days,
        last_lookahead_days=args.last_lookahead_days,
    )
    setup_logging(str(config.summary_path.with_suffix(".jsonl")))
    result = build_lifecycle_event_search_batch(config)
    get_logger(__name__).info(
        "lifecycle event search batch complete",
        extra={"event": "lifecycle_event_search_batch_complete", "detail": result["summary"]},
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", default=str(DEFAULT_INPUT_PATH))
    parser.add_argument("--output-path", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument("--summary-path", default=str(DEFAULT_SUMMARY_PATH))
    parser.add_argument("--limit", type=int)
    parser.add_argument("--first-lookback-days", type=int, default=45)
    parser.add_argument("--first-lookahead-days", type=int, default=180)
    parser.add_argument("--last-lookback-days", type=int, default=180)
    parser.add_argument("--last-lookahead-days", type=int, default=90)
    return parser.parse_args()


def build_lifecycle_event_search_batch(config: LifecycleEventSearchBatchConfig) -> dict[str, Any]:
    validate_config(config)
    source = pl.read_csv(config.input_path, infer_schema_length=0)
    require_columns(source)
    limited = source.sort(pl.col("priority_rank").cast(pl.Int64, strict=False))
    limited = limited.head(config.limit) if config.limit else limited
    batch = pl.concat([anchor_rows(limited, config, "first"), anchor_rows(limited, config, "last")])
    summary = build_summary(config, source, limited, batch)
    write_outputs(config, batch, summary)
    return {"summary": summary, "rows": batch.to_dicts()}


def validate_config(config: LifecycleEventSearchBatchConfig) -> None:
    if not config.input_path.exists():
        raise FileNotFoundError(f"input batch does not exist: {config.input_path}")
    windows = (
        config.first_lookback_days,
        config.first_lookahead_days,
        config.last_lookback_days,
        config.last_lookahead_days,
    )
    if min(windows) < 0:
        raise ValueError("lookback/lookahead days cannot be negative")
    if config.limit is not None and config.limit <= 0:
        raise ValueError("--limit must be positive")


def require_columns(frame: pl.DataFrame) -> None:
    required = ["symbol", "symbol_era_id", "first_day", "last_day", "priority_rank"]
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"input batch missing required columns: {missing}")


def anchor_rows(
    source: pl.DataFrame, config: LifecycleEventSearchBatchConfig, anchor: str
) -> pl.DataFrame:
    before, after = anchor_window(config, anchor)
    target_column = f"{anchor}_day"
    return source.with_columns(
        pl.col("first_day").alias("original_first_day"),
        pl.col("last_day").alias("original_last_day"),
        pl.lit(anchor).alias("lifecycle_anchor"),
        pl.col(target_column).alias("lifecycle_target_day"),
        pl.col(target_column)
        .map_elements(lambda value: shifted_day(value, -before), return_dtype=pl.String)
        .alias("first_day"),
        pl.col(target_column)
        .map_elements(lambda value: shifted_day(value, after), return_dtype=pl.String)
        .alias("last_day"),
        pl.lit(f"{anchor}_window_{before}d_before_{after}d_after").alias("search_window_reason"),
    )


def anchor_window(config: LifecycleEventSearchBatchConfig, anchor: str) -> tuple[int, int]:
    if anchor == "first":
        return config.first_lookback_days, config.first_lookahead_days
    return config.last_lookback_days, config.last_lookahead_days


def shifted_day(value: Any, days: int) -> str | None:
    text = str(value or "")
    if len(text) != 8 or not text.isdigit():
        return None
    parsed = datetime.strptime(text, "%Y%m%d").date()
    return (parsed + timedelta(days=days)).strftime("%Y%m%d")


def build_summary(
    config: LifecycleEventSearchBatchConfig,
    source: pl.DataFrame,
    limited: pl.DataFrame,
    batch: pl.DataFrame,
) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "operating lifecycle SEC search window batch",
        "input_path": str(config.input_path),
        "output_path": str(config.output_path),
        "input_row_count": source.height,
        "source_row_count": limited.height,
        "row_count": batch.height,
        "symbol_count": limited["symbol"].n_unique() if limited.height else 0,
        "first_window_days": [config.first_lookback_days, config.first_lookahead_days],
        "last_window_days": [config.last_lookback_days, config.last_lookahead_days],
        "limitations": [
            "Each source row emits first-day and last-day SEC search windows.",
            "Original era bounds are preserved as original_first_day/original_last_day.",
        ],
    }


def write_outputs(
    config: LifecycleEventSearchBatchConfig, batch: pl.DataFrame, summary: dict[str, Any]
) -> None:
    config.output_path.parent.mkdir(parents=True, exist_ok=True)
    config.summary_path.parent.mkdir(parents=True, exist_ok=True)
    batch.write_csv(config.output_path)
    config.summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
