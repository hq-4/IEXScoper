from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import polars as pl

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.framework.logging import get_logger, setup_logging
from utils.dead_ticker_review_schema import DEFAULT_OUTPUT_ROOT

DEFAULT_PRIORITY_QUEUE_PATH = DEFAULT_OUTPUT_ROOT / "unresolved_priority_queue.parquet"
DEFAULT_OUTPUT_PATH = DEFAULT_OUTPUT_ROOT / "manual_resolution_template.csv"
DEFAULT_LIMIT = 100

RESEARCH_COLUMNS = [
    "research_status",
    "proposed_historical_identity_status",
    "proposed_historical_issuer_name",
    "proposed_historical_event_type",
    "proposed_historical_event_date",
    "proposed_historical_successor",
    "primary_source_url",
    "secondary_source_url",
    "research_note",
]


@dataclass(frozen=True)
class ResolutionTemplateConfig:
    priority_queue_path: Path
    output_path: Path
    limit: int


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--priority-queue-path", default=str(DEFAULT_PRIORITY_QUEUE_PATH))
    parser.add_argument("--output-path", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    args = parser.parse_args()
    config = ResolutionTemplateConfig(
        priority_queue_path=Path(args.priority_queue_path),
        output_path=Path(args.output_path),
        limit=args.limit,
    )
    setup_logging(str(config.output_path.parent / "dead_ticker_resolution_template.jsonl"))
    result = build_resolution_template(config)
    get_logger(__name__).info(
        "dead ticker resolution template complete",
        extra={"event": "dead_ticker_resolution_template_complete", "detail": result},
    )
    return 0


def build_resolution_template(config: ResolutionTemplateConfig) -> dict[str, int | str]:
    validate_config(config)
    priority = pl.read_parquet(config.priority_queue_path)
    template = resolution_template(priority, config.limit)
    config.output_path.parent.mkdir(parents=True, exist_ok=True)
    template.write_csv(config.output_path)
    return {
        "row_count": template.height,
        "priority_queue_path": str(config.priority_queue_path),
        "output_path": str(config.output_path),
    }


def validate_config(config: ResolutionTemplateConfig) -> None:
    if not config.priority_queue_path.exists():
        raise FileNotFoundError(f"priority queue does not exist: {config.priority_queue_path}")
    if config.limit <= 0:
        raise ValueError("--limit must be positive")


def resolution_template(priority: pl.DataFrame, limit: int) -> pl.DataFrame:
    require_columns(
        priority,
        [
            "priority_rank",
            "symbol",
            "symbol_era_id",
            "source_classification",
            "instrument_hint",
            "trade_rows",
            "first_day",
            "last_day",
        ],
    )
    return (
        priority.head(limit)
        .with_columns(
            pl.lit("todo").alias("research_status"),
            pl.lit(None, dtype=pl.String).alias("proposed_historical_identity_status"),
            pl.lit(None, dtype=pl.String).alias("proposed_historical_issuer_name"),
            pl.lit(None, dtype=pl.String).alias("proposed_historical_event_type"),
            pl.lit(None, dtype=pl.String).alias("proposed_historical_event_date"),
            pl.lit(None, dtype=pl.String).alias("proposed_historical_successor"),
            pl.lit(None, dtype=pl.String).alias("primary_source_url"),
            pl.lit(None, dtype=pl.String).alias("secondary_source_url"),
            pl.lit(None, dtype=pl.String).alias("research_note"),
        )
        .select(template_columns(priority.columns))
    )


def require_columns(frame: pl.DataFrame, columns: list[str]) -> None:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise ValueError(f"priority queue missing required columns: {missing}")


def template_columns(source_columns: list[str]) -> list[str]:
    carried = [
        "priority_rank",
        "symbol",
        "symbol_era_id",
        "source_classification",
        "instrument_hint",
        "is_probable_operating",
        "is_delisted_candidate",
        "first_day",
        "last_day",
        "observed_days",
        "trade_rows",
        "main_rows",
        "sec_current_confidence",
        "sec_cik",
        "sec_name",
        "iex_entity_confidence",
        "iex_latest_issuer",
    ]
    return [column for column in carried if column in source_columns] + RESEARCH_COLUMNS


if __name__ == "__main__":
    raise SystemExit(main())
