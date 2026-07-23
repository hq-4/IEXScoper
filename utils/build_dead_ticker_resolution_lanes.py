from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.framework.logging import get_logger, setup_logging
from utils.dead_ticker_review_schema import DEFAULT_OUTPUT_ROOT
from utils.resolution_ledger import DEFAULT_RESOLUTION_OUTPUT_ROOT

DEFAULT_PRIORITY_QUEUE_PATH = DEFAULT_OUTPUT_ROOT / "unresolved_priority_queue.parquet"
OPERATING_ROUTE = "operating_company_sec_event"
PREFERRED_ROUTE = "preferred_redemption_or_delisting"
SECURITY_ACTION_ROUTE = "warrant_unit_right_security_action"
SHARE_CLASS_ROUTE = "share_class_corporate_action"
SPARSE_TRADE_ROW_LIMIT = 1_000


@dataclass(frozen=True)
class ResolutionLaneConfig:
    priority_queue_path: Path
    output_root: Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--priority-queue-path", default=str(DEFAULT_PRIORITY_QUEUE_PATH))
    parser.add_argument("--output-root", default=str(DEFAULT_RESOLUTION_OUTPUT_ROOT))
    args = parser.parse_args()
    config = ResolutionLaneConfig(
        priority_queue_path=Path(args.priority_queue_path),
        output_root=Path(args.output_root),
    )
    setup_logging(str(config.output_root / "resolution_lanes.jsonl"))
    result = build_resolution_lanes(config)
    get_logger(__name__).info(
        "dead ticker resolution lanes complete",
        extra={"event": "dead_ticker_resolution_lanes_complete", "detail": result["summary"]},
    )
    return 0


def build_resolution_lanes(config: ResolutionLaneConfig) -> dict[str, Any]:
    validate_config(config)
    config.output_root.mkdir(parents=True, exist_ok=True)
    priority = pl.read_parquet(config.priority_queue_path)
    require_columns(priority)
    lanes = priority.with_columns(
        resolution_lane_expr().alias("resolution_lane"),
        trade_tier_expr().alias("trade_tier"),
        pl.col("last_day").str.slice(0, 4).alias("last_year"),
    )
    summary = build_summary(config, lanes)
    write_outputs(config.output_root, lanes, summary)
    return {"summary": summary, "rows": lanes.to_dicts()}


def validate_config(config: ResolutionLaneConfig) -> None:
    if not config.priority_queue_path.exists():
        raise FileNotFoundError(f"priority queue does not exist: {config.priority_queue_path}")


def require_columns(frame: pl.DataFrame) -> None:
    required = [
        "symbol",
        "symbol_era_id",
        "source_classification",
        "research_route",
        "instrument_type",
        "trade_rows",
        "last_day",
    ]
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"priority queue missing required columns: {missing}")


def resolution_lane_expr() -> pl.Expr:
    return (
        pl.when(
            (pl.col("research_route") == OPERATING_ROUTE)
            & (pl.col("source_classification") == "delisted_or_acquired_candidate")
        )
        .then(pl.lit("operating_terminal_event"))
        .when(
            (pl.col("research_route") == OPERATING_ROUTE)
            & (pl.col("source_classification") == "partial_window_candidate")
        )
        .then(pl.lit("operating_partial_window"))
        .when(
            (pl.col("research_route") == OPERATING_ROUTE)
            & (pl.col("trade_rows") < SPARSE_TRADE_ROW_LIMIT)
        )
        .then(pl.lit("operating_sparse_intermit"))
        .when(pl.col("research_route") == OPERATING_ROUTE)
        .then(pl.lit("operating_intermit_material"))
        .when(pl.col("research_route") == PREFERRED_ROUTE)
        .then(pl.lit("preferred_parent_or_redemption"))
        .when(pl.col("research_route") == SECURITY_ACTION_ROUTE)
        .then(pl.lit("warrant_unit_right_action"))
        .when(pl.col("research_route") == SHARE_CLASS_ROUTE)
        .then(pl.lit("share_class_action"))
        .otherwise(pl.lit("manual_syntax"))
    )


def trade_tier_expr() -> pl.Expr:
    return (
        pl.when(pl.col("trade_rows") >= 1_000_000)
        .then(pl.lit(">=1M"))
        .when(pl.col("trade_rows") >= 100_000)
        .then(pl.lit("100K-999K"))
        .when(pl.col("trade_rows") >= 10_000)
        .then(pl.lit("10K-99K"))
        .when(pl.col("trade_rows") >= 1_000)
        .then(pl.lit("1K-9K"))
        .when(pl.col("trade_rows") >= 100)
        .then(pl.lit("100-999"))
        .when(pl.col("trade_rows") >= 1)
        .then(pl.lit("1-99"))
        .otherwise(pl.lit("0"))
    )


def build_summary(config: ResolutionLaneConfig, lanes: pl.DataFrame) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "resolution lane profile for unresolved ticker eras",
        "priority_queue_path": str(config.priority_queue_path),
        "row_count": lanes.height,
        "unique_symbol_count": lanes["symbol"].n_unique(),
        "lane_counts": count_by(lanes, "resolution_lane"),
        "research_route_counts": count_by(lanes, "research_route"),
        "instrument_type_counts": count_by(lanes, "instrument_type"),
        "source_classification_counts": count_by(lanes, "source_classification"),
        "trade_tier_counts": count_by(lanes, "trade_tier"),
        "last_year_counts": count_by(lanes, "last_year"),
        "limitations": [
            "Lanes are routing work queues, not proof of issuer identity.",
            "Sparse/intermittent rows still need deterministic disposition criteria before closure.",
        ],
    }


def count_by(frame: pl.DataFrame, column: str) -> dict[str, int]:
    return {
        str(row[column]): row["len"] for row in frame.group_by(column).len().sort(column).to_dicts()
    }


def write_outputs(root: Path, lanes: pl.DataFrame, summary: dict[str, Any]) -> None:
    lanes.write_parquet(root / "resolution_lanes.parquet", compression="zstd")
    lanes.write_csv(root / "resolution_lanes.csv")
    write_lane_files(root, lanes)
    write_profile(root, lanes)
    (root / "resolution_lanes_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    write_markdown(root / "resolution_lanes_report.md", summary)


def write_lane_files(root: Path, lanes: pl.DataFrame) -> None:
    for lane in sorted(lanes["resolution_lane"].unique().to_list()):
        lane_frame = lanes.filter(pl.col("resolution_lane") == lane).sort("priority_rank")
        lane_frame.write_csv(root / f"{lane}.csv")


def write_profile(root: Path, lanes: pl.DataFrame) -> None:
    profile = (
        lanes.group_by(["resolution_lane", "research_route", "instrument_type", "trade_tier"])
        .agg(
            pl.len().alias("eras"),
            pl.col("symbol").n_unique().alias("symbols"),
            pl.col("trade_rows").sum().alias("trade_rows"),
        )
        .sort(["resolution_lane", "eras"], descending=[False, True])
    )
    profile.write_csv(root / "backlog_profile.csv")


def write_markdown(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# Dead Ticker Resolution Lanes",
        "",
        f"- Remaining eras: `{summary['row_count']}`",
        f"- Unique symbols: `{summary['unique_symbol_count']}`",
        "",
        "## Lanes",
        "",
    ]
    lines.extend(f"- `{key}`: `{value}`" for key, value in summary["lane_counts"].items())
    lines.extend(["", "## Trade Tiers", ""])
    lines.extend(f"- `{key}`: `{value}`" for key, value in summary["trade_tier_counts"].items())
    lines.extend(["", "## Limitations", ""])
    lines.extend(f"- {item}" for item in summary["limitations"])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
