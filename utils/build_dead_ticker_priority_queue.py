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

DEFAULT_REVIEW_QUEUE_PATH = DEFAULT_OUTPUT_ROOT / "dead_ticker_review_queue.parquet"
UNRESOLVED_STATUS = "historical_identity_unresolved"
OPERATING_HINT = "probable_operating_or_other"
OPERATING_TYPE = "probable_operating_company"
DELISTED_CLASS = "delisted_or_acquired_candidate"
DEFAULT_TOP_N = 100


@dataclass(frozen=True)
class PriorityQueueConfig:
    review_queue_path: Path
    output_root: Path
    top_n: int


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--review-queue-path", default=str(DEFAULT_REVIEW_QUEUE_PATH))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N)
    args = parser.parse_args()
    config = PriorityQueueConfig(
        review_queue_path=Path(args.review_queue_path),
        output_root=Path(args.output_root),
        top_n=args.top_n,
    )
    setup_logging(str(config.output_root / "dead_ticker_priority_queue.jsonl"))
    result = build_priority_queue(config)
    get_logger(__name__).info(
        "dead ticker priority queue complete",
        extra={"event": "dead_ticker_priority_queue_complete", "detail": result["summary"]},
    )
    return 0


def build_priority_queue(config: PriorityQueueConfig) -> dict[str, Any]:
    validate_config(config)
    config.output_root.mkdir(parents=True, exist_ok=True)
    review = load_review_queue(config.review_queue_path)
    priority = prioritize_unresolved(review)
    summary = build_summary(config, priority)
    write_outputs(config.output_root, priority, summary, config.top_n)
    return {"summary": summary, "rows": priority.head(config.top_n).to_dicts()}


def validate_config(config: PriorityQueueConfig) -> None:
    if not config.review_queue_path.exists():
        raise FileNotFoundError(
            f"dead ticker review queue does not exist: {config.review_queue_path}"
        )
    if config.top_n <= 0:
        raise ValueError("--top-n must be positive")


def load_review_queue(path: Path) -> pl.DataFrame:
    required = [
        "symbol",
        "symbol_era_id",
        "source_classification",
        "first_day",
        "last_day",
        "observed_days",
        "trade_rows",
        "main_rows",
        "identity_evidence_status",
        "instrument_hint",
    ]
    frame = pl.read_parquet(path)
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"{path} missing required columns: {missing}")
    return frame


def prioritize_unresolved(frame: pl.DataFrame) -> pl.DataFrame:
    operating_expr = (
        pl.col("instrument_type") == OPERATING_TYPE
        if "instrument_type" in frame.columns
        else pl.col("instrument_hint") == OPERATING_HINT
    )
    return (
        frame.filter(pl.col("identity_evidence_status") == UNRESOLVED_STATUS)
        .with_columns(
            operating_expr.alias("is_probable_operating"),
            (pl.col("source_classification") == DELISTED_CLASS).alias("is_delisted_candidate"),
        )
        .with_row_index("unresolved_rank", offset=1)
        .sort(
            ["is_probable_operating", "is_delisted_candidate", "trade_rows", "symbol"],
            descending=[True, True, True, False],
        )
        .with_row_index("priority_rank", offset=1)
        .select(priority_columns(frame.columns))
    )


def priority_columns(source_columns: list[str]) -> list[str]:
    preferred = [
        "priority_rank",
        "symbol",
        "symbol_era_id",
        "source_classification",
        "instrument_hint",
        "instrument_type",
        "instrument_reason",
        "research_route",
        "recommended_evidence",
        "routing_reason",
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
        "iex_product_hint",
        "identity_evidence_status",
    ]
    generated = {"priority_rank", "is_probable_operating", "is_delisted_candidate"}
    return [column for column in preferred if column in source_columns or column in generated]


def build_summary(config: PriorityQueueConfig, priority: pl.DataFrame) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "prioritized unresolved historical ticker identity review queue",
        "review_queue_path": str(config.review_queue_path),
        "unresolved_era_count": priority.height,
        "top_n": min(config.top_n, priority.height),
        "probable_operating_count": int(priority["is_probable_operating"].sum()),
        "delisted_candidate_count": int(priority["is_delisted_candidate"].sum()),
        "top_classification_counts": count_by(priority.head(config.top_n), "source_classification"),
        "top_instrument_hint_counts": count_by(priority.head(config.top_n), "instrument_hint"),
        "top_instrument_type_counts": count_by_if_present(
            priority.head(config.top_n), "instrument_type"
        ),
        "top_research_route_counts": count_by_if_present(
            priority.head(config.top_n), "research_route"
        ),
        "sort_order": [
            "is_probable_operating descending",
            "is_delisted_candidate descending",
            "trade_rows descending",
            "symbol ascending",
        ],
    }


def count_by(frame: pl.DataFrame, column: str) -> dict[str, int]:
    return {
        str(row[column]): row["len"] for row in frame.group_by(column).len().sort(column).to_dicts()
    }


def count_by_if_present(frame: pl.DataFrame, column: str) -> dict[str, int]:
    if column not in frame.columns:
        return {}
    return count_by(frame, column)


def write_outputs(root: Path, priority: pl.DataFrame, summary: dict[str, Any], top_n: int) -> None:
    priority.write_parquet(root / "unresolved_priority_queue.parquet", compression="zstd")
    priority.write_csv(root / "unresolved_priority_queue.csv")
    priority.head(top_n).write_csv(root / "unresolved_priority_top.csv")
    (root / "unresolved_priority_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    write_markdown(root / "unresolved_priority_report.md", priority.head(top_n), summary)


def write_markdown(path: Path, top_rows: pl.DataFrame, summary: dict[str, Any]) -> None:
    lines = [
        "# Unresolved Dead Ticker Priority Queue",
        "",
        "This report ranks unresolved historical ticker-era identity targets for manual review.",
        "",
        f"- Unresolved eras: `{summary['unresolved_era_count']}`",
        f"- Top rows shown: `{summary['top_n']}`",
        f"- Probable operating rows: `{summary['probable_operating_count']}`",
        f"- Delisted/acquired candidate rows: `{summary['delisted_candidate_count']}`",
        "",
        "## Sort Order",
        "",
    ]
    lines.extend(f"- `{item}`" for item in summary["sort_order"])
    lines.extend(["", "## Top Review Targets", ""])
    type_column = "instrument_type" if "instrument_type" in top_rows.columns else "instrument_hint"
    route_column = "research_route" if "research_route" in top_rows.columns else "instrument_hint"
    lines.append("| Rank | Symbol | Era | Class | Hint | Type | Route | Trades | First | Last |")
    lines.append("|---:|---|---|---|---|---|---|---:|---|---|")
    for row in top_rows.to_dicts():
        row["display_instrument_type"] = row.get(type_column, "")
        row["display_research_route"] = row.get(route_column, "")
        lines.append(
            "| {priority_rank} | {symbol} | {symbol_era_id} | {source_classification} | "
            "{instrument_hint} | {display_instrument_type} | {display_research_route} | "
            "{trade_rows} | {first_day} | {last_day} |".format(**row)
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
