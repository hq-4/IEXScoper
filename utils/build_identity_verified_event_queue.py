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
from utils.resolution_v2_schema import DEFAULT_FACT_ROOT, DEFAULT_REPORT_ROOT, VERIFIED

DEFAULT_EVENT_GAP_PATH = DEFAULT_REPORT_ROOT / "event_gap_queue.csv"
DEFAULT_IDENTITY_FACTS_PATH = DEFAULT_FACT_ROOT / "identity_facts.jsonl"
DEFAULT_REVIEW_PATH = DEFAULT_OUTPUT_ROOT / "dead_ticker_review_queue.csv"
DEFAULT_OUTPUT_PATH = DEFAULT_REPORT_ROOT / "identity_verified_event_queue.csv"
DEFAULT_SUMMARY_PATH = DEFAULT_REPORT_ROOT / "identity_verified_event_queue_summary.json"
ACTION_REQUIRED = "action_required"
REQUIRED_GAP_COLUMNS = (
    "symbol",
    "symbol_era_id",
    "trade_rows",
    "identity_status",
    "event_status",
    "research_status",
    "next_resolver",
)
REQUIRED_REVIEW_COLUMNS = ("symbol", "symbol_era_id", "first_day", "last_day")


@dataclass(frozen=True)
class EventQueueConfig:
    event_gap_path: Path
    identity_facts_path: Path
    review_path: Path
    output_path: Path
    summary_path: Path
    top_n: int


def main() -> int:
    args = parse_args()
    config = EventQueueConfig(
        event_gap_path=Path(args.event_gap_path),
        identity_facts_path=Path(args.identity_facts_path),
        review_path=Path(args.review_path),
        output_path=Path(args.output_path),
        summary_path=Path(args.summary_path),
        top_n=args.top_n,
    )
    setup_logging(str(config.summary_path.with_suffix(".jsonl")))
    result = build_event_queue(config)
    get_logger(__name__).info(
        "identity-verified event queue complete",
        extra={"event": "identity_verified_event_queue_complete", "detail": result["summary"]},
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the V2 identity-verified, event-unproven review queue."
    )
    parser.add_argument("--event-gap-path", default=str(DEFAULT_EVENT_GAP_PATH))
    parser.add_argument("--identity-facts-path", default=str(DEFAULT_IDENTITY_FACTS_PATH))
    parser.add_argument("--review-path", default=str(DEFAULT_REVIEW_PATH))
    parser.add_argument("--output-path", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument("--summary-path", default=str(DEFAULT_SUMMARY_PATH))
    parser.add_argument("--top-n", type=int, default=200)
    return parser.parse_args()


def build_event_queue(config: EventQueueConfig) -> dict[str, Any]:
    validate_config(config)
    gap = read_csv(config.event_gap_path, REQUIRED_GAP_COLUMNS)
    identities = read_identity_facts(config.identity_facts_path)
    review = read_csv(config.review_path, REQUIRED_REVIEW_COLUMNS)
    queue = event_queue_rows(gap, identities, review)
    summary = build_summary(config, queue)
    write_outputs(config, queue, summary)
    return {"summary": summary, "rows": queue.head(config.top_n).to_dicts()}


def validate_config(config: EventQueueConfig) -> None:
    for path in (config.event_gap_path, config.identity_facts_path, config.review_path):
        if not path.exists():
            raise FileNotFoundError(f"queue input does not exist: {path}")
    if config.top_n <= 0:
        raise ValueError("--top-n must be positive")


def read_csv(path: Path, required: tuple[str, ...]) -> pl.DataFrame:
    frame = pl.read_csv(path, infer_schema_length=0)
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"{path} missing required columns: {missing}")
    duplicates = frame.filter(pl.col("symbol_era_id").is_duplicated())
    if duplicates.height:
        eras = duplicates["symbol_era_id"].unique().sort().head(10).to_list()
        raise ValueError(f"{path} has duplicate symbol_era_id values: {eras}")
    return frame


def read_identity_facts(path: Path) -> pl.DataFrame:
    records = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    latest = {
        row["symbol_era_id"]: row
        for row in sorted(
            records,
            key=lambda row: (row.get("created_at", ""), row.get("fact_id", "")),
        )
    }
    rows = [
        {
            "symbol_era_id": row["symbol_era_id"],
            "identity_symbol": row["symbol"],
            "verified_identity_entity_id": row.get("entity_id", ""),
            "verified_identity_issuer": row.get("issuer", ""),
            "verified_identity_source": row.get("source", ""),
            "verified_identity_evidence_method": row.get("evidence_method", ""),
            "verified_identity_fact_id": row.get("fact_id", ""),
        }
        for row in latest.values()
        if row.get("verification_state") == VERIFIED
    ]
    return pl.DataFrame(rows)


def event_queue_rows(
    gap: pl.DataFrame, identities: pl.DataFrame, review: pl.DataFrame
) -> pl.DataFrame:
    selected = gap.filter(
        (pl.col("identity_status") == VERIFIED)
        & (pl.col("event_status") != VERIFIED)
        & (pl.col("research_status") == ACTION_REQUIRED)
    )
    validate_join(selected, identities, review)
    review_columns = [column for column in review.columns if column not in {"symbol", "trade_rows"}]
    return (
        selected.join(identities.drop("identity_symbol"), on="symbol_era_id", how="left")
        .join(review.select(review_columns), on="symbol_era_id", how="left")
        .with_columns(pl.col("trade_rows").cast(pl.Int64, strict=False).fill_null(0))
        .sort(["trade_rows", "symbol_era_id"], descending=[True, False])
        .with_row_index("priority_rank", offset=1)
    )


def validate_join(selected: pl.DataFrame, identities: pl.DataFrame, review: pl.DataFrame) -> None:
    validate_identity_join(selected, identities)
    validate_review_join(selected, review)


def validate_identity_join(selected: pl.DataFrame, identities: pl.DataFrame) -> None:
    mappings = selected.select("symbol", "symbol_era_id").join(
        identities.select("symbol_era_id", "identity_symbol"),
        on="symbol_era_id",
        how="left",
    )
    missing = mappings.filter(pl.col("identity_symbol").is_null())
    if missing.height:
        eras = missing["symbol_era_id"].to_list()[:10]
        raise ValueError(f"verified event-gap eras missing identity facts: {eras}")
    mismatched = mappings.filter(pl.col("symbol") != pl.col("identity_symbol"))
    if mismatched.height:
        raise ValueError(f"event gap/identity symbol mismatch: {mismatched.to_dicts()[:10]}")


def validate_review_join(selected: pl.DataFrame, review: pl.DataFrame) -> None:
    mappings = selected.select("symbol", "symbol_era_id").join(
        review.select(
            pl.col("symbol").alias("review_symbol"),
            pl.col("symbol_era_id"),
        ),
        on="symbol_era_id",
        how="left",
    )
    missing = mappings.filter(pl.col("review_symbol").is_null())
    if missing.height:
        eras = missing["symbol_era_id"].to_list()[:10]
        raise ValueError(f"event gap eras missing from review queue: {eras}")
    mismatched = mappings.filter(pl.col("symbol") != pl.col("review_symbol"))
    if mismatched.height:
        details = mismatched.select("symbol_era_id", "symbol", "review_symbol").to_dicts()
        raise ValueError(f"event gap/review symbol mismatch: {details[:10]}")


def build_summary(config: EventQueueConfig, queue: pl.DataFrame) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "V2 identity-verified event-gap queue joined to era review metadata",
        "event_gap_path": str(config.event_gap_path),
        "identity_facts_path": str(config.identity_facts_path),
        "review_path": str(config.review_path),
        "output_path": str(config.output_path),
        "row_count": queue.height,
        "trade_rows": int(queue["trade_rows"].sum()),
        "top_n": min(config.top_n, queue.height),
        "top_n_trade_rows": int(queue.head(config.top_n)["trade_rows"].sum()),
        "event_status_counts": count_by(queue, "event_status"),
        "next_resolver_counts": count_by(queue, "next_resolver"),
    }


def count_by(frame: pl.DataFrame, column: str) -> dict[str, int]:
    if not frame.height:
        return {}
    return {
        str(row[column]): int(row["len"])
        for row in frame.group_by(column).len().sort(column).to_dicts()
    }


def write_outputs(config: EventQueueConfig, queue: pl.DataFrame, summary: dict[str, Any]) -> None:
    config.output_path.parent.mkdir(parents=True, exist_ok=True)
    config.summary_path.parent.mkdir(parents=True, exist_ok=True)
    queue.write_csv(config.output_path)
    top_path = config.output_path.with_name(f"{config.output_path.stem}_top.csv")
    queue.head(config.top_n).write_csv(top_path)
    config.summary_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    raise SystemExit(main())
