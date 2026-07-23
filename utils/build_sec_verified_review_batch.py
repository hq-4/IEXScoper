from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.framework.logging import get_logger, setup_logging
from utils.dead_ticker_review_schema import DEFAULT_OUTPUT_ROOT

DEFAULT_VERIFIER_PATH = DEFAULT_OUTPUT_ROOT / "sec_override_candidates_verified_triage.csv"
DEFAULT_OUTPUT_PATH = DEFAULT_OUTPUT_ROOT / "sec_strong_review_batch.csv"
DEFAULT_LOG_PATH = DEFAULT_OUTPUT_ROOT / "sec_strong_review_batch.jsonl"
DEFAULT_MIN_BUCKET = "strong_review_candidate"
BUCKET_ORDER = {
    "strong_review_candidate": 0,
    "moderate_review_candidate": 1,
    "weak_review_candidate": 2,
    "fetch_error": 3,
}
REQUIRED_COLUMNS = [
    "symbol",
    "symbol_era_id",
    "research_status",
    "verifier_bucket",
    "verifier_score",
    "verifier_flags",
    "verifier_document_url",
]


@dataclass(frozen=True)
class ReviewBatchConfig:
    verifier_path: Path
    output_path: Path
    min_bucket: str
    max_rows: int | None


def main() -> int:
    args = parse_args()
    config = ReviewBatchConfig(
        verifier_path=Path(args.verifier_path),
        output_path=Path(args.output_path),
        min_bucket=args.min_bucket,
        max_rows=args.max_rows,
    )
    setup_logging(str(DEFAULT_LOG_PATH))
    result = build_review_batch(config)
    get_logger(__name__).info(
        "SEC verified review batch complete",
        extra={"event": "sec_verified_review_batch_complete", "detail": result},
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--verifier-path", default=str(DEFAULT_VERIFIER_PATH))
    parser.add_argument("--output-path", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument(
        "--min-bucket",
        choices=("strong_review_candidate", "moderate_review_candidate", "weak_review_candidate"),
        default=DEFAULT_MIN_BUCKET,
    )
    parser.add_argument("--max-rows", type=int)
    return parser.parse_args()


def build_review_batch(config: ReviewBatchConfig) -> dict[str, Any]:
    validate_config(config)
    verifier = pl.read_csv(config.verifier_path, infer_schema_length=0)
    missing = [column for column in REQUIRED_COLUMNS if column not in verifier.columns]
    if missing:
        raise ValueError(f"verifier output missing required columns: {missing}")
    batch = review_batch(verifier, config)
    config.output_path.parent.mkdir(parents=True, exist_ok=True)
    batch.write_csv(config.output_path)
    return {
        "verifier_path": str(config.verifier_path),
        "output_path": str(config.output_path),
        "min_bucket": config.min_bucket,
        "row_count": batch.height,
        "bucket_counts": count_by(batch, "verifier_bucket"),
    }


def validate_config(config: ReviewBatchConfig) -> None:
    if not config.verifier_path.exists():
        raise FileNotFoundError(f"verifier output does not exist: {config.verifier_path}")
    if config.max_rows is not None and config.max_rows <= 0:
        raise ValueError("--max-rows must be positive")


def review_batch(verifier: pl.DataFrame, config: ReviewBatchConfig) -> pl.DataFrame:
    allowed = allowed_buckets(config.min_bucket)
    batch = (
        verifier.filter(pl.col("verifier_bucket").is_in(allowed))
        .with_columns(
            pl.col("verifier_score").cast(pl.Int64, strict=False).alias("_score"),
            pl.col("priority_rank").cast(pl.Int64, strict=False).alias("_priority"),
        )
        .sort(["_score", "_priority"], descending=[True, False])
        .drop(["_score", "_priority"])
    )
    return batch.head(config.max_rows) if config.max_rows else batch


def allowed_buckets(min_bucket: str) -> list[str]:
    rank = BUCKET_ORDER[min_bucket]
    return [bucket for bucket, bucket_rank in BUCKET_ORDER.items() if bucket_rank <= rank]


def count_by(frame: pl.DataFrame, column: str) -> dict[str, int]:
    if frame.height == 0:
        return {}
    return {
        str(row[column]): row["len"] for row in frame.group_by(column).len().sort(column).to_dicts()
    }


if __name__ == "__main__":
    raise SystemExit(main())
