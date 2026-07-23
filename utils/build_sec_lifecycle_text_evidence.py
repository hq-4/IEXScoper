from __future__ import annotations

import argparse
import json
import time
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
from utils.sec_lifecycle_text_evidence import (
    MAX_DATE_DISTANCE_DAYS,
    PASS_BUCKET,
    lifecycle_empty_result,
    lifecycle_text_result,
)
from utils.verify_sec_override_candidates import (
    VerifyConfig,
    fetch_text,
    resolve_document_url,
    resolve_user_agent,
)

DEFAULT_REPORT_DIR = DEFAULT_OUTPUT_ROOT / "sec-lifecycle-iterations"
DEFAULT_VERIFIER_PATH = DEFAULT_REPORT_DIR / "edgar" / "lifecycle_candidates_verified_triage.csv"
DEFAULT_OUTPUT_DIR = DEFAULT_REPORT_DIR / "edgar"
DEFAULT_INPUT_BUCKETS = (
    "strong_review_candidate",
    "moderate_review_candidate",
    "weak_review_candidate",
)


@dataclass(frozen=True)
class LifecycleTextEvidenceConfig:
    verifier_path: Path
    output_dir: Path
    user_agent: str
    timeout_seconds: float
    sleep_seconds: float
    max_rows: int | None
    input_buckets: tuple[str, ...]
    max_date_distance_days: int = MAX_DATE_DISTANCE_DAYS


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir)
    setup_logging(str(output_dir / "lifecycle_text_evidence.jsonl"))
    try:
        result = build_sec_lifecycle_text_evidence(config_from_args(args, output_dir))
    except Exception as exc:
        get_logger(__name__).exception(
            "SEC lifecycle text evidence build failed",
            extra={"event": "lifecycle_text_evidence_failed", "detail": {"error": repr(exc)}},
        )
        return 1
    get_logger(__name__).info(
        "SEC lifecycle text evidence build complete",
        extra={"event": "lifecycle_text_evidence_complete", "detail": result["summary"]},
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--verifier-path", default=str(DEFAULT_VERIFIER_PATH))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--user-agent")
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--sleep-seconds", type=float, default=0.2)
    parser.add_argument("--max-rows", type=int)
    parser.add_argument("--input-buckets", nargs="+", default=list(DEFAULT_INPUT_BUCKETS))
    return parser.parse_args()


def config_from_args(args: argparse.Namespace, output_dir: Path) -> LifecycleTextEvidenceConfig:
    return LifecycleTextEvidenceConfig(
        verifier_path=Path(args.verifier_path),
        output_dir=output_dir,
        user_agent=resolve_user_agent(args.user_agent),
        timeout_seconds=args.timeout_seconds,
        sleep_seconds=args.sleep_seconds,
        max_rows=args.max_rows,
        input_buckets=tuple(args.input_buckets),
    )


def build_sec_lifecycle_text_evidence(config: LifecycleTextEvidenceConfig) -> dict[str, Any]:
    validate_config(config)
    source = pl.read_csv(config.verifier_path, infer_schema_length=0)
    require_columns(source)
    rows = source.filter(pl.col("verifier_bucket").is_in(config.input_buckets))
    rows = rows.head(config.max_rows) if config.max_rows else rows
    reviewed = empty_review_frame(source) if rows.is_empty() else review_rows(rows, config)
    auto_ready = auto_ready_rows(reviewed)
    summary = build_summary(config, reviewed, auto_ready)
    write_outputs(config, reviewed, auto_ready, summary)
    return {"summary": summary, "rows": reviewed.to_dicts()}


def validate_config(config: LifecycleTextEvidenceConfig) -> None:
    if not config.verifier_path.exists():
        raise FileNotFoundError(f"verifier output does not exist: {config.verifier_path}")
    if config.timeout_seconds <= 0:
        raise ValueError("--timeout-seconds must be positive")
    if config.sleep_seconds < 0:
        raise ValueError("--sleep-seconds cannot be negative")
    if config.max_rows is not None and config.max_rows <= 0:
        raise ValueError("--max-rows must be positive")


def require_columns(frame: pl.DataFrame) -> None:
    required = [
        "symbol",
        "symbol_era_id",
        "lifecycle_anchor",
        "original_first_day",
        "original_last_day",
        "proposed_historical_issuer_name",
        "primary_source_url",
        "verifier_bucket",
        "entity",
        "research_note",
    ]
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"verifier output missing required columns: {missing}")


def review_rows(rows: pl.DataFrame, config: LifecycleTextEvidenceConfig) -> pl.DataFrame:
    return pl.DataFrame([review_row(row, config) for row in rows.to_dicts()])


def review_row(row: dict[str, Any], config: LifecycleTextEvidenceConfig) -> dict[str, Any]:
    document_url = None
    try:
        document_url = document_url_for(row, config)
        text = fetch_text(document_url, sec_config(config))
        result = lifecycle_text_result(row, text, document_url, config.max_date_distance_days)
    except Exception as exc:
        result = lifecycle_empty_result("fetch_error", 0, str(exc), repr(exc), document_url)
    time.sleep(config.sleep_seconds)
    return {**row, **result}


def document_url_for(row: dict[str, Any], config: LifecycleTextEvidenceConfig) -> str:
    direct = str(row.get("verifier_document_url") or "").strip()
    if direct:
        return direct
    return resolve_document_url(str(row.get("primary_source_url") or ""), sec_config(config))


def sec_config(config: LifecycleTextEvidenceConfig) -> VerifyConfig:
    return VerifyConfig(
        candidates_path=config.verifier_path,
        output_path=config.output_dir / "lifecycle_text_evidence_review.csv",
        summary_path=config.output_dir / "lifecycle_text_evidence_summary.json",
        user_agent=config.user_agent,
        timeout_seconds=config.timeout_seconds,
        sleep_seconds=config.sleep_seconds,
        max_rows=config.max_rows,
    )


def empty_review_frame(source: pl.DataFrame) -> pl.DataFrame:
    return source.head(0).with_columns(
        pl.lit(None).alias("lifecycle_text_bucket"),
        pl.lit(None).alias("lifecycle_text_score"),
        pl.lit(None).alias("lifecycle_text_reason"),
        pl.lit(None).alias("lifecycle_text_snippet"),
        pl.lit(None).alias("lifecycle_text_date"),
        pl.lit(None).alias("days_lifecycle_text_to_anchor"),
        pl.lit(None).alias("lifecycle_text_flags"),
        pl.lit(None).alias("lifecycle_text_document_url"),
    )


def auto_ready_rows(reviewed: pl.DataFrame) -> pl.DataFrame:
    if reviewed.is_empty():
        return reviewed
    rows = reviewed.filter(pl.col("lifecycle_text_bucket") == PASS_BUCKET)
    if rows.is_empty():
        return rows
    return (
        rows.sort(["symbol_era_id", "lifecycle_text_score"], descending=[False, True])
        .unique("symbol_era_id", keep="first")
        .with_columns(
            pl.lit("verified").alias("research_status"),
            pl.col("lifecycle_text_document_url").alias("primary_source_url"),
            pl.col("lifecycle_text_date").alias("proposed_historical_event_date"),
            (
                pl.col("research_note")
                + pl.lit(" Lifecycle text evidence: ")
                + pl.col("lifecycle_text_reason")
            ).alias("research_note"),
        )
    )


def build_summary(
    config: LifecycleTextEvidenceConfig,
    reviewed: pl.DataFrame,
    auto_ready: pl.DataFrame,
) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "SEC operating lifecycle text evidence extraction",
        "verifier_path": str(config.verifier_path),
        "output_dir": str(config.output_dir),
        "row_count": reviewed.height,
        "auto_ready_count": auto_ready.height,
        "input_buckets": list(config.input_buckets),
        "max_date_distance_days": config.max_date_distance_days,
        "bucket_counts": count_by(reviewed, "lifecycle_text_bucket"),
        "limitations": [
            "Only short snippets are persisted; full SEC filing text is not stored.",
            "Auto-ready rows are candidate feeds and are imported only by the runner flag.",
        ],
    }


def count_by(frame: pl.DataFrame, column: str) -> dict[str, int]:
    if frame.is_empty():
        return {}
    return {
        str(row[column]): row["len"] for row in frame.group_by(column).len().sort(column).to_dicts()
    }


def write_outputs(
    config: LifecycleTextEvidenceConfig,
    reviewed: pl.DataFrame,
    auto_ready: pl.DataFrame,
    summary: dict[str, Any],
) -> None:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    reviewed.write_csv(config.output_dir / "lifecycle_text_evidence_review.csv")
    auto_ready.write_csv(config.output_dir / "lifecycle_text_auto_verified.csv")
    (config.output_dir / "lifecycle_text_evidence_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n"
    )


if __name__ == "__main__":
    raise SystemExit(main())
