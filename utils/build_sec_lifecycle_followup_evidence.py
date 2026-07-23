from __future__ import annotations

import argparse
import json
import os
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
    PASS_BUCKET,
    lifecycle_empty_result,
    lifecycle_text_result,
    parse_yyyymmdd,
)
from utils.sec_terminal_followup_sources import (
    SecRequestConfig,
    candidate_filings_for_target,
    cik_for_row,
)
from utils.verify_sec_override_candidates import VerifyConfig, fetch_text

DEFAULT_REPORT_DIR = DEFAULT_OUTPUT_ROOT / "sec-lifecycle-iterations"
DEFAULT_INPUT_PATH = DEFAULT_REPORT_DIR / "edgar" / "lifecycle_text_evidence_review.csv"
DEFAULT_OUTPUT_DIR = DEFAULT_REPORT_DIR / "edgar"
DEFAULT_INPUT_BUCKETS = ("missing_lifecycle_date_review", "date_mismatch_review",
    "first_day_lifecycle_review", "last_day_lifecycle_review", "reject_symbol_collision",
    "prospective_language_review")
CANDIDATE_FORMS = ("8-K", "6-K", "10-K", "10-Q", "20-F", "25-NSE", "15-12B", "425",
    "DEFM14A", "DEF 14A", "S-4", "S-4/A", "S-1", "S-1/A", "F-1", "F-1/A", "424B4")


@dataclass(frozen=True)
class LifecycleFollowupEvidenceConfig:
    input_path: Path
    output_dir: Path
    user_agent: str
    timeout_seconds: float
    sleep_seconds: float
    max_rows: int | None
    days_before: int
    days_after: int
    max_docs_per_row: int
    forms: tuple[str, ...]
    input_buckets: tuple[str, ...]


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir)
    setup_logging(str(output_dir / "lifecycle_followup_evidence.jsonl"))
    try:
        result = build_sec_lifecycle_followup_evidence(config_from_args(args, output_dir))
    except Exception as exc:
        get_logger(__name__).exception(
            "SEC lifecycle follow-up evidence failed",
            extra={"event": "lifecycle_followup_evidence_failed", "detail": {"error": repr(exc)}},
        )
        return 1
    get_logger(__name__).info(
        "SEC lifecycle follow-up evidence complete",
        extra={"event": "lifecycle_followup_evidence_complete", "detail": result["summary"]},
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", default=str(DEFAULT_INPUT_PATH))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--user-agent")
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--sleep-seconds", type=float, default=0.2)
    parser.add_argument("--max-rows", type=int)
    parser.add_argument("--days-before", type=int, default=90)
    parser.add_argument("--days-after", type=int, default=120)
    parser.add_argument("--max-docs-per-row", type=int, default=10)
    parser.add_argument("--forms", nargs="+", default=list(CANDIDATE_FORMS))
    parser.add_argument("--input-buckets", nargs="+", default=list(DEFAULT_INPUT_BUCKETS))
    return parser.parse_args()


def config_from_args(args: argparse.Namespace, output_dir: Path) -> LifecycleFollowupEvidenceConfig:
    return LifecycleFollowupEvidenceConfig(
        input_path=Path(args.input_path),
        output_dir=output_dir,
        user_agent=resolve_user_agent(args.user_agent),
        timeout_seconds=args.timeout_seconds,
        sleep_seconds=args.sleep_seconds,
        max_rows=args.max_rows,
        days_before=args.days_before,
        days_after=args.days_after,
        max_docs_per_row=args.max_docs_per_row,
        forms=tuple(args.forms),
        input_buckets=tuple(args.input_buckets),
    )


def resolve_user_agent(value: str | None) -> str:
    user_agent = value or os.getenv("SEC_USER_AGENT")
    if not user_agent:
        raise ValueError("provide --user-agent or set SEC_USER_AGENT before hitting SEC")
    return user_agent


def build_sec_lifecycle_followup_evidence(
    config: LifecycleFollowupEvidenceConfig,
) -> dict[str, Any]:
    validate_config(config)
    source = pl.read_csv(config.input_path, infer_schema_length=0)
    require_columns(source)
    rows = source.filter(pl.col("lifecycle_text_bucket").is_in(config.input_buckets))
    rows = rows.head(config.max_rows) if config.max_rows else rows
    reviewed = string_frame([doc for row in rows.to_dicts() for doc in review_row(row, config)])
    auto_ready = auto_ready_rows(reviewed)
    summary = build_summary(config, source.height, rows.height, reviewed, auto_ready)
    write_outputs(config, reviewed, auto_ready, summary)
    return {"summary": summary, "rows": reviewed.to_dicts()}


def validate_config(config: LifecycleFollowupEvidenceConfig) -> None:
    if not config.input_path.exists():
        raise FileNotFoundError(f"input file does not exist: {config.input_path}")
    if config.timeout_seconds <= 0:
        raise ValueError("--timeout-seconds must be positive")
    if config.sleep_seconds < 0:
        raise ValueError("--sleep-seconds cannot be negative")
    if min(config.days_before, config.days_after, config.max_docs_per_row) < 0:
        raise ValueError("window and document limits cannot be negative")
    if not config.forms:
        raise ValueError("--forms requires at least one form")


def require_columns(frame: pl.DataFrame) -> None:
    required = ["symbol", "symbol_era_id", "lifecycle_anchor", "original_first_day",
        "original_last_day", "lifecycle_text_bucket", "research_note"]
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"lifecycle review missing required columns: {missing}")


def review_row(row: dict[str, Any], config: LifecycleFollowupEvidenceConfig) -> list[dict[str, Any]]:
    cik = cik_for_row(row)
    if not cik:
        result = lifecycle_empty_result("missing_cik", 0, "missing CIK", "", None)
        return [{**row, **prefixed(result)}]
    try:
        filings = candidate_filings_for_target(
            cik,
            sec_request_config(config),
            forms=config.forms,
            days_before=config.days_before,
            days_after=config.days_after,
            max_docs=config.max_docs_per_row,
            target=anchor_date(row),
        )
    except Exception as exc:
        result = lifecycle_empty_result("submissions_fetch_error", 0, str(exc), repr(exc), None)
        return [{**row, "followup_cik": cik, **prefixed(result)}]
    if not filings:
        result = lifecycle_empty_result("no_candidate_filings", 0, "no SEC filings in window", "", None)
        return [{**row, "followup_cik": cik, **prefixed(result)}]
    return [review_filing(row, cik, filing, config) for filing in filings]


def review_filing(
    row: dict[str, Any], cik: str, filing: dict[str, str], config: LifecycleFollowupEvidenceConfig
) -> dict[str, Any]:
    try:
        text = fetch_text(filing["document_url"], sec_config(config))
        result = lifecycle_text_result(row, text, filing["document_url"])
    except Exception as exc:
        result = lifecycle_empty_result("fetch_error", 0, str(exc), repr(exc), filing["document_url"])
    time.sleep(config.sleep_seconds)
    return {
        **row,
        "followup_cik": cik,
        "followup_form": filing["form"],
        "followup_filed_at": filing["filing_date"],
        "followup_accession_no": filing["accession_no"],
        **prefixed(result),
    }


def anchor_date(row: dict[str, Any]) -> Any:
    key = "original_first_day" if row.get("lifecycle_anchor") == "first" else "original_last_day"
    return parse_yyyymmdd(row.get(key))


def sec_request_config(config: LifecycleFollowupEvidenceConfig) -> SecRequestConfig:
    return SecRequestConfig(
        user_agent=config.user_agent, timeout_seconds=config.timeout_seconds,
        sleep_seconds=config.sleep_seconds,
    )


def sec_config(config: LifecycleFollowupEvidenceConfig) -> VerifyConfig:
    return VerifyConfig(
        candidates_path=config.input_path,
        output_path=config.output_dir / "lifecycle_followup_evidence_review.csv",
        summary_path=config.output_dir / "lifecycle_followup_evidence_summary.json",
        user_agent=config.user_agent,
        timeout_seconds=config.timeout_seconds,
        sleep_seconds=config.sleep_seconds,
        max_rows=config.max_rows,
    )


def prefixed(result: dict[str, Any]) -> dict[str, Any]:
    return {f"followup_{key}": value for key, value in result.items()}


def auto_ready_rows(reviewed: pl.DataFrame) -> pl.DataFrame:
    if reviewed.is_empty() or "followup_lifecycle_text_bucket" not in reviewed.columns:
        return reviewed
    rows = reviewed.filter(pl.col("followup_lifecycle_text_bucket") == PASS_BUCKET)
    if rows.is_empty():
        return rows
    return (
        rows.sort(["symbol_era_id", "followup_lifecycle_text_score"], descending=[False, True])
        .unique("symbol_era_id", keep="first")
        .with_columns(
            pl.lit("verified").alias("research_status"),
            pl.col("followup_lifecycle_text_document_url").alias("primary_source_url"),
            pl.col("followup_lifecycle_text_date").alias("proposed_historical_event_date"),
            (
                pl.col("research_note")
                + pl.lit(" Lifecycle follow-up evidence: ")
                + pl.col("followup_lifecycle_text_reason")
            ).alias("research_note"),
        )
    )


def string_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    normalized = [{key: "" if value is None else str(value) for key, value in row.items()} for row in rows]
    return pl.DataFrame(normalized, infer_schema_length=None)


def build_summary(
    config: LifecycleFollowupEvidenceConfig,
    input_row_count: int,
    selected_row_count: int,
    reviewed: pl.DataFrame,
    auto_ready: pl.DataFrame,
) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "CIK-based SEC lifecycle follow-up evidence extraction",
        "input_path": str(config.input_path),
        "output_dir": str(config.output_dir),
        "input_row_count": input_row_count,
        "selected_row_count": selected_row_count,
        "document_row_count": reviewed.height,
        "auto_ready_count": auto_ready.height,
        "days_before": config.days_before,
        "days_after": config.days_after,
        "forms": list(config.forms),
        "input_buckets": list(config.input_buckets),
        "bucket_counts": count_by(reviewed, "followup_lifecycle_text_bucket"),
    }


def count_by(frame: pl.DataFrame, column: str) -> dict[str, int]:
    if frame.is_empty() or column not in frame.columns:
        return {}
    return {
        str(row[column]): row["len"] for row in frame.group_by(column).len().sort(column).to_dicts()
    }


def write_outputs(
    config: LifecycleFollowupEvidenceConfig,
    reviewed: pl.DataFrame,
    auto_ready: pl.DataFrame,
    summary: dict[str, Any],
) -> None:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    reviewed.write_csv(config.output_dir / "lifecycle_followup_evidence_review.csv")
    auto_ready.write_csv(config.output_dir / "lifecycle_followup_auto_verified.csv")
    (config.output_dir / "lifecycle_followup_evidence_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n"
    )


if __name__ == "__main__":
    raise SystemExit(main())
