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
from utils.build_strict_terminal_review import (
    days_filed_to_last,
    has_parenthetical_ticker_collision,
    split_flags,
)
from utils.dead_ticker_review_schema import DEFAULT_OUTPUT_ROOT

DEFAULT_INPUT_PATH = (
    DEFAULT_OUTPUT_ROOT
    / "edgar-operating-terminal-top250-terminal-window"
    / "strong_needs_close_review.csv"
)
DEFAULT_OUTPUT_PATH = (
    DEFAULT_OUTPUT_ROOT
    / "edgar-operating-terminal-top250-terminal-window"
    / "close_evidence_review.csv"
)
DEFAULT_AUTO_VERIFIED_PATH = (
    DEFAULT_OUTPUT_ROOT
    / "edgar-operating-terminal-top250-terminal-window"
    / "close_evidence_auto_verified.csv"
)
DEFAULT_SUMMARY_PATH = (
    DEFAULT_OUTPUT_ROOT
    / "edgar-operating-terminal-top250-terminal-window"
    / "close_evidence_review_summary.json"
)
DEFAULT_MAX_ABS_DAYS = 5
DEFAULT_FORMS = ("8-K", "425")
PASS_BUCKET = "close_evidence_ready"


@dataclass(frozen=True)
class CloseEvidenceReviewConfig:
    input_path: Path
    output_path: Path
    auto_verified_path: Path
    summary_path: Path
    max_abs_days: int
    forms: tuple[str, ...]


def main() -> int:
    args = parse_args()
    config = CloseEvidenceReviewConfig(
        input_path=Path(args.input_path),
        output_path=Path(args.output_path),
        auto_verified_path=Path(args.auto_verified_path),
        summary_path=Path(args.summary_path),
        max_abs_days=args.max_abs_days,
        forms=tuple(args.forms),
    )
    setup_logging(str(config.summary_path.with_suffix(".jsonl")))
    result = build_close_evidence_review(config)
    get_logger(__name__).info(
        "close evidence review complete",
        extra={"event": "close_evidence_review_complete", "detail": result["summary"]},
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", default=str(DEFAULT_INPUT_PATH))
    parser.add_argument("--output-path", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument("--auto-verified-path", default=str(DEFAULT_AUTO_VERIFIED_PATH))
    parser.add_argument("--summary-path", default=str(DEFAULT_SUMMARY_PATH))
    parser.add_argument("--max-abs-days", type=int, default=DEFAULT_MAX_ABS_DAYS)
    parser.add_argument("--forms", nargs="+", default=list(DEFAULT_FORMS))
    return parser.parse_args()


def build_close_evidence_review(config: CloseEvidenceReviewConfig) -> dict[str, Any]:
    validate_config(config)
    source = pl.read_csv(config.input_path, infer_schema_length=0)
    require_columns(source)
    reviewed = pl.DataFrame([review_row(row, config) for row in source.to_dicts()])
    auto_verified = auto_verified_rows(reviewed)
    summary = build_summary(config, reviewed, auto_verified)
    write_outputs(config, reviewed, auto_verified, summary)
    return {"summary": summary, "rows": reviewed.to_dicts()}


def validate_config(config: CloseEvidenceReviewConfig) -> None:
    if not config.input_path.exists():
        raise FileNotFoundError(f"close review input does not exist: {config.input_path}")
    if config.max_abs_days < 0:
        raise ValueError("--max-abs-days cannot be negative")
    if not config.forms:
        raise ValueError("--forms requires at least one form")


def require_columns(frame: pl.DataFrame) -> None:
    required = [
        "symbol",
        "symbol_era_id",
        "research_status",
        "verifier_bucket",
        "verifier_flags",
        "form",
        "filed_at",
        "entity",
        "research_note",
        "verifier_document_url",
    ]
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"close review input missing required columns: {missing}")


def review_row(row: dict[str, Any], config: CloseEvidenceReviewConfig) -> dict[str, Any]:
    flags = set(split_flags(row.get("verifier_flags")))
    filed_to_last = days_filed_to_last(row)
    collision = has_parenthetical_ticker_collision(row)
    bucket = close_evidence_bucket(row, flags, filed_to_last, collision, config)
    return {
        **row,
        "close_evidence_bucket": bucket,
        "close_evidence_reason": close_evidence_reason(
            row, flags, filed_to_last, collision, config
        ),
    }


def close_evidence_bucket(
    row: dict[str, Any],
    flags: set[str],
    filed_to_last: int | None,
    collision: bool,
    config: CloseEvidenceReviewConfig,
) -> str:
    if collision:
        return "reject_symbol_collision"
    if row.get("verifier_bucket") != "strong_review_candidate":
        return "not_strong_verifier"
    if normalize_form(row.get("form")) not in normalized_forms(config):
        return "unsupported_close_form"
    if filed_to_last is None or abs(filed_to_last) > config.max_abs_days:
        return "outside_close_window"
    if "issuer_name_match" not in flags or "event_language" not in flags:
        return "missing_issuer_or_event_evidence"
    if "completion_language" in flags or "delisting_language" in flags:
        return PASS_BUCKET
    return "missing_completion_or_delisting_evidence"


def close_evidence_reason(
    row: dict[str, Any],
    flags: set[str],
    filed_to_last: int | None,
    collision: bool,
    config: CloseEvidenceReviewConfig,
) -> str:
    parts = [
        f"form={row.get('form')}",
        f"verifier={row.get('verifier_bucket')}",
        f"flags={'+'.join(sorted(flags)) or 'none'}",
        f"filed_to_original_last_days={filed_to_last}",
        f"max_abs_days={config.max_abs_days}",
    ]
    if collision:
        parts.append("parenthetical_ticker_collision")
    return "; ".join(parts)


def auto_verified_rows(reviewed: pl.DataFrame) -> pl.DataFrame:
    rows = reviewed.filter(pl.col("close_evidence_bucket") == PASS_BUCKET)
    if rows.is_empty():
        return rows
    return rows.with_columns(
        pl.lit("verified").alias("research_status"),
        pl.col("verifier_document_url").alias("primary_source_url"),
        (
            pl.col("research_note")
            + pl.lit(" Close evidence review: ")
            + pl.col("close_evidence_reason")
        ).alias("research_note"),
    )


def build_summary(
    config: CloseEvidenceReviewConfig,
    reviewed: pl.DataFrame,
    auto_verified: pl.DataFrame,
) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "close-date evidence post-processing",
        "input_path": str(config.input_path),
        "output_path": str(config.output_path),
        "auto_verified_path": str(config.auto_verified_path),
        "row_count": reviewed.height,
        "auto_verified_count": auto_verified.height,
        "max_abs_days": config.max_abs_days,
        "forms": list(config.forms),
        "close_evidence_bucket_counts": count_by(reviewed, "close_evidence_bucket"),
        "limitations": [
            "Close-date evidence is still heuristic and requires SEC document text.",
            "This pass intentionally excludes proxy and registration forms.",
        ],
    }


def count_by(frame: pl.DataFrame, column: str) -> dict[str, int]:
    if frame.is_empty():
        return {}
    return {
        str(row[column]): row["len"] for row in frame.group_by(column).len().sort(column).to_dicts()
    }


def normalized_forms(config: CloseEvidenceReviewConfig) -> set[str]:
    return {normalize_form(form) for form in config.forms}


def normalize_form(value: Any) -> str:
    return str(value or "").strip().upper()


def write_outputs(
    config: CloseEvidenceReviewConfig,
    reviewed: pl.DataFrame,
    auto_verified: pl.DataFrame,
    summary: dict[str, Any],
) -> None:
    config.output_path.parent.mkdir(parents=True, exist_ok=True)
    reviewed.write_csv(config.output_path)
    auto_verified.write_csv(config.auto_verified_path)
    config.summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
