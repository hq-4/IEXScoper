from __future__ import annotations

import argparse
import json
import re
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

DEFAULT_VERIFIER_PATH = (
    DEFAULT_OUTPUT_ROOT
    / "edgar-operating-terminal-top250-terminal-window-suspects"
    / "sec_override_candidates_verified_triage.csv"
)
DEFAULT_OUTPUT_PATH = (
    DEFAULT_OUTPUT_ROOT
    / "edgar-operating-terminal-top250-terminal-window-suspects"
    / "strict_terminal_review.csv"
)
DEFAULT_AUTO_VERIFIED_PATH = (
    DEFAULT_OUTPUT_ROOT
    / "edgar-operating-terminal-top250-terminal-window-suspects"
    / "strict_terminal_auto_verified.csv"
)
DEFAULT_SUMMARY_PATH = (
    DEFAULT_OUTPUT_ROOT
    / "edgar-operating-terminal-top250-terminal-window-suspects"
    / "strict_terminal_review_summary.json"
)
MAX_CLOSE_DAYS_FROM_LAST = 14
TERMINAL_EXIT_FORMS = {"25-NSE", "15-12B"}


@dataclass(frozen=True)
class StrictTerminalReviewConfig:
    verifier_path: Path
    output_path: Path
    auto_verified_path: Path
    summary_path: Path
    max_close_days_from_last: int


def main() -> int:
    args = parse_args()
    config = StrictTerminalReviewConfig(
        verifier_path=Path(args.verifier_path),
        output_path=Path(args.output_path),
        auto_verified_path=Path(args.auto_verified_path),
        summary_path=Path(args.summary_path),
        max_close_days_from_last=args.max_close_days_from_last,
    )
    setup_logging(str(config.summary_path.with_suffix(".jsonl")))
    result = build_strict_terminal_review(config)
    get_logger(__name__).info(
        "strict terminal review complete",
        extra={"event": "strict_terminal_review_complete", "detail": result["summary"]},
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--verifier-path", default=str(DEFAULT_VERIFIER_PATH))
    parser.add_argument("--output-path", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument("--auto-verified-path", default=str(DEFAULT_AUTO_VERIFIED_PATH))
    parser.add_argument("--summary-path", default=str(DEFAULT_SUMMARY_PATH))
    parser.add_argument("--max-close-days-from-last", type=int, default=MAX_CLOSE_DAYS_FROM_LAST)
    return parser.parse_args()


def build_strict_terminal_review(config: StrictTerminalReviewConfig) -> dict[str, Any]:
    validate_config(config)
    verifier = pl.read_csv(config.verifier_path, infer_schema_length=0)
    require_columns(verifier)
    reviewed = pl.DataFrame([review_row(row, config) for row in verifier.to_dicts()])
    auto_verified = auto_verified_rows(reviewed)
    summary = build_summary(config, reviewed, auto_verified)
    write_outputs(config, reviewed, auto_verified, summary)
    return {"summary": summary, "rows": reviewed.to_dicts()}


def validate_config(config: StrictTerminalReviewConfig) -> None:
    if not config.verifier_path.exists():
        raise FileNotFoundError(f"verifier output does not exist: {config.verifier_path}")
    if config.max_close_days_from_last < 0:
        raise ValueError("--max-close-days-from-last cannot be negative")


def require_columns(frame: pl.DataFrame) -> None:
    required = [
        "symbol",
        "symbol_era_id",
        "research_status",
        "triage_bucket",
        "verifier_bucket",
        "verifier_flags",
        "form",
        "filed_at",
        "entity",
    ]
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"verifier output missing required columns: {missing}")


def review_row(row: dict[str, Any], config: StrictTerminalReviewConfig) -> dict[str, Any]:
    flags = set(split_flags(row.get("verifier_flags")))
    filed_to_last = days_filed_to_last(row)
    collision = has_parenthetical_ticker_collision(row)
    ticker_match = has_parenthetical_ticker_match(row)
    terminal_exit = normalize(row.get("form")) in TERMINAL_EXIT_FORMS
    close_near_last = (
        filed_to_last is not None and abs(filed_to_last) <= config.max_close_days_from_last
    )
    bucket = strict_bucket(row, flags, collision, ticker_match, terminal_exit, close_near_last)
    return {
        **row,
        "strict_terminal_bucket": bucket,
        "days_filed_to_original_last": filed_to_last,
        "parenthetical_ticker_collision": str(collision).lower(),
        "strict_terminal_reason": strict_reason(
            row, flags, collision, terminal_exit, close_near_last, filed_to_last
        ),
    }


def strict_bucket(
    row: dict[str, Any],
    flags: set[str],
    collision: bool,
    ticker_match: bool,
    terminal_exit: bool,
    close_near_last: bool,
) -> str:
    if collision:
        return "reject_symbol_collision"
    if row.get("verifier_bucket") == "weak_review_candidate":
        return "reject_weak_terminal_evidence"
    if (
        terminal_exit
        and row.get("verifier_bucket") == "strong_review_candidate"
        and ticker_match
        and close_near_last
    ):
        return "strict_verified_ready"
    if (
        row.get("triage_bucket") == "high_confidence_lead"
        and row.get("verifier_bucket") == "strong_review_candidate"
        and "completion_language" in flags
        and ticker_match
        and close_near_last
    ):
        return "strict_verified_ready"
    if row.get("verifier_bucket") == "strong_review_candidate":
        return "strong_needs_close_or_completion_review"
    if row.get("verifier_bucket") == "moderate_review_candidate":
        return "moderate_needs_completion_evidence"
    return "manual_review_required"


def strict_reason(
    row: dict[str, Any],
    flags: set[str],
    collision: bool,
    terminal_exit: bool,
    close_near_last: bool,
    filed_to_last: int | None,
) -> str:
    parts = [
        f"triage={row.get('triage_bucket')}",
        f"verifier={row.get('verifier_bucket')}",
        f"form={row.get('form')}",
        f"flags={'+'.join(sorted(flags)) or 'none'}",
        f"filed_to_original_last_days={filed_to_last}",
    ]
    if collision:
        parts.append("parenthetical_ticker_collision")
    if terminal_exit:
        parts.append("terminal_exit_form")
    if close_near_last:
        parts.append("filed_near_original_last")
    return "; ".join(parts)


def auto_verified_rows(reviewed: pl.DataFrame) -> pl.DataFrame:
    rows = reviewed.filter(pl.col("strict_terminal_bucket") == "strict_verified_ready")
    if rows.is_empty():
        return rows
    return rows.with_columns(
        pl.lit("verified").alias("research_status"),
        pl.col("verifier_document_url").alias("primary_source_url"),
        (
            pl.col("research_note")
            + pl.lit(" Strict terminal review: ")
            + pl.col("strict_terminal_reason")
        ).alias("research_note"),
    )


def days_filed_to_last(row: dict[str, Any]) -> int | None:
    filed = parse_date(row.get("filed_at"), "%Y-%m-%d")
    last = parse_date(row.get("original_last_day") or row.get("last_day"), "%Y%m%d")
    if not filed or not last:
        return None
    return (last - filed).days


def has_parenthetical_ticker_collision(row: dict[str, Any]) -> bool:
    symbol = normalize(row.get("symbol"))
    tickers = parenthetical_tickers(row.get("entity"))
    return bool(tickers) and symbol not in tickers


def has_parenthetical_ticker_match(row: dict[str, Any]) -> bool:
    return normalize(row.get("symbol")) in parenthetical_tickers(row.get("entity"))


def parenthetical_tickers(entity: Any) -> set[str]:
    tickers = set()
    for group in re.findall(r"\(([^)]*)\)", str(entity or "")):
        for token in group.split(","):
            cleaned = normalize(token)
            if cleaned and cleaned != "CIK" and re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,7}", cleaned):
                tickers.add(cleaned)
    return tickers


def split_flags(value: Any) -> list[str]:
    return [flag for flag in str(value or "").split("|") if flag]


def parse_date(value: Any, fmt: str) -> datetime.date | None:
    text = str(value or "")
    try:
        return datetime.strptime(text, fmt).date()
    except ValueError:
        return None


def normalize(value: Any) -> str:
    return str(value or "").strip().upper()


def build_summary(
    config: StrictTerminalReviewConfig, reviewed: pl.DataFrame, auto_verified: pl.DataFrame
) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "strict terminal-event verifier post-processing",
        "verifier_path": str(config.verifier_path),
        "output_path": str(config.output_path),
        "auto_verified_path": str(config.auto_verified_path),
        "row_count": reviewed.height,
        "auto_verified_count": auto_verified.height,
        "strict_terminal_bucket_counts": count_by(reviewed, "strict_terminal_bucket"),
        "max_close_days_from_last": config.max_close_days_from_last,
        "limitations": [
            "Auto-verified rows require terminal-close quality evidence.",
            "Strong verifier rows without close timing or terminal forms remain review candidates.",
        ],
    }


def count_by(frame: pl.DataFrame, column: str) -> dict[str, int]:
    if frame.is_empty():
        return {}
    return {
        str(row[column]): row["len"] for row in frame.group_by(column).len().sort(column).to_dicts()
    }


def write_outputs(
    config: StrictTerminalReviewConfig,
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
