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
from utils.dead_ticker_review_schema import DEFAULT_MANUAL_OVERRIDES_PATH, DEFAULT_OUTPUT_ROOT

DEFAULT_TEMPLATE_PATH = DEFAULT_OUTPUT_ROOT / "manual_resolution_template.csv"
DEFAULT_SUMMARY_PATH = DEFAULT_OUTPUT_ROOT / "manual_override_import_summary.json"
VERIFIED_STATUS = "verified"

OVERRIDE_COLUMNS = [
    "symbol",
    "symbol_era_id",
    "historical_identity_status",
    "historical_issuer_name",
    "historical_event_type",
    "historical_event_date",
    "historical_successor",
    "source_url",
    "source_note",
]

REQUIRED_TEMPLATE_COLUMNS = [
    "symbol",
    "symbol_era_id",
    "research_status",
    "proposed_historical_identity_status",
    "proposed_historical_issuer_name",
    "proposed_historical_event_type",
    "proposed_historical_event_date",
    "primary_source_url",
    "research_note",
]


@dataclass(frozen=True)
class ManualOverrideImportConfig:
    template_path: Path
    overrides_path: Path
    summary_path: Path
    dry_run: bool


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--template-path", default=str(DEFAULT_TEMPLATE_PATH))
    parser.add_argument("--overrides-path", default=str(DEFAULT_MANUAL_OVERRIDES_PATH))
    parser.add_argument("--summary-path", default=str(DEFAULT_SUMMARY_PATH))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    config = ManualOverrideImportConfig(
        template_path=Path(args.template_path),
        overrides_path=Path(args.overrides_path),
        summary_path=Path(args.summary_path),
        dry_run=args.dry_run,
    )
    setup_logging(str(config.summary_path.parent / "manual_override_import.jsonl"))
    result = import_manual_overrides(config)
    get_logger(__name__).info(
        "manual override import complete",
        extra={"event": "manual_override_import_complete", "detail": result["summary"]},
    )
    return 0


def import_manual_overrides(config: ManualOverrideImportConfig) -> dict[str, Any]:
    validate_paths(config)
    template = load_csv(config.template_path, REQUIRED_TEMPLATE_COLUMNS, "resolution template")
    existing = load_csv(config.overrides_path, OVERRIDE_COLUMNS, "manual override file")
    verified = verified_template_rows(template)
    additions = template_rows_to_overrides(verified)
    validate_additions(additions, existing)
    summary = build_summary(config, template, additions)
    if not config.dry_run:
        write_overrides(config.overrides_path, existing, additions)
    write_summary(config.summary_path, summary)
    return {"summary": summary, "rows": additions.to_dicts()}


def validate_paths(config: ManualOverrideImportConfig) -> None:
    if not config.template_path.exists():
        raise FileNotFoundError(f"resolution template does not exist: {config.template_path}")
    if not config.overrides_path.exists():
        raise FileNotFoundError(f"manual override file does not exist: {config.overrides_path}")


def load_csv(path: Path, required: list[str], label: str) -> pl.DataFrame:
    frame = pl.read_csv(path, infer_schema_length=0)
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"{label} missing required columns: {missing}")
    return frame


def verified_template_rows(template: pl.DataFrame) -> pl.DataFrame:
    return template.filter(pl.col("research_status").str.to_lowercase() == VERIFIED_STATUS)


def template_rows_to_overrides(verified: pl.DataFrame) -> pl.DataFrame:
    if verified.is_empty():
        return pl.DataFrame(schema={column: pl.String for column in OVERRIDE_COLUMNS})
    optional_successor = (
        pl.col("proposed_historical_successor")
        if "proposed_historical_successor" in verified.columns
        else pl.lit(None, dtype=pl.String)
    )
    return (
        verified.select(
            pl.col("symbol").str.to_uppercase().alias("symbol"),
            pl.col("symbol_era_id"),
            pl.col("proposed_historical_identity_status").alias("historical_identity_status"),
            pl.col("proposed_historical_issuer_name").alias("historical_issuer_name"),
            pl.col("proposed_historical_event_type").alias("historical_event_type"),
            pl.col("proposed_historical_event_date").alias("historical_event_date"),
            optional_successor.alias("historical_successor"),
            pl.col("primary_source_url").alias("source_url"),
            pl.col("research_note").alias("source_note"),
        )
        .with_columns(pl.all().str.strip_chars())
        .select(OVERRIDE_COLUMNS)
    )


def validate_additions(additions: pl.DataFrame, existing: pl.DataFrame) -> None:
    if additions.is_empty():
        return
    validate_required_values(additions)
    duplicate_new = duplicate_values(additions, "symbol_era_id")
    if duplicate_new:
        raise ValueError(f"duplicate verified symbol_era_id values in template: {duplicate_new}")
    existing_ids = set(existing["symbol_era_id"].drop_nulls().to_list())
    duplicate_existing = sorted(set(additions["symbol_era_id"].to_list()) & existing_ids)
    if duplicate_existing:
        raise ValueError(
            f"manual overrides already exist for symbol_era_id values: {duplicate_existing}"
        )


def validate_required_values(additions: pl.DataFrame) -> None:
    required = [column for column in OVERRIDE_COLUMNS if column != "historical_successor"]
    bad = {}
    for column in required:
        invalid = additions.filter(pl.col(column).is_null() | (pl.col(column) == ""))
        if invalid.height:
            bad[column] = invalid["symbol_era_id"].to_list()
    if bad:
        raise ValueError(f"verified rows missing required override values: {bad}")


def duplicate_values(frame: pl.DataFrame, column: str) -> list[str]:
    return frame.group_by(column).len().filter(pl.col("len") > 1).sort(column)[column].to_list()


def build_summary(
    config: ManualOverrideImportConfig, template: pl.DataFrame, additions: pl.DataFrame
) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "manual dead ticker override import",
        "template_path": str(config.template_path),
        "overrides_path": str(config.overrides_path),
        "dry_run": config.dry_run,
        "template_row_count": template.height,
        "verified_row_count": additions.height,
        "verified_symbol_era_ids": additions["symbol_era_id"].to_list() if additions.height else [],
    }


def write_overrides(path: Path, existing: pl.DataFrame, additions: pl.DataFrame) -> None:
    if additions.is_empty():
        return
    merged = pl.concat([existing.select(OVERRIDE_COLUMNS), additions.select(OVERRIDE_COLUMNS)])
    path.parent.mkdir(parents=True, exist_ok=True)
    merged.write_csv(path)


def write_summary(path: Path, summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
