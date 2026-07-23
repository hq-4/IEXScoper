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
from utils.resolution_ledger import (
    DEFAULT_RESOLUTION_LEDGER_PATH,
    DEFAULT_RESOLUTION_OUTPUT_ROOT,
    RESOLUTION_COLUMNS,
    load_resolution_ledger,
    validate_resolution_ledger,
)

DEFAULT_CANDIDATES_PATH = (
    DEFAULT_RESOLUTION_OUTPUT_ROOT / "parent_security_resolution_candidates.csv"
)
DEFAULT_SUMMARY_PATH = DEFAULT_RESOLUTION_OUTPUT_ROOT / "resolution_ledger_import_summary.json"


@dataclass(frozen=True)
class ResolutionLedgerImportConfig:
    candidates_path: Path
    ledger_path: Path
    summary_path: Path
    dry_run: bool


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidates-path", default=str(DEFAULT_CANDIDATES_PATH))
    parser.add_argument("--ledger-path", default=str(DEFAULT_RESOLUTION_LEDGER_PATH))
    parser.add_argument("--summary-path", default=str(DEFAULT_SUMMARY_PATH))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    config = ResolutionLedgerImportConfig(
        candidates_path=Path(args.candidates_path),
        ledger_path=Path(args.ledger_path),
        summary_path=Path(args.summary_path),
        dry_run=args.dry_run,
    )
    setup_logging(str(config.summary_path.parent / "resolution_ledger_import.jsonl"))
    result = import_resolution_ledger(config)
    get_logger(__name__).info(
        "resolution ledger import complete",
        extra={"event": "resolution_ledger_import_complete", "detail": result["summary"]},
    )
    return 0


def import_resolution_ledger(config: ResolutionLedgerImportConfig) -> dict[str, Any]:
    validate_paths(config)
    existing = load_resolution_ledger(config.ledger_path)
    candidates = load_candidates(config.candidates_path)
    additions = new_candidate_rows(candidates, existing)
    validate_resolution_ledger(pl.concat([existing, additions]))
    summary = build_summary(config, candidates, additions)
    if not config.dry_run:
        write_ledger(config.ledger_path, existing, additions)
    write_summary(config.summary_path, summary)
    return {"summary": summary, "rows": additions.to_dicts()}


def validate_paths(config: ResolutionLedgerImportConfig) -> None:
    if not config.candidates_path.exists():
        raise FileNotFoundError(f"resolution candidates do not exist: {config.candidates_path}")
    if not config.ledger_path.exists():
        raise FileNotFoundError(f"resolution ledger does not exist: {config.ledger_path}")


def load_candidates(path: Path) -> pl.DataFrame:
    frame = pl.read_csv(path, infer_schema_length=0)
    missing = [column for column in RESOLUTION_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"resolution candidates missing required columns: {missing}")
    candidates = (
        frame.select(RESOLUTION_COLUMNS)
        .with_columns(pl.all().str.strip_chars())
        .filter(pl.col("symbol_era_id").is_not_null() & (pl.col("symbol_era_id") != ""))
    )
    validate_required_values(candidates)
    validate_resolution_ledger(candidates)
    return candidates


def validate_required_values(candidates: pl.DataFrame) -> None:
    required = [
        "symbol",
        "symbol_era_id",
        "resolution_status",
        "resolution_disposition",
        "evidence_tier",
        "research_route",
        "instrument_type",
        "source_note",
        "resolver",
    ]
    bad = {}
    for column in required:
        invalid = candidates.filter(pl.col(column).is_null() | (pl.col(column) == ""))
        if invalid.height:
            bad[column] = invalid["symbol_era_id"].to_list()
    if bad:
        raise ValueError(f"resolution candidates missing required values: {bad}")


def new_candidate_rows(candidates: pl.DataFrame, existing: pl.DataFrame) -> pl.DataFrame:
    if existing.is_empty():
        return candidates
    existing_ids = set(existing["symbol_era_id"].drop_nulls().to_list())
    return candidates.filter(~pl.col("symbol_era_id").is_in(sorted(existing_ids)))


def build_summary(
    config: ResolutionLedgerImportConfig,
    candidates: pl.DataFrame,
    additions: pl.DataFrame,
) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "ticker-era resolution ledger import",
        "candidates_path": str(config.candidates_path),
        "ledger_path": str(config.ledger_path),
        "dry_run": config.dry_run,
        "candidate_row_count": candidates.height,
        "appended_row_count": additions.height,
        "skipped_existing_count": candidates.height - additions.height,
        "appended_symbol_era_ids": additions["symbol_era_id"].to_list() if additions.height else [],
    }


def write_ledger(path: Path, existing: pl.DataFrame, additions: pl.DataFrame) -> None:
    if additions.is_empty():
        return
    merged = pl.concat([existing.select(RESOLUTION_COLUMNS), additions.select(RESOLUTION_COLUMNS)])
    path.parent.mkdir(parents=True, exist_ok=True)
    merged.write_csv(path)


def write_summary(path: Path, summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
