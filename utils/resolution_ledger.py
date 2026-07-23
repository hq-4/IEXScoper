from __future__ import annotations

from pathlib import Path

import polars as pl

from utils.dead_ticker_review_schema import DEFAULT_OUTPUT_ROOT

DEFAULT_RESOLUTION_LEDGER_PATH = Path("data/manual_overrides/ticker_era_resolution_ledger.csv")
DEFAULT_RESOLUTION_OUTPUT_ROOT = DEFAULT_OUTPUT_ROOT / "resolution-lanes"

RESOLUTION_COLUMNS = [
    "symbol",
    "symbol_era_id",
    "resolution_status",
    "resolution_disposition",
    "evidence_tier",
    "research_route",
    "instrument_type",
    "historical_issuer_name",
    "event_type",
    "event_date",
    "successor",
    "primary_source_url",
    "secondary_source_url",
    "source_note",
    "resolver",
]

TERMINAL_RESOLUTION_STATUSES = {
    "auto_verified",
    "terminal_disposition",
    "no_evidence_after_search",
}

REVIEW_RESOLUTION_STATUSES = {
    "review_ready",
    "manual_required",
}

WORKFLOW_MANUAL_VERIFIED = "manual_verified_historical_identity"
WORKFLOW_TERMINAL = "ledger_terminal_disposition"
WORKFLOW_REVIEW_READY = "ledger_review_ready"
WORKFLOW_MANUAL_REQUIRED = "ledger_manual_required"
WORKFLOW_NEEDS_RESOLUTION = "needs_resolution"


def empty_resolution_ledger() -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.String for column in RESOLUTION_COLUMNS})


def load_resolution_ledger(path: Path) -> pl.DataFrame:
    if not path.exists():
        return empty_resolution_ledger()
    frame = pl.read_csv(path, infer_schema_length=0)
    missing = [column for column in RESOLUTION_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"resolution ledger missing required columns: {missing}")
    return (
        frame.select(RESOLUTION_COLUMNS)
        .with_columns(pl.all().str.strip_chars())
        .filter(pl.col("symbol_era_id").is_not_null() & (pl.col("symbol_era_id") != ""))
    )


def validate_resolution_ledger(frame: pl.DataFrame) -> None:
    duplicate_ids = (
        frame.group_by("symbol_era_id").len().filter(pl.col("len") > 1)["symbol_era_id"].to_list()
    )
    if duplicate_ids:
        raise ValueError(f"duplicate resolution ledger symbol_era_id values: {duplicate_ids}")
    invalid_statuses = (
        frame.filter(
            ~pl.col("resolution_status").is_in(
                sorted(TERMINAL_RESOLUTION_STATUSES | REVIEW_RESOLUTION_STATUSES)
            )
        )["symbol_era_id"].to_list()
    )
    if invalid_statuses:
        raise ValueError(f"invalid resolution_status values for: {invalid_statuses}")


def ledger_select_for_join(frame: pl.DataFrame) -> pl.DataFrame:
    validate_resolution_ledger(frame)
    return frame.select(
        [
            "symbol_era_id",
            "resolution_status",
            "resolution_disposition",
            "evidence_tier",
            pl.col("research_route").alias("ledger_research_route"),
            pl.col("instrument_type").alias("ledger_instrument_type"),
            pl.col("historical_issuer_name").alias("ledger_historical_issuer_name"),
            pl.col("event_type").alias("ledger_event_type"),
            pl.col("event_date").alias("ledger_event_date"),
            pl.col("successor").alias("ledger_successor"),
            pl.col("primary_source_url").alias("ledger_primary_source_url"),
            pl.col("secondary_source_url").alias("ledger_secondary_source_url"),
            pl.col("source_note").alias("ledger_source_note"),
            "resolver",
        ]
    )


def resolution_workflow_status_expr() -> pl.Expr:
    terminal = sorted(TERMINAL_RESOLUTION_STATUSES)
    return (
        pl.when(pl.col("historical_identity_status").is_not_null())
        .then(pl.lit(WORKFLOW_MANUAL_VERIFIED))
        .when(pl.col("resolution_status").is_in(terminal))
        .then(pl.lit(WORKFLOW_TERMINAL))
        .when(pl.col("resolution_status") == "review_ready")
        .then(pl.lit(WORKFLOW_REVIEW_READY))
        .when(pl.col("resolution_status") == "manual_required")
        .then(pl.lit(WORKFLOW_MANUAL_REQUIRED))
        .otherwise(pl.lit(WORKFLOW_NEEDS_RESOLUTION))
    )
