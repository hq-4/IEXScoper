from __future__ import annotations

import polars as pl

from utils.resolution_ledger import RESOLUTION_COLUMNS, validate_resolution_ledger


def build_low_materiality_ledger_candidates(workplan: pl.DataFrame) -> pl.DataFrame:
    low = workplan.filter(pl.col("workplan_bucket") == "low_materiality_bulk_disposition")
    if low.is_empty():
        return pl.DataFrame(schema={column: pl.String for column in RESOLUTION_COLUMNS})
    candidates = low.with_columns(ledger_candidate_exprs()).select(RESOLUTION_COLUMNS)
    validate_low_materiality_candidates(candidates)
    return candidates


def ledger_candidate_exprs() -> list[pl.Expr]:
    return [
        pl.lit("terminal_disposition").alias("resolution_status"),
        pl.lit("low_materiality_market_data_artifact").alias("resolution_disposition"),
        pl.lit("local_market_data_workflow").alias("evidence_tier"),
        pl.lit(None, dtype=pl.String).alias("historical_issuer_name"),
        pl.lit("low_materiality_market_data_artifact").alias("event_type"),
        pl.col("last_day").alias("event_date"),
        pl.lit(None, dtype=pl.String).alias("successor"),
        pl.lit("local:dead-ticker-resolution-workplan").alias("primary_source_url"),
        pl.lit(None, dtype=pl.String).alias("secondary_source_url"),
        ledger_source_note_expr().alias("source_note"),
        pl.lit("dead_ticker_resolution_workplan_v1").alias("resolver"),
    ]


def ledger_source_note_expr() -> pl.Expr:
    return pl.concat_str(
        [
            pl.lit("Low-materiality terminal disposition candidate: trade_rows="),
            pl.col("trade_rows").cast(pl.String),
            pl.lit(", observed_days="),
            pl.col("observed_days").cast(pl.String),
            pl.lit(", no current SEC/IEX match, threshold trade_rows<=99 and observed_days<=5."),
        ]
    )


def validate_low_materiality_candidates(candidates: pl.DataFrame) -> None:
    missing = candidates.filter(pl.col("symbol_era_id").is_null() | (pl.col("symbol_era_id") == ""))
    if missing.height:
        raise ValueError("low-materiality candidates include missing symbol_era_id values")
    validate_resolution_ledger(candidates)
