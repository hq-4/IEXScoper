from __future__ import annotations

from pathlib import Path

import polars as pl

AUTOMATION_COLUMNS = (
    "automation_status",
    "automation_workflow_version",
    "automation_attempt_count",
    "automation_last_bucket",
)


def attach_automation_status(workplan: pl.DataFrame, path: Path | None) -> pl.DataFrame:
    if path is None or not path.exists():
        return workplan.with_columns(default_expressions())
    automation = pl.read_csv(path, infer_schema_length=0)
    required = ["symbol_era_id", *AUTOMATION_COLUMNS]
    missing = [column for column in required if column not in automation.columns]
    if missing:
        raise ValueError(f"automation status CSV missing columns: {missing}")
    return workplan.join(automation.select(required), on="symbol_era_id", how="left").with_columns(
        pl.col("automation_status").fill_null("unattempted"),
        pl.col("automation_workflow_version").fill_null(""),
        pl.col("automation_attempt_count").cast(pl.Int64, strict=False).fill_null(0),
        pl.col("automation_last_bucket").fill_null(""),
    )


def default_expressions() -> list[pl.Expr]:
    return [
        pl.lit("unattempted").alias("automation_status"),
        pl.lit("").alias("automation_workflow_version"),
        pl.lit(0).alias("automation_attempt_count"),
        pl.lit("").alias("automation_last_bucket"),
    ]
