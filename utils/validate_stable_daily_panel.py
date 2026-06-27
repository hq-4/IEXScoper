from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.framework.logging import get_logger, setup_logging
from utils.build_stable_daily_panel import QUALITY_EVENT_TYPES
from utils.stable_daily_panel_validation_outputs import write_outputs, write_schema_failure

DEFAULT_PANEL_PATH = Path("/media/tn/pq/derived/stable-daily-panel/stable_daily_panel.parquet")
DEFAULT_QUALITY_EVENTS_PATH = Path("reports/stable-long-window-quality/quality_events.parquet")
DEFAULT_REPORT_ROOT = Path("reports/stable-daily-panel-validation")
REQUIRED_COLUMNS = [
    "day",
    "symbol",
    "symbol_era_id",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trade_count",
    "notional",
    "vwap",
    "first_timestamp",
    "last_timestamp",
    "liquidity_tier",
    "expected_days_in_era",
    "iex_latest_issuer",
    "iex_entity_confidence",
    "quality_has_event",
]
CRITICAL_NOT_NULL_COLUMNS = [
    "day",
    "symbol",
    "symbol_era_id",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trade_count",
    "notional",
    "vwap",
    "liquidity_tier",
    "iex_latest_issuer",
    "iex_entity_confidence",
]


@dataclass(frozen=True)
class PanelValidationConfig:
    panel_path: Path
    quality_events_path: Path
    report_root: Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--panel-path", default=str(DEFAULT_PANEL_PATH))
    parser.add_argument("--quality-events-path", default=str(DEFAULT_QUALITY_EVENTS_PATH))
    parser.add_argument("--report-root", default=str(DEFAULT_REPORT_ROOT))
    args = parser.parse_args()
    config = PanelValidationConfig(
        panel_path=Path(args.panel_path),
        quality_events_path=Path(args.quality_events_path),
        report_root=Path(args.report_root),
    )
    setup_logging(str(config.report_root / "stable_daily_panel_validation.jsonl"))
    result = validate_stable_daily_panel(config)
    get_logger(__name__).info(
        "stable daily panel validation complete",
        extra={"event": "stable_daily_panel_validation_complete", "detail": result["summary"]},
    )
    return 0 if result["summary"]["validation_status"] == "pass" else 1


def validate_stable_daily_panel(config: PanelValidationConfig) -> dict[str, Any]:
    validate_inputs(config)
    config.report_root.mkdir(parents=True, exist_ok=True)
    missing_columns = missing_required_columns(config.panel_path)
    if missing_columns:
        summary = failed_schema_summary(config, missing_columns)
        write_schema_failure(config.report_root, summary)
        return {"summary": summary}
    panel = pl.scan_parquet(config.panel_path)
    core = core_metrics(panel)
    nulls = null_counts(panel)
    flag_mismatches = quality_flag_mismatches(panel, config.quality_events_path)
    year_tier = year_tier_coverage(panel)
    era_coverage = era_coverage_frame(panel)
    confidence = count_frame(panel, "iex_entity_confidence")
    tier = count_frame(panel, "liquidity_tier")
    summary = build_summary(config, core, nulls, flag_mismatches, year_tier, era_coverage)
    write_outputs(config.report_root, summary, nulls, flag_mismatches, year_tier, era_coverage, confidence, tier)
    return {"summary": summary}


def validate_inputs(config: PanelValidationConfig) -> None:
    for path, label in [
        (config.panel_path, "stable daily panel"),
        (config.quality_events_path, "quality events parquet"),
    ]:
        if not path.exists():
            raise FileNotFoundError(f"{label} does not exist: {path}")


def missing_required_columns(path: Path) -> list[str]:
    columns = pl.scan_parquet(path).collect_schema().names()
    quality_columns = [f"quality_{event}" for event in QUALITY_EVENT_TYPES]
    required = [*REQUIRED_COLUMNS, *quality_columns]
    return [column for column in required if column not in columns]


def core_metrics(panel: pl.LazyFrame) -> dict[str, Any]:
    duplicate_keys = (
        panel.group_by("day", "symbol_era_id")
        .len()
        .filter(pl.col("len") > 1)
        .select((pl.col("len") - 1).sum().fill_null(0).alias("duplicate_key_count"))
    )
    row = panel.select(
        pl.len().alias("row_count"),
        pl.col("symbol_era_id").n_unique().alias("symbol_era_count"),
        pl.col("symbol").n_unique().alias("symbol_count"),
        pl.col("day").min().alias("first_day"),
        pl.col("day").max().alias("last_day"),
        invalid_ohlc_expr().sum().alias("invalid_ohlc_count"),
        nonpositive_price_expr().sum().alias("nonpositive_price_count"),
        (pl.col("volume") <= 0).sum().alias("nonpositive_volume_count"),
        (pl.col("trade_count") <= 0).sum().alias("nonpositive_trade_count"),
        (pl.col("notional") <= 0).sum().alias("nonpositive_notional_count"),
        (pl.col("first_timestamp") > pl.col("last_timestamp")).sum().alias("timestamp_order_count"),
        quality_has_event_mismatch_expr().sum().alias("quality_has_event_mismatch_count"),
    ).collect().to_dicts()[0]
    row.update(duplicate_keys.collect().to_dicts()[0])
    return row


def invalid_ohlc_expr() -> pl.Expr:
    return (
        (pl.col("high") < pl.col("low"))
        | (pl.col("open") < pl.col("low"))
        | (pl.col("open") > pl.col("high"))
        | (pl.col("close") < pl.col("low"))
        | (pl.col("close") > pl.col("high"))
    ).fill_null(True)


def nonpositive_price_expr() -> pl.Expr:
    return (
        (pl.col("open") <= 0)
        | (pl.col("high") <= 0)
        | (pl.col("low") <= 0)
        | (pl.col("close") <= 0)
        | (pl.col("vwap") <= 0)
    ).fill_null(True)


def quality_has_event_mismatch_expr() -> pl.Expr:
    flag_columns = [f"quality_{event}" for event in QUALITY_EVENT_TYPES]
    return pl.col("quality_has_event") != pl.any_horizontal(flag_columns)


def null_counts(panel: pl.LazyFrame) -> pl.DataFrame:
    return panel.select(
        [pl.col(column).is_null().sum().alias(column) for column in CRITICAL_NOT_NULL_COLUMNS]
    ).collect().transpose(include_header=True, header_name="column", column_names=["null_count"])


def quality_flag_mismatches(panel: pl.LazyFrame, events_path: Path) -> pl.DataFrame:
    expected = expected_quality_flags(events_path)
    flag_columns = [f"quality_{event}" for event in QUALITY_EVENT_TYPES]
    joined = panel.select("day", "symbol_era_id", *flag_columns).join(
        expected, on=["day", "symbol_era_id"], how="left"
    )
    return joined.select(
        [
            (
                pl.col(column)
                != pl.col(f"expected_{column}").fill_null(False)
            )
            .sum()
            .alias(column)
            for column in flag_columns
        ]
    ).collect().transpose(include_header=True, header_name="flag", column_names=["mismatch_count"])


def expected_quality_flags(path: Path) -> pl.LazyFrame:
    events = pl.scan_parquet(path).select("day", "symbol_era_id", "event_type")
    return events.group_by("day", "symbol_era_id").agg(
        [
            (pl.col("event_type") == event).any().alias(f"expected_quality_{event}")
            for event in QUALITY_EVENT_TYPES
        ]
    )


def year_tier_coverage(panel: pl.LazyFrame) -> pl.DataFrame:
    return (
        panel.with_columns(pl.col("day").str.slice(0, 4).alias("year"))
        .group_by("year", "liquidity_tier")
        .agg(
            pl.len().alias("row_count"),
            pl.col("symbol_era_id").n_unique().alias("symbol_era_count"),
            pl.col("quality_has_event").sum().alias("quality_event_rows"),
        )
        .sort(["year", "liquidity_tier"])
        .collect()
    )


def era_coverage_frame(panel: pl.LazyFrame) -> pl.DataFrame:
    return (
        panel.group_by("symbol", "symbol_era_id", "liquidity_tier")
        .agg(
            pl.col("day").n_unique().alias("observed_panel_days"),
            pl.col("day").min().alias("first_panel_day"),
            pl.col("day").max().alias("last_panel_day"),
            pl.col("expected_days_in_era").first().alias("expected_days_in_era"),
            pl.col("quality_has_event").sum().alias("quality_event_rows"),
        )
        .with_columns(
            (pl.col("observed_panel_days") / pl.col("expected_days_in_era")).alias(
                "panel_day_coverage_ratio"
            )
        )
        .sort(["panel_day_coverage_ratio", "observed_panel_days", "symbol"])
        .collect()
    )


def count_frame(panel: pl.LazyFrame, column: str) -> pl.DataFrame:
    return panel.group_by(column).len().sort(column).collect()


def build_summary(
    config: PanelValidationConfig,
    core: dict[str, Any],
    nulls: pl.DataFrame,
    flag_mismatches: pl.DataFrame,
    year_tier: pl.DataFrame,
    era_coverage: pl.DataFrame,
) -> dict[str, Any]:
    hard_failure_count = hard_failure_count_from(core, nulls, flag_mismatches)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "stable daily panel contract validation",
        "panel_path": str(config.panel_path),
        "quality_events_path": str(config.quality_events_path),
        "validation_status": "pass" if hard_failure_count == 0 else "fail",
        "hard_failure_count": hard_failure_count,
        **core,
        "critical_null_count": int(nulls["null_count"].sum()),
        "quality_flag_source_mismatch_count": int(flag_mismatches["mismatch_count"].sum()),
        "year_count": year_tier.select("year").n_unique(),
        "min_panel_day_coverage_ratio": era_coverage["panel_day_coverage_ratio"].min(),
        "median_panel_day_coverage_ratio": era_coverage["panel_day_coverage_ratio"].median(),
    }


def hard_failure_count_from(core: dict[str, Any], nulls: pl.DataFrame, mismatches: pl.DataFrame) -> int:
    keys = [
        "duplicate_key_count",
        "invalid_ohlc_count",
        "nonpositive_price_count",
        "nonpositive_volume_count",
        "nonpositive_trade_count",
        "nonpositive_notional_count",
        "timestamp_order_count",
        "quality_has_event_mismatch_count",
    ]
    return int(sum(core[key] for key in keys) + nulls["null_count"].sum() + mismatches["mismatch_count"].sum())


def failed_schema_summary(config: PanelValidationConfig, missing: list[str]) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "stable daily panel contract validation",
        "panel_path": str(config.panel_path),
        "quality_events_path": str(config.quality_events_path),
        "validation_status": "fail",
        "hard_failure_count": len(missing),
        "missing_columns": missing,
    }


if __name__ == "__main__":
    raise SystemExit(main())
